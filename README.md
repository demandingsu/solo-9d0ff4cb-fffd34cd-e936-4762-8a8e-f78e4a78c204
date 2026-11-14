# 雨量站批次存储 API（FastAPI + PyArrow + SQLite）

把雨量站频繁上传的小批 CSV 观测持久化为独立 Parquet 批次，按站日合并成
单个日文件，供暴雨复盘脚本以**快照一致**的方式读取；并提供按站号的
**增量拉取 API**，把原始上传批次按入库顺序送往复盘库，断线后从已确认
批次继续。

- **Web 框架**：FastAPI（同步端点，跑在 threadpool 上，避免阻塞事件循环）
- **列存文件**：PyArrow / Parquet（zstd 压缩，UTC 毫秒时间戳）
- **元数据**：SQLite（WAL 模式，`BEGIN IMMEDIATE` 事务 + 进程内写锁）
- **交付**：可直接启动的 Docker Compose、本地样例 CSV、一键演示脚本、24 个测试

## 目录结构

```
app/
  config.py      环境变量与路径配置（RAIN_DATA_DIR 一处切换存储位置）
  db.py          SQLite 连接、WAL/忙等待、串行化写事务、表结构
  errors.py      领域异常 -> HTTP 状态码映射
  ingestion.py   CSV 解析/校验、规范 Parquet schema、记录去留（不去重、不排序）
  storage.py     Parquet 原子写、查询快照与引用计数、后台清理、崩溃恢复扫描
  services.py    上传 / 查询 / 合并 / 列表 的事务编排
  replay.py      增量拉取（复盘 feed）：游标编解码、轮次上界、按来源批次找回原行
  main.py        FastAPI 路由与生命周期
samples/         本地雨量 CSV 样例（含中文表头样例与故意保留的重复行）
tests/           端到端与并发测试（pytest + TestClient + 线程池）
demo.sh          对运行中的服务执行 上传→查询→合并→冲突→清理→增量续拉 全流程
Dockerfile
docker-compose.yml
```

## 快速开始

### Docker Compose

```bash
docker compose up --build
# API: http://localhost:8000  交互式文档: http://localhost:8000/docs
```

数据保存在命名卷 `rain-data`（容器内 `/data`），含 SQLite 元数据库与
`parquet/<站号>/<日期>/*.parquet` 文件树。

### 本地直接运行（无需 Docker）

```bash
pip install -r requirements.txt
export RAIN_DATA_DIR=./data
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 一键演示

先启动服务，然后：

```bash
./demo.sh                                 # 默认 http://127.0.0.1:8000
RAIN_BASE_URL=http://其它主机:8000 ./demo.sh
```

### 跑测试

```bash
pip install -r requirements.txt pytest httpx
pytest -q
```

## API 入口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/stations/{station_id}/days/{day}/batches` | 上传单站单日 CSV（multipart `file`），作为独立批次保留，返回 `201` |
| `GET`  | `/stations/{station_id}/days/{day}/records` | **实际读取已发布 Parquet 文件**，返回全部记录与所用批次清单 |
| `POST` | `/stations/{station_id}/days/{day}/merge` | 把当前已发布的小批次原子合并为一个 Parquet 文件 |
| `GET`  | `/stations/{station_id}/days/{day}/batches` | 列出该站日全部批次（含 `published` / `superseded` 与文件是否还在） |
| `GET`  | `/stations/{station_id}/pull` | **增量拉取**：按入库顺序分页返回原上传批次（批次号 + 原始观测行 + 续接游标） |
| `POST` | `/admin/reap` | 立即清理无查询引用的旧文件（后台线程默认每 5 秒自动执行） |
| `GET`  | `/health` | 健康检查 |

`day` 为 `YYYY-MM-DD`（UTC），`station_id` 为 1–64 位字母数字、`_`、`-`。
查询支持 `?limit=N`。

### CSV 格式

表头支持中英文别名，例如 `station_id,obs_time,rain_mm` 或
`站号,观测时间,雨量毫米`。要求：

- 每行站号与 URL 站号一致；
- 时间为 UTC，且必须落在 URL 指定的 UTC 日期内；
- 雨量为有限非负数值（毫米）；
- **完全保留重复行与原始顺序**，不做去重。

### curl 示例

```bash
curl -X POST -F "file=@samples/st001_2026-11-12_batch1.csv" \
  http://localhost:8000/stations/ST001/days/2026-11-12/batches

curl http://localhost:8000/stations/ST001/days/2026-11-12/records

curl -X POST http://localhost:8000/stations/ST001/days/2026-11-12/merge
```

## 增量拉取 API（复盘库对接）

`GET /stations/{station_id}/pull?cursor=...&limit=N` 把**原始上传批次**按
入库顺序（元数据提交序）分页送往复盘库；合并批次不算新上传，不会出现在
流里。每页返回批次号、该批次的全部原始观测行（重复行原样保留）和续接
游标 `next_cursor`；`limit` 为每页最多批次个数（默认 1，最大 100）。

**轮次与上界**：一轮开始时（不带游标，或带上一轮完成时返回的游标）固定
当时的入库上界 `round_bound`，本轮只交付上界之内的批次；之后到达的上
传等下一轮。读到上界后响应 `round_complete=true`，此时的 `next_cursor`
从已确认位置开启新一轮，自动纳入后来上传。游标是 opaque 的 URL 安全
令牌，绑定站号；篡改或跨站使用返回 `400`。

**断线续传**：游标无状态，客户端只需持久化最后确认页的 `next_cursor`。
同一游标重试永远返回相同内容——即使原批次已合并、源 Parquet 已清理，
服务也会沿 `source_batches` 谱系找到当前已发布的合并文件，按
`source_batch_id` 过滤回原行（行序与重复行保持不变）。读取失败返回
`503`，不含部分页、不推进游标，客户端用原游标重试即可。

### 合并清理后的续拉示例

```bash
# 1) 上传两个批次并开始第一轮拉取，记下每页的 next_cursor
curl -X POST -F "file=@b1.csv" http://localhost:8000/stations/ST900/days/2026-11-12/batches
curl -X POST -F "file=@b2.csv" http://localhost:8000/stations/ST900/days/2026-11-12/batches
curl "http://localhost:8000/stations/ST900/pull"                      # 页1 + 游标 C1
curl "http://localhost:8000/stations/ST900/pull?cursor=$C1"           # 页2，round_complete=true，游标 C2

# 2) 合并并清理：原始 Parquet 文件被删除
curl -X POST http://localhost:8000/stations/ST900/days/2026-11-12/merge
curl -X POST http://localhost:8000/admin/reap

# 3) 断线后用合并前的同一游标重试：从已发布合并文件按来源批次找回原行，
#    返回与合并前完全相同的一页
curl "http://localhost:8000/stations/ST900/pull?cursor=$C1"           # 与页2逐字节一致

# 4) 新上传到达后，用已确认游标开启下一轮：只交付新批次
curl -X POST -F "file=@b3.csv" http://localhost:8000/stations/ST900/days/2026-11-12/batches
curl "http://localhost:8000/stations/ST900/pull?cursor=$C2"           # 仅 b3
```

`./demo.sh` 的第 7–10 步对运行中的服务完整演示上述流程（含逐字节比对）。

## 并发与一致性设计（对应需求逐条）

1. **每次上传独立保留**：每次上传生成 `B-...` 批次，各自一个 Parquet 文件
   与一行元数据；查询结果里的 `source_batch_id` 保留每条记录的来源批次。
2. **查询真读 Parquet**：`GET .../records` 通过 `pyarrow.parquet.read_table`
   读取快照内每个文件并 `concat_tables` 拼接，不读缓存表。
3. **合并期间读写不中断**：合并在临时区构建新文件，完成前不触碰已发布集合；
   上传走独立文件 + 独立元数据行，全程可继续。
4. **只替换启动时选中的文件**：合并开始即固定源批次集合（写入
   `merge_locks` 并按 `rowid` 排序选取）；合并期间新上传的批次不在集合内，
   发布事务只 supersede 这批固定源，新批次继续保持 published。
5. **查询全程同一文件集合**：查询开始时选取已发布文件并对每个文件加引用计数，
   关闭快照前集合不变；即使合并在查询中途完成，查询仍读旧文件。
6. **旧文件等查询结束再清理**：合并只把旧批次标记 `superseded`，不立即删文件；
   后台 reaper（或 `/admin/reap`）仅删除引用计数为零的旧文件。
7. **同站日并发合并不重复纳入**：`merge_locks` 对 `(station_id, day)` 加主键行锁，
   第二个并发合并立即得到 `409 MergeConflictError`，不会把同一批数据纳入两次。
8. **合并失败不改已发布集合、半成品不可查**：合并文件先落临时/独立路径，
   元数据在单个 `BEGIN IMMEDIATE` 事务里完成“插入合并批次 + supersede 源”，
   任一步失败即回滚并删除半成品；半成品没有元数据行，永远不会出现在查询快照中。
   进程崩溃后重启时 `startup_sweep` 清理临时文件、无元数据的孤儿 Parquet 与残留锁。
9. **增量拉取不漏批、不重复**：拉取页是不可变元数据上 `(after, bound]`
   的切片（`batches.rowid` 提交序，只含 `is_merged=0` 的原上传批次）；
   上界在轮次开始即固定，并发上传落入下一轮，合并不进入流，清理只动
   已 pin 之外的文件，因此上传/合并/清理并行时交付集合确定。
10. **拉取读取全有或全无**：页内文件先按谱系解析到当前已发布文件并加
    引用计数再读；若合并+清理在解析与读取之间移走了文件，重新解析谱系
    重试（有界次数）；任何读取失败返回 `503`，不返回部分页、不推进游标，
    同一游标重试得到相同内容。

### 边界说明

- 当前为**单容器/单进程**部署：引用计数与进程内写锁在进程内生效，SQLite 行锁
  提供持久化的合并互斥。多副本水平扩展需把文件存储换成 S3 之类共享存储、把
  引用计数与锁迁移到数据库/Redis，本仓库未包含该拓扑。
- 单文件上传大小受 `RAIN_MAX_UPLOAD_BYTES` 限制（默认 50 MiB）。

## 环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RAIN_DATA_DIR` | `./data` | 数据根目录（SQLite、Parquet、临时文件） |
| `RAIN_DB_PATH` | `$RAIN_DATA_DIR/metadata.sqlite3` | SQLite 路径 |
| `RAIN_REAPER_INTERVAL` | `5` | 旧文件清理轮询间隔（秒） |
| `RAIN_MAX_UPLOAD_BYTES` | `52428800` | 单次上传字节上限 |
