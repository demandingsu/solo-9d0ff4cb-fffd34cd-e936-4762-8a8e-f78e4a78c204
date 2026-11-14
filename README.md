# 雨量站批次存储 API（FastAPI + PyArrow + SQLite）

把雨量站频繁上传的小批 CSV 观测持久化为独立 Parquet 批次，按站日合并成
单个日文件，供暴雨复盘脚本以**快照一致**的方式读取。

- **Web 框架**：FastAPI（同步端点，跑在 threadpool 上，避免阻塞事件循环）
- **列存文件**：PyArrow / Parquet（zstd 压缩，UTC 毫秒时间戳）
- **元数据**：SQLite（WAL 模式，`BEGIN IMMEDIATE` 事务 + 进程内写锁）
- **交付**：可直接启动的 Docker Compose、本地样例 CSV、一键演示脚本、13 个测试

## 目录结构

```
app/
  config.py      环境变量与路径配置（RAIN_DATA_DIR 一处切换存储位置）
  db.py          SQLite 连接、WAL/忙等待、串行化写事务、表结构
  errors.py      领域异常 -> HTTP 状态码映射
  ingestion.py   CSV 解析/校验、规范 Parquet schema、记录去留（不去重、不排序）
  storage.py     Parquet 原子写、查询快照与引用计数、后台清理、崩溃恢复扫描
  services.py    上传 / 查询 / 合并 / 列表 的事务编排
  main.py        FastAPI 路由与生命周期
samples/         本地雨量 CSV 样例（含中文表头样例与故意保留的重复行）
tests/           端到端与并发测试（pytest + TestClient + 线程池）
demo.sh          对运行中的服务执行 上传→查询→合并→冲突→清理 全流程
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
