#!/usr/bin/env bash
# End-to-end demo: upload -> query -> merge (incl. concurrent-merge conflict)
# -> query -> reap. Works against a locally running server (docker compose or
# uvicorn). Override the base URL with RAIN_BASE_URL.
set -euo pipefail

BASE_URL="${RAIN_BASE_URL:-http://127.0.0.1:8000}"
# Station id matches the station_id column inside samples/*.csv.
STATION="ST001"
DAY="2026-11-12"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLES="${SCRIPT_DIR}/samples"

pp() { python3 -c "
import sys, json
raw = sys.stdin.read()
try:
    print(json.dumps(json.loads(raw), ensure_ascii=False, indent=2))
except json.JSONDecodeError:
    print(raw)
"; }

echo "### 0. health"
curl -fsS "${BASE_URL}/health" | pp

echo; echo "### 1. upload three independent batches (batch 2 has a deliberate duplicate row)"
for f in \
  "${SAMPLES}/st001_2026-11-12_batch1.csv" \
  "${SAMPLES}/st001_2026-11-12_batch2.csv" \
  "${SAMPLES}/st001_2026-11-12_batch3_zh.csv"; do
  curl -fsS -X POST -F "file=@${f};filename=$(basename "$f")" \
    "${BASE_URL}/stations/${STATION}/days/${DAY}/batches" | pp
done

echo; echo "### 2. query: reads the three published Parquet files"
curl -fsS "${BASE_URL}/stations/${STATION}/days/${DAY}/records" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(json.dumps({'file_count':d['file_count'],'row_count':d['row_count'],'batch_ids':d['batch_ids']}, indent=2))"

echo; echo "### 3. two concurrent merges: exactly one succeeds, the other gets HTTP 409"
curl -s -o /tmp/rain-demo-m1.json -w "merge A http=%{http_code}\n" -X POST \
  "${BASE_URL}/stations/${STATION}/days/${DAY}/merge" &
curl -s -o /tmp/rain-demo-m2.json -w "merge B http=%{http_code}\n" -X POST \
  "${BASE_URL}/stations/${STATION}/days/${DAY}/merge" &
wait
echo "merge A response:"; cat /tmp/rain-demo-m1.json | pp 2>/dev/null || cat /tmp/rain-demo-m1.json
echo "merge B response:"; cat /tmp/rain-demo-m2.json | pp 2>/dev/null || cat /tmp/rain-demo-m2.json

echo; echo "### 4. query after merge: one file, every original row kept (duplicates included)"
curl -fsS "${BASE_URL}/stations/${STATION}/days/${DAY}/records" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
dupes = [r for r in d['records'] if r['obs_time'] == '2026-11-12T08:01:00Z']
print(json.dumps({'file_count': d['file_count'], 'row_count': d['row_count'],
                  'duplicate_08:01_rows': len(dupes)}, indent=2))"

echo; echo "### 5. batch listing shows published vs superseded"
curl -fsS "${BASE_URL}/stations/${STATION}/days/${DAY}/batches" \
  | python3 -c "
import sys, json
for b in json.load(sys.stdin)['batches']:
    print(f\"  {b['batch_id']}  status={b['status']:<10} rows={b['row_count']:<3} file_exists={b['file_exists']}\")"

echo; echo "### 6. reap: superseded files are deleted only after queries release them"
curl -fsS -X POST "${BASE_URL}/admin/reap" | pp
echo "final batch listing:"
curl -fsS "${BASE_URL}/stations/${STATION}/days/${DAY}/batches" \
  | python3 -c "
import sys, json
for b in json.load(sys.stdin)['batches']:
    print(f\"  {b['batch_id']}  status={b['status']:<10} file_exists={b['file_exists']}\")"

# ---------------------------------------------------------------------------
# Incremental pull: resume from the confirmed batch after merge + cleanup.
# ---------------------------------------------------------------------------
PULL_STATION="ST900"
DEMO_TMP="$(mktemp -d)"
trap 'rm -rf "${DEMO_TMP}"' EXIT

cat > "${DEMO_TMP}/p1.csv" <<CSV
station_id,obs_time,rain_mm
${PULL_STATION},2026-11-12T00:00Z,0.0
${PULL_STATION},2026-11-12T00:01Z,0.2
CSV
cat > "${DEMO_TMP}/p2.csv" <<CSV
station_id,obs_time,rain_mm
${PULL_STATION},2026-11-12T08:00Z,1.1
${PULL_STATION},2026-11-12T08:01Z,2.4
${PULL_STATION},2026-11-12T08:01Z,2.4
CSV
cat > "${DEMO_TMP}/p3.csv" <<CSV
station_id,obs_time,rain_mm
${PULL_STATION},2026-11-12T23:00Z,9.9
CSV

jget() { python3 -c "import sys, json; print(json.load(sys.stdin)$1)"; }

echo; echo "### 7. incremental pull: upload two batches, pull page 1 (round bound fixed)"
curl -fsS -X POST -F "file=@${DEMO_TMP}/p1.csv" \
  "${BASE_URL}/stations/${PULL_STATION}/days/${DAY}/batches" | pp
curl -fsS -X POST -F "file=@${DEMO_TMP}/p2.csv" \
  "${BASE_URL}/stations/${PULL_STATION}/days/${DAY}/batches" | pp

PAGE1="$(curl -fsS "${BASE_URL}/stations/${PULL_STATION}/pull")"
echo "page 1:"; echo "${PAGE1}" | pp
CURSOR1="$(echo "${PAGE1}" | jget "['next_cursor']")"
PAGE2="$(curl -fsS "${BASE_URL}/stations/${PULL_STATION}/pull?cursor=${CURSOR1}")"
echo "page 2 (round completes, cursor opens the next round):"
echo "${PAGE2}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(json.dumps({'round_bound': d['round_bound'], 'round_complete': d['round_complete'],
                  'batch_ids': [b['batch_id'] for b in d['batches']]}, indent=2))"
CURSOR2="$(echo "${PAGE2}" | jget "['next_cursor']")"

echo; echo "### 8. merge + reap: the original Parquet files are deleted"
curl -fsS -X POST "${BASE_URL}/stations/${PULL_STATION}/days/${DAY}/merge" \
  | python3 -c "import sys, json; d=json.load(sys.stdin); print('merged_batch_id:', d['merged_batch_id'], ' sources:', d['source_batch_ids'])"
curl -fsS -X POST "${BASE_URL}/admin/reap" | pp
curl -fsS "${BASE_URL}/stations/${PULL_STATION}/days/${DAY}/batches" \
  | python3 -c "
import sys, json
for b in json.load(sys.stdin)['batches']:
    print(f\"  {b['batch_id']}  status={b['status']:<10} file_exists={b['file_exists']}\")"

echo; echo "### 9. resume with the pre-merge cursor: rows recovered from the merged file"
RETRY2="$(curl -fsS "${BASE_URL}/stations/${PULL_STATION}/pull?cursor=${CURSOR1}")"
if [ "${RETRY2}" = "${PAGE2}" ]; then
  echo "OK: same cursor returned an identical page after merge+cleanup"
else
  echo "MISMATCH between pre/post-merge pages"; exit 1
fi
echo "${RETRY2}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for b in d['batches']:
    print(f\"  batch {b['batch_id']} rows={b['row_count']}\")
    for r in b['records']:
        print(f\"    {r['obs_time']}  rain={r['rain_mm']}  source={r['source_batch_id']}\")"

echo; echo "### 10. new upload lands after the round; next round from the confirmed cursor sees only it"
curl -fsS -X POST -F "file=@${DEMO_TMP}/p3.csv" \
  "${BASE_URL}/stations/${PULL_STATION}/days/${DAY}/batches" | pp
curl -fsS "${BASE_URL}/stations/${PULL_STATION}/pull?cursor=${CURSOR2}" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(json.dumps({'round_bound': d['round_bound'], 'round_complete': d['round_complete'],
                  'batch_ids': [b['batch_id'] for b in d['batches']],
                  'obs_times': [r['obs_time'] for b in d['batches'] for r in b['records']]}, indent=2))"
