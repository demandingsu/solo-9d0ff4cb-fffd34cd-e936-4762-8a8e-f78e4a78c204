"""Concurrency guarantees: merge locking, concurrent uploads, snapshot pinning."""
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow.parquet as pq

from app import services
from app.storage import file_manager
from tests.conftest import make_csv


def _seed(station, day, n_batches=3, rows_each=2):
    for i in range(n_batches):
        services.upload_csv(
            station,
            day,
            make_csv(
                [
                    (f"2026-11-12T{i:02d}:00Z", float(i)),
                    (f"2026-11-12T{i:02d}:01Z", float(i) + 0.5),
                ],
                station,
            ),
        )


def test_concurrent_merges_one_wins_one_conflicts():
    station, day = "CM1", "2026-11-12"
    _seed(station, day)
    results = []
    errors = []
    barrier = threading.Barrier(2)

    def run_merge():
        barrier.wait()
        try:
            results.append(services.merge_station_day(station, day))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run_merge) for _ in range(2)]
        for f in futures:
            f.result(timeout=30)

    assert len(results) == 1
    assert len(errors) == 1
    assert type(errors[0]).__name__ == "MergeConflictError"

    # Exactly one published file and no lost/duplicated rows.
    body = services.query_station_day(station, day)
    assert body["file_count"] == 1
    assert body["row_count"] == 6
    # Lock row must be released afterwards.
    from app import db

    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM merge_locks").fetchone()["c"] == 0
    finally:
        conn.close()


def test_uploads_during_merge_are_kept_outside_merged_file():
    station, day = "CM2", "2026-11-12"
    _seed(station, day, n_batches=2)  # 4 rows

    original_write = file_manager.write_parquet_atomic
    proceed = threading.Event()

    def blocking_write(table, s, d, batch_id):
        # Only block the merged file; uploads must keep flowing concurrently.
        if batch_id.startswith("M"):
            proceed.wait(timeout=10)
        return original_write(table, s, d, batch_id)

    file_manager.write_parquet_atomic = blocking_write
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            merge_future = pool.submit(services.merge_station_day, station, day)
            # Give the merge time to select its source set and start building.
            for _ in range(100):
                if _lock_held(station, day):
                    break
                threading.Event().wait(0.02)

            late = services.upload_csv(
                station, day, make_csv([("2026-11-12T23:00Z", 9.9)], station)
            )
            proceed.set()
            merged = merge_future.result(timeout=30)
    finally:
        file_manager.write_parquet_atomic = original_write

    assert late["batch_id"] not in merged["source_batch_ids"]

    body = services.query_station_day(station, day)
    # The late batch remains an independent published file next to the merge.
    assert body["file_count"] == 2
    assert body["row_count"] == 5
    assert late["batch_id"] in body["batch_ids"]
    assert merged["merged_batch_id"] in body["batch_ids"]

    # A follow-up merge absorbs the late batch; rows never duplicated.
    second = services.merge_station_day(station, day)
    assert set(second["source_batch_ids"]) == {
        merged["merged_batch_id"],
        late["batch_id"],
    }
    assert second["row_count"] == 5
    body = services.query_station_day(station, day)
    assert body["file_count"] == 1
    assert body["row_count"] == 5


def test_snapshot_pins_old_files_until_query_finishes():
    station, day = "CM3", "2026-11-12"
    _seed(station, day)
    original_read = file_manager.read_table
    inside_read = threading.Event()
    release_read = threading.Event()
    seen_paths = []

    def slow_read(refs):
        seen_paths.extend(r.path for r in refs)
        inside_read.set()
        release_read.wait(timeout=10)
        return original_read(refs)

    file_manager.read_table = slow_read
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            query_future = pool.submit(services.query_station_day, station, day)
            assert inside_read.wait(timeout=10)

            merged = services.merge_station_day(station, day)
            # Merge has swapped the published set, but the old files survive
            # because the in-flight query still references them.
            old_paths = [
                file_manager.target_path(station, day, bid)
                for bid in merged["source_batch_ids"]
            ]
            assert all(p.exists() for p in old_paths)
            reaped = set(file_manager.reap_once())
            assert reaped.isdisjoint(merged["source_batch_ids"])

            release_read.set()
            result = query_future.result(timeout=10)
    finally:
        file_manager.read_table = original_read

    # The query read the pre-merge file set.
    assert set(result["batch_ids"]) == set(merged["source_batch_ids"])
    assert result["row_count"] == 6

    # After the query closes, the reaper can remove the superseded files.
    reaped = set(file_manager.reap_once())
    assert set(merged["source_batch_ids"]).issubset(reaped)
    assert all(not p.exists() for p in old_paths)


def test_merge_failure_leaves_published_set_unchanged():
    station, day = "CM4", "2026-11-12"
    _seed(station, day)
    before = services.query_station_day(station, day)

    def boom(*args, **kwargs):
        raise RuntimeError("injected parquet write failure")

    original_write = file_manager.write_parquet_atomic
    file_manager.write_parquet_atomic = boom
    try:
        try:
            services.merge_station_day(station, day)
            assert False, "expected MergeFailedError"
        except Exception as exc:  # noqa: BLE001
            assert type(exc).__name__ == "MergeFailedError"
    finally:
        file_manager.write_parquet_atomic = original_write

    after = services.query_station_day(station, day)
    assert after["batch_ids"] == before["batch_ids"]
    assert after["row_count"] == before["row_count"] == 6
    assert after["file_count"] == 3
    # No half-finished files or temp leftovers remain.
    assert list(file_manager.tmp_dir.glob("*.tmp-*")) == []
    parquet_files = list(file_manager.station_day_dir(station, day).glob("*.parquet"))
    assert len(parquet_files) == 3

    from app import db

    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM merge_locks").fetchone()["c"] == 0
    finally:
        conn.close()

    # The station/day can still be merged successfully after the failure.
    merged = services.merge_station_day(station, day)
    body = services.query_station_day(station, day)
    assert body["file_count"] == 1 and body["row_count"] == 6
    assert merged["row_count"] == 6


def _lock_held(station, day):
    from app import db

    conn = db.connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM merge_locks WHERE station_id = ? AND day = ?",
            (station, day),
        ).fetchone()
        return row is not None
    finally:
        conn.close()
