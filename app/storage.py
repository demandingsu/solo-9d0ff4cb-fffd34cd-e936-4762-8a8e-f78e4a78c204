"""Parquet file lifecycle management.

Responsibilities:

* deterministic on-disk layout (``parquet/STATION/DAY/*.parquet``)
* atomic write via temp file + ``fsync`` + ``os.replace``
* reference counting of *published* file snapshots so a query pins every file
  it reads until it finishes
* background reaping of superseded files whose references have drained

Invariant: a Parquet file becomes queryable only after its metadata row is
committed in SQLite. A half-written file never appears in a snapshot.
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from . import db
from .config import Settings, settings


@dataclass(frozen=True)
class FileRef:
    batch_id: str
    path: Path
    row_count: int
    is_merged: bool
    created_at: str


class FileManager:
    def __init__(self, cfg: Settings = settings) -> None:
        self.cfg = cfg
        self.parquet_root = cfg.parquet_root
        self.tmp_dir = cfg.tmp_dir
        # station/day -> reference count. One lock guards the registry and the
        # snapshot selection, so "register query refs" and "check refs before
        # unlink" cannot interleave.
        self._ref_lock = threading.Lock()
        self._refs: dict[str, int] = {}
        self._reaper_stop = threading.Event()
        self._reaper_thread: threading.Thread | None = None

    # ------------------------------------------------------------------ paths
    def station_day_dir(self, station_id: str, day: str) -> Path:
        return self.parquet_root / station_id / day

    def target_path(self, station_id: str, day: str, batch_id: str) -> Path:
        return self.station_day_dir(station_id, day) / f"{batch_id}.parquet"

    def temp_path(self, batch_id: str) -> Path:
        return self.tmp_dir / f"{batch_id}.tmp-{os.getpid()}-{threading.get_ident()}"

    # ------------------------------------------------------------- atomic IO
    def write_parquet_atomic(
        self,
        table: pa.Table,
        station_id: str,
        day: str,
        batch_id: str,
    ) -> tuple[Path, int, int]:
        """Write a Parquet file into its final location atomically.

        Returns ``(path, row_count, size_bytes)``. The file is invisible to
        queries until a metadata row is committed separately.
        """
        final_path = self.target_path(station_id, day, batch_id)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.temp_path(batch_id)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        try:
            pq.write_table(table, tmp, compression="zstd", version="2.6")
            with open(tmp, "rb") as fh:
                os.fsync(fh.fileno())
            os.replace(tmp, final_path)  # atomic within one filesystem
            # fsync the directory so the rename is durable
            dir_fd = os.open(final_path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
            size = final_path.stat().st_size
            return final_path, table.num_rows, size
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    def read_table(self, refs: List[FileRef]) -> pa.Table:
        """Read and concatenate Parquet files.

        ``pyarrow.concat_tables`` preserves every row, including exact
        duplicates, in the order the refs are given (publish order).
        """
        if not refs:
            return self.empty_table()
        tables = [pq.read_table(r.path) for r in refs]
        return pa.concat_tables(tables, promote_options="default")

    @staticmethod
    def empty_table() -> pa.Table:
        return pa.table(
            {
                "station_id": pa.array([], pa.string()),
                "obs_time": pa.array([], pa.timestamp("ms", tz="UTC")),
                "rain_mm": pa.array([], pa.float64()),
                "source_batch_id": pa.array([], pa.string()),
            }
        )

    # ------------------------------------------------------------ snapshots
    def open_snapshot(self, station_id: str, day: str) -> tuple[List[FileRef], str]:
        """Select the currently published file set and pin it.

        The same file set is used from start to end of a query because the
        reference is held for the query's whole lifetime.
        """
        conn = db.connect()
        try:
            rows = conn.execute(
                "SELECT batch_id, file_path, row_count, is_merged, created_at "
                "FROM batches "
                "WHERE station_id = ? AND day = ? AND status = 'published' "
                "ORDER BY created_at ASC, rowid ASC",
                (station_id, day),
            ).fetchall()
        finally:
            conn.close()

        refs = [
            FileRef(
                batch_id=r["batch_id"],
                path=Path(r["file_path"]),
                row_count=r["row_count"],
                is_merged=bool(r["is_merged"]),
                created_at=r["created_at"],
            )
            for r in rows
        ]
        keys = [r.path.name for r in refs]
        with self._ref_lock:
            for key in keys:
                self._refs[key] = self._refs.get(key, 0) + 1
        token = f"{station_id}/{day}@{time.time_ns()}"
        return refs, token

    def close_snapshot(self, refs: List[FileRef]) -> None:
        with self._ref_lock:
            for r in refs:
                key = r.path.name
                count = self._refs.get(key, 0)
                if count <= 1:
                    self._refs.pop(key, None)
                else:
                    self._refs[key] = count - 1

    def _active_ref_names(self) -> set[str]:
        with self._ref_lock:
            return set(self._refs.keys())

    # --------------------------------------------------------------- reaper
    def reap_once(self) -> list[str]:
        """Delete superseded files that no query still references."""
        reaped: list[str] = []
        conn = db.connect()
        try:
            rows = conn.execute(
                "SELECT batch_id, file_path FROM batches "
                "WHERE status = 'superseded' AND reaped = 0"
            ).fetchall()
        finally:
            conn.close()

        active = self._active_ref_names()
        for row in rows:
            path = Path(row["file_path"])
            if path.name in active:
                continue  # a query started before the merge is still reading it
            path.unlink(missing_ok=True)
            reaped.append(row["batch_id"])
        if reaped:
            with db.transaction() as tx:
                tx.executemany(
                    "UPDATE batches SET reaped = 1 WHERE batch_id = ?",
                    [(bid,) for bid in reaped],
                )
        return reaped

    def _reaper_loop(self) -> None:
        while not self._reaper_stop.wait(self.cfg.reaper_interval):
            try:
                self.reap_once()
            except Exception:  # pragma: no cover - background safety net
                pass

    def start_reaper(self) -> None:
        if self._reaper_thread is not None:
            return
        self._reaper_stop.clear()
        self._reaper_thread = threading.Thread(
            target=self._reaper_loop, name="parquet-reaper", daemon=True
        )
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        self._reaper_stop.set()
        if self._reaper_thread is not None:
            self._reaper_thread.join(timeout=10)
            self._reaper_thread = None

    # -------------------------------------------------------------- recovery
    def startup_sweep(self) -> dict[str, list[str]]:
        """Clean leftovers from a previous process.

        * temp files from a crashed upload/merge are removed (never published,
          therefore never queryable);
        * Parquet files without a metadata row are removed (half-finished
          output of a crashed merge);
        * stale merge locks are released.
        """
        removed_tmp: list[str] = []
        removed_orphans: list[str] = []

        if self.tmp_dir.exists():
            for tmp in self.tmp_dir.glob("*.tmp-*"):
                tmp.unlink(missing_ok=True)
                removed_tmp.append(str(tmp))

        conn = db.connect()
        try:
            known = {
                Path(r["file_path"]).resolve()
                for r in conn.execute("SELECT file_path FROM batches").fetchall()
            }
        finally:
            conn.close()

        if self.parquet_root.exists():
            for path in self.parquet_root.rglob("*.parquet"):
                if path.resolve() not in known:
                    path.unlink(missing_ok=True)
                    removed_orphans.append(str(path))

        with db.transaction() as tx:
            tx.execute("DELETE FROM merge_locks")

        return {"temp_files": removed_tmp, "orphan_parquet": removed_orphans}


file_manager = FileManager()
