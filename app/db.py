"""SQLite access primitives.

The metadata database is the *source of truth* for which Parquet files are
"published". Only rows committed here are visible to queries.

SQLite runs in WAL mode with a busy timeout. All writes are additionally
serialized in-process by ``write_lock`` so that transactions such as
"publish merged file + supersede old files" never interleave.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import settings

# Single in-process writer gate. FastAPI runs sync endpoints on a threadpool,
# so this lock is what makes metadata transactions atomic across requests.
write_lock = threading.RLock()


SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    station_id      TEXT NOT NULL,
    day             TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    size_bytes      INTEGER NOT NULL,
    content_sha256  TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('published', 'superseded')),
    is_merged       INTEGER NOT NULL DEFAULT 0,
    source_batches  TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    reaped          INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_batches_lookup
    ON batches (station_id, day, status);

CREATE TABLE IF NOT EXISTS merge_locks (
    station_id  TEXT NOT NULL,
    day         TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    locked_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (station_id, day)
);
"""


def connect() -> sqlite3.Connection:
    """Open a new connection (caller closes it)."""
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        settings.db_path,
        timeout=30.0,
        isolation_level=None,  # explicit BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    conn = connect()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Serialized immediate-write transaction."""
    conn = connect()
    # RLock + BEGIN IMMEDIATE: the first writer wins the SQLite reserved lock,
    # other threads wait on busy_timeout and are additionally gated in-process.
    with write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
