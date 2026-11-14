"""Incremental pull of original upload batches (the replay feed).

A downstream sink (复盘库) consumes every *original* upload for one station in
ingestion order, resumes after a disconnect from the last confirmed batch, and
never sees a merge as a new upload.

Cursor semantics
----------------
A cursor is an opaque, URL-safe token::

    {"v": 1, "st": "<station_id>", "after": <seq>, "bound": <seq|null>}

* ``after`` — ingestion sequence (``batches.rowid``) of the last confirmed
  original batch. Everything up to and including ``after`` is acknowledged.
* ``bound`` — the round's fixed ingestion upper bound, captured when the
  round started. ``null`` means "start a new round from ``after``": the
  server fixes a fresh bound at request time so uploads that landed since
  the previous round are included.

A request without a cursor starts the first round at ``after = 0``.

Guarantees
----------
* **Idempotent pages.** Retrying with the same cursor returns the same page.
  Batch metadata rows are immutable and every batch's rows remain recoverable
  from the published file set — its own Parquet file, or the merged file that
  absorbed it (rows are filtered back out by ``source_batch_id``; duplicates
  and original row order are preserved).
* **No loss, no duplication under concurrency.** Pages are slices
  ``(after, bound]`` of immutable metadata, so concurrent uploads (higher
  seq, next round), merges (not in the stream) and reaping (pinned files
  only) cannot drop or repeat a batch.
* **All-or-nothing pages.** If any file read fails the request fails with
  503 and returns no partial page and no new cursor; the client retries with
  the cursor it already holds.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import List, Optional

import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import db
from .errors import CursorError, PullReadError
from .ingestion import table_to_records, validate_station_id
from .storage import FileManager, file_manager

_CURSOR_VERSION = 1
# Lineage re-resolution retries when a merge+reap moves a file mid-read.
_MAX_ATTEMPTS = 8


# ------------------------------------------------------------------- cursor
def encode_cursor(station_id: str, after: int, bound: Optional[int]) -> str:
    payload = {"v": _CURSOR_VERSION, "st": station_id, "after": after, "bound": bound}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(token: str, station_id: str) -> tuple[int, Optional[int]]:
    try:
        padded = token + "=" * (-len(token) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")))
    except Exception as exc:  # any decode failure means a malformed cursor
        raise CursorError("malformed cursor") from exc
    if not isinstance(payload, dict) or payload.get("v") != _CURSOR_VERSION:
        raise CursorError("unsupported cursor version")
    if payload.get("st") != station_id:
        raise CursorError("cursor was issued for a different station")
    after, bound = payload.get("after"), payload.get("bound")
    if not isinstance(after, int) or isinstance(after, bool) or after < 0:
        raise CursorError("cursor has an invalid 'after' position")
    if bound is not None and (
        not isinstance(bound, int) or isinstance(bound, bool) or bound < after
    ):
        raise CursorError("cursor has an invalid 'bound'")
    return after, bound


# ------------------------------------------------------------ lineage lookup
def _resolve_locations(station_id: str, batch_ids: List[str]) -> dict[str, Path]:
    """Map each original batch to the published file currently holding its rows.

    A still-published batch is read from its own file. A superseded batch is
    followed through ``source_batches`` lineage (merge of a merge is handled)
    until the published merged file that absorbed it is found.
    """
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT batch_id, file_path, status, source_batches FROM batches "
            "WHERE station_id = ?",
            (station_id,),
        ).fetchall()
    finally:
        conn.close()

    by_id = {r["batch_id"]: r for r in rows}
    container_of: dict[str, str] = {}
    for r in rows:
        if r["source_batches"]:
            for src in json.loads(r["source_batches"]):
                container_of[src] = r["batch_id"]

    locations: dict[str, Path] = {}
    for bid in batch_ids:
        cur = bid
        for _ in range(len(by_id) + 1):
            row = by_id.get(cur)
            if row is None:
                raise PullReadError(f"lineage broken: batch {cur} has no metadata")
            if row["status"] == "published":
                locations[bid] = Path(row["file_path"])
                break
            nxt = container_of.get(cur)
            if nxt is None:
                raise PullReadError(
                    f"lineage broken: batch {cur} is superseded but no merge absorbed it"
                )
            cur = nxt
        else:
            raise PullReadError(f"lineage cycle detected at batch {bid}")
    return locations


# --------------------------------------------------------------- page reads
def _read_pages(
    station_id: str, page_rows: List[dict], fm: FileManager
) -> List[dict]:
    """Read one page of batches from the published file set, all or nothing.

    Files are pinned before reading so the reaper cannot unlink them. If a
    merge+reap moves a file between lineage resolution and the read, the
    lineage is re-resolved and retried; any other read failure aborts the
    whole page (no partial delivery).
    """
    wanted = [r["batch_id"] for r in page_rows]
    expected = {r["batch_id"]: r["row_count"] for r in page_rows}
    last_exc: Optional[Exception] = None

    for _ in range(_MAX_ATTEMPTS):
        locations = _resolve_locations(station_id, wanted)
        paths = sorted(set(locations.values()))
        fm.pin_files(paths)
        try:
            if any(not p.exists() for p in paths):
                # Lineage moved between resolve and pin; re-resolve.
                continue
            tables = {p: pq.read_table(p) for p in paths}
        except FileNotFoundError as exc:
            # Lost the race with the reaper; re-resolve and retry.
            last_exc = exc
            continue
        except Exception as exc:
            raise PullReadError(f"failed to read published files: {exc}") from exc
        finally:
            fm.unpin_files(paths)

        pages: List[dict] = []
        for row in page_rows:
            bid = row["batch_id"]
            table = tables[locations[bid]]
            owned = table.filter(pc.equal(table.column("source_batch_id"), bid))
            if owned.num_rows != expected[bid]:
                raise PullReadError(
                    f"batch {bid}: expected {expected[bid]} rows, found "
                    f"{owned.num_rows} in {locations[bid].name}"
                )
            pages.append(
                {
                    "seq": row["seq"],
                    "batch_id": bid,
                    "day": row["day"],
                    "row_count": owned.num_rows,
                    "records": table_to_records(owned),
                }
            )
        return pages

    raise PullReadError(
        f"could not pin a consistent file set after {_MAX_ATTEMPTS} attempts"
        + (f": {last_exc}" if last_exc else "")
    )


# -------------------------------------------------------------- pull rounds
def pull_batches(
    station_id: str,
    cursor_token: Optional[str] = None,
    limit: int = 1,
    fm: FileManager = file_manager,
) -> dict:
    """Return the next page of original upload batches for one station.

    Ingestion order is the metadata commit order (``batches.rowid``); merged
    batches are not part of the stream. With no cursor (or a cursor whose
    ``bound`` is null) a new round starts: the ingestion upper bound is fixed
    at this moment, and later uploads wait for the next round.
    """
    validate_station_id(station_id)
    if limit < 1:
        raise CursorError("limit must be >= 1")
    if cursor_token:
        after, bound = decode_cursor(cursor_token, station_id)
    else:
        after, bound = 0, None

    conn = db.connect()
    try:
        if bound is None:
            # Fix this round's ingestion upper bound at request start.
            bound = conn.execute(
                "SELECT COALESCE(MAX(rowid), 0) AS b FROM batches "
                "WHERE station_id = ? AND is_merged = 0",
                (station_id,),
            ).fetchone()["b"]
        rows = [
            dict(r)
            for r in conn.execute(
                "SELECT rowid AS seq, batch_id, day, row_count FROM batches "
                "WHERE station_id = ? AND is_merged = 0 AND rowid > ? AND rowid <= ? "
                "ORDER BY rowid ASC LIMIT ?",
                (station_id, after, bound, limit),
            ).fetchall()
        ]
        has_more = False
        if rows:
            has_more = (
                conn.execute(
                    "SELECT 1 FROM batches WHERE station_id = ? AND is_merged = 0 "
                    "AND rowid > ? AND rowid <= ? LIMIT 1",
                    (station_id, rows[-1]["seq"], bound),
                ).fetchone()
                is not None
            )
    finally:
        conn.close()

    pages = _read_pages(station_id, rows, fm) if rows else []

    if rows and has_more:
        # Same round continues: keep the fixed bound.
        next_cursor = encode_cursor(station_id, rows[-1]["seq"], bound)
        round_complete = False
    else:
        # Everything up to the bound is delivered; the next call with this
        # cursor opens a new round and picks up uploads that landed since.
        next_cursor = encode_cursor(station_id, bound, None)
        round_complete = True

    return {
        "station_id": station_id,
        "round_bound": bound,
        "round_complete": round_complete,
        "batch_count": len(pages),
        "batches": pages,
        "next_cursor": next_cursor,
    }
