"""FastAPI entrypoint: upload / merge / query API for rain-gauge batches."""
from __future__ import annotations

import contextlib
from typing import List, Optional

from fastapi import FastAPI, File, HTTPException, Path as PathParam, Query, UploadFile
from pydantic import BaseModel

from . import db, replay, services
from .config import settings
from .errors import StorageError
from .storage import file_manager


# ----------------------------------------------------------------- schemas
class BatchResponse(BaseModel):
    batch_id: str
    station_id: str
    day: str
    row_count: int
    size_bytes: int
    content_sha256: str
    status: str
    is_merged: bool


class MergeResponse(BaseModel):
    merged_batch_id: str
    station_id: str
    day: str
    source_batch_ids: List[str]
    source_count: int
    row_count: int
    size_bytes: int
    content_sha256: str
    status: str


class RainRecord(BaseModel):
    station_id: str
    obs_time: str
    rain_mm: float
    source_batch_id: str


class QueryResponse(BaseModel):
    station_id: str
    day: str
    snapshot_token: str
    file_count: int
    batch_ids: List[str]
    row_count: int
    truncated: bool
    records: List[RainRecord]


class BatchInfo(BaseModel):
    batch_id: str
    row_count: int
    size_bytes: int
    status: str
    is_merged: bool
    source_batches: Optional[List[str]]
    file_exists: bool
    created_at: str


class ListBatchesResponse(BaseModel):
    station_id: str
    day: str
    batches: List[BatchInfo]


class PulledBatch(BaseModel):
    seq: int
    batch_id: str
    day: str
    row_count: int
    records: List[RainRecord]


class PullResponse(BaseModel):
    station_id: str
    round_bound: int
    round_complete: bool
    batch_count: int
    batches: List[PulledBatch]
    next_cursor: str


# ---------------------------------------------------------------- app setup
@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.parquet_root.mkdir(parents=True, exist_ok=True)
    settings.tmp_dir.mkdir(parents=True, exist_ok=True)
    db.init_db()
    file_manager.startup_sweep()
    file_manager.start_reaper()
    yield
    file_manager.stop_reaper()


app = FastAPI(
    title="Rain Gauge Batch Storage API",
    version="1.0.0",
    description=(
        "Upload per-station/per-day rain CSV batches, compact them into one "
        "Parquet file, and query published files with snapshot consistency."
    ),
    lifespan=lifespan,
)


@app.exception_handler(StorageError)
async def storage_error_handler(request, exc: StorageError):
    from fastapi.responses import JSONResponse

    return JSONResponse(
        status_code=exc.status_code,
        content={"error": type(exc).__name__, "detail": str(exc)},
    )


STATION_PATH = PathParam(..., pattern=r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")
DAY_PATH = PathParam(..., pattern=r"^\d{4}-\d{2}-\d{2}$")


# ----------------------------------------------------------------- endpoints
@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "data_dir": str(settings.data_dir)}


@app.post(
    "/stations/{station_id}/days/{day}/batches",
    response_model=BatchResponse,
    status_code=201,
    tags=["upload"],
    summary="Upload one station/day CSV as an independent batch",
)
async def upload_batch(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
    file: UploadFile = File(..., description="CSV with station, UTC time, rain_mm"),
) -> BatchResponse:
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="uploaded file is empty")
    if len(content) > settings.max_upload_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"file exceeds {settings.max_upload_bytes} bytes",
        )
    result = services.upload_csv(station_id, day, content)
    return BatchResponse(**result)


@app.get(
    "/stations/{station_id}/days/{day}/records",
    response_model=QueryResponse,
    tags=["query"],
    summary="Query a station/day by actually reading published Parquet files",
)
def query_records(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
    limit: Optional[int] = Query(None, ge=0, description="optional row cap"),
) -> QueryResponse:
    result = services.query_station_day(station_id, day, limit=limit)
    return QueryResponse(**result)


@app.post(
    "/stations/{station_id}/days/{day}/merge",
    response_model=MergeResponse,
    tags=["merge"],
    summary="Compact published small batches into one Parquet file",
)
def merge_day(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
) -> MergeResponse:
    result = services.merge_station_day(station_id, day)
    return MergeResponse(**result)


@app.get(
    "/stations/{station_id}/days/{day}/batches",
    response_model=ListBatchesResponse,
    tags=["query"],
    summary="List all batches (published and superseded) for a station/day",
)
def get_batches(
    station_id: str = STATION_PATH,
    day: str = DAY_PATH,
) -> ListBatchesResponse:
    return ListBatchesResponse(**services.list_batches(station_id, day))


@app.post("/admin/reap", tags=["ops"], summary="Force reaping of unreferenced superseded files")
def force_reap() -> dict:
    return services.force_reap()


@app.get(
    "/stations/{station_id}/pull",
    response_model=PullResponse,
    tags=["pull"],
    summary="Incrementally pull original upload batches in ingestion order",
    description=(
        "Replay feed for a downstream sink: pages of original uploads "
        "(merges are not new uploads) in ingestion order. Each round fixes "
        "its ingestion upper bound at request start; when the bound is "
        "reached, `next_cursor` opens a new round that includes later "
        "uploads. Retrying with the same cursor returns the same page, even "
        "after the original batches were merged and their files reaped. A "
        "read failure returns 503 with no partial page and no new cursor."
    ),
)
def pull_station(
    station_id: str = STATION_PATH,
    cursor: Optional[str] = Query(
        None, description="opaque continuation cursor from a previous page"
    ),
    limit: int = Query(1, ge=1, le=100, description="max batches per page"),
) -> PullResponse:
    return PullResponse(**replay.pull_batches(station_id, cursor, limit))
