"""CSV ingestion and the canonical Parquet schema.

The uploaded CSV is parsed in full and validated before any file is written,
so a malformed upload cannot leave partial state behind.
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import date, datetime, timezone
from typing import Iterable

import pyarrow as pa

from .errors import CsvValidationError

# Canonical Parquet schema. obs_time is UTC minute-resolution timestamps;
# source_batch_id keeps provenance for every record inside merged files.
SCHEMA = pa.schema(
    [
        pa.field("station_id", pa.string(), nullable=False),
        pa.field("obs_time", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("rain_mm", pa.float64(), nullable=False),
        pa.field("source_batch_id", pa.string(), nullable=False),
    ]
)

def _normalize_header(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", name.strip().lower())


# Keys are normalized with the same rule as incoming headers so that
# "obs_time", "obs time" and "obstime" all map to the same canonical column.
COLUMN_ALIASES = {
    _normalize_header(k): v
    for k, v in {
        "station_id": "station_id",
        "stationid": "station_id",
        "station": "station_id",
        "站号": "station_id",
        "站点": "station_id",
        "站点编号": "station_id",
        "obs_time": "obs_time",
        "time": "obs_time",
        "timestamp": "obs_time",
        "datetime": "obs_time",
        "utc_time": "obs_time",
        "obstime": "obs_time",
        "时间": "obs_time",
        "观测时间": "obs_time",
        "utc分钟时间": "obs_time",
        "utc时间": "obs_time",
        "rain_mm": "rain_mm",
        "rain": "rain_mm",
        "rainfall": "rain_mm",
        "rainfallmm": "rain_mm",
        "雨量": "rain_mm",
        "雨量mm": "rain_mm",
        "雨量毫米": "rain_mm",
        "雨量毫米值": "rain_mm",
    }.items()
}

STATION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_\-]{0,63}$")
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Accepted UTC timestamp shapes. Everything is interpreted as UTC.
_TS_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S+00:00",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
)


def validate_station_id(station_id: str) -> str:
    if not isinstance(station_id, str) or not STATION_RE.match(station_id):
        raise CsvValidationError(
            f"invalid station_id {station_id!r}: 1-64 chars, letters/digits/_/-"
        )
    return station_id


def validate_day(day: str) -> str:
    if not isinstance(day, str) or not DAY_RE.match(day):
        raise CsvValidationError(f"invalid day {day!r}, expected YYYY-MM-DD")
    try:
        date.fromisoformat(day)
    except ValueError as exc:
        raise CsvValidationError(f"invalid day {day!r}: {exc}") from exc
    return day


def _normalize_header(name: str) -> str:
    return re.sub(r"[\s_\-]+", "", name.strip().lower())


def _parse_timestamp(raw: str, line_no: int) -> datetime:
    value = raw.strip()
    if value.endswith("Z"):
        candidate = value
    else:
        candidate = value
    for fmt in _TS_FORMATS:
        try:
            dt = datetime.strptime(candidate, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Last resort: ISO 8601 parser
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError as exc:
        raise CsvValidationError(
            f"line {line_no}: unparseable UTC timestamp {raw!r}"
        ) from exc


def _parse_rain(raw: str, line_no: int) -> float:
    try:
        value = float(raw.strip())
    except (ValueError, AttributeError) as exc:
        raise CsvValidationError(f"line {line_no}: rain_mm {raw!r} is not numeric") from exc
    if value != value or value in (float("inf"), float("-inf")):
        raise CsvValidationError(f"line {line_no}: rain_mm must be finite")
    if value < 0:
        raise CsvValidationError(f"line {line_no}: rain_mm {value} is negative")
    return value


def parse_csv(
    content: bytes,
    station_id: str,
    day: str,
    batch_id: str,
) -> tuple[pa.Table, str]:
    """Parse and validate an uploaded CSV.

    All rows must belong to ``station_id`` / ``day`` (UTC). Duplicate rows are
    kept exactly as uploaded. Returns ``(table, sha256_of_raw_bytes)``.
    """
    validate_station_id(station_id)
    validate_day(day)
    digest = hashlib.sha256(content).hexdigest()

    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV must be UTF-8 encoded") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvValidationError("empty CSV: header row required") from exc

    normalized = [_normalize_header(h) for h in header]
    aliased = [COLUMN_ALIASES.get(h) for h in normalized]
    if "station_id" not in aliased or "obs_time" not in aliased or "rain_mm" not in aliased:
        raise CsvValidationError(
            "CSV must contain station, timestamp and rainfall columns; "
            f"got header {header!r}"
        )
    idx = {
        col: aliased.index(col) for col in ("station_id", "obs_time", "rain_mm")
    }

    stations: list[str] = []
    times: list[datetime] = []
    rains: list[float] = []
    seen_rows = 0
    for line_no, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue  # tolerate blank lines
        if len(row) < len(header):
            raise CsvValidationError(
                f"line {line_no}: expected {len(header)} columns, got {len(row)}"
            )
        row_station = row[idx["station_id"]].strip()
        if row_station != station_id:
            raise CsvValidationError(
                f"line {line_no}: station_id {row_station!r} does not match "
                f"upload target {station_id!r}"
            )
        dt = _parse_timestamp(row[idx["obs_time"]], line_no)
        if dt.date().isoformat() != day:
            raise CsvValidationError(
                f"line {line_no}: timestamp {dt.isoformat()} is not within UTC day {day}"
            )
        rain = _parse_rain(row[idx["rain_mm"]], line_no)
        stations.append(row_station)
        times.append(dt)
        rains.append(rain)
        seen_rows += 1

    if seen_rows == 0:
        raise CsvValidationError("CSV contains no data rows")

    table = pa.Table.from_pydict(
        {
            "station_id": stations,
            "obs_time": times,
            "rain_mm": rains,
            "source_batch_id": [batch_id] * seen_rows,
        },
        schema=SCHEMA,
    )
    return table, digest


def table_to_records(table: pa.Table) -> list[dict]:
    """Convert a result table into JSON-friendly records."""
    records: list[dict] = []
    cols = {name: table.column(name).to_pylist() for name in table.column_names}
    n = table.num_rows
    for i in range(n):
        dt: datetime = cols["obs_time"][i]
        records.append(
            {
                "station_id": cols["station_id"][i],
                "obs_time": dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                "rain_mm": cols["rain_mm"][i],
                "source_batch_id": cols["source_batch_id"][i],
            }
        )
    return records


def hash_tables(tables: Iterable[pa.Table]) -> str:
    """Deterministic combined digest used to describe a merge input set."""
    h = hashlib.sha256()
    for t in tables:
        for name in t.column_names:
            h.update(name.encode())
            h.update(b"\x00")
            h.update(str(t.column(name).type).encode())
            h.update(b"\x00")
            h.update(b",".join(str(v).encode() for v in t.column(name).to_pylist()))
            h.update(b"\n")
    return h.hexdigest()
