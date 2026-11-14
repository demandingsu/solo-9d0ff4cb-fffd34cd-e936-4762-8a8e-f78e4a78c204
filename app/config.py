"""Runtime configuration.

All paths derive from ``RAIN_DATA_DIR`` so the whole service can be moved
between a local folder, a Docker volume and the test sandbox by changing one
environment variable.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    db_path: Path
    parquet_root: Path
    tmp_dir: Path
    reaper_interval: float
    max_upload_bytes: int


def load_settings() -> Settings:
    data_dir = Path(os.environ.get("RAIN_DATA_DIR", "./data")).resolve()
    db_path = Path(
        os.environ.get("RAIN_DB_PATH", str(data_dir / "metadata.sqlite3"))
    ).resolve()
    return Settings(
        data_dir=data_dir,
        db_path=db_path,
        parquet_root=data_dir / "parquet",
        tmp_dir=data_dir / "tmp",
        reaper_interval=float(os.environ.get("RAIN_REAPER_INTERVAL", "5")),
        max_upload_bytes=int(os.environ.get("RAIN_MAX_UPLOAD_BYTES", str(50 * 1024 * 1024))),
    )


settings = load_settings()
