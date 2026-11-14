"""Pytest configuration: isolated data directory per test session."""
import os
import re
import sys
import tempfile
from pathlib import Path

import pytest

# Point every path at a throwaway directory before importing application code.
_TMP = Path(tempfile.mkdtemp(prefix="rain-test-"))
os.environ["RAIN_DATA_DIR"] = str(_TMP)
os.environ["RAIN_REAPER_INTERVAL"] = "3600"  # disable background reaping in tests

# Local dependency folder used when no system pip/venv is available.
_DEPS = Path(__file__).resolve().parents[1] / ".pydeps"
if _DEPS.exists() and str(_DEPS) not in sys.path:
    sys.path.insert(0, str(_DEPS))

from fastapi.testclient import TestClient  # noqa: E402

from app import db as db_module  # noqa: E402
from app.main import app  # noqa: E402
from app.storage import file_manager  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _initialize_storage():
    # Works even for tests that call the service layer directly without the
    # HTTP client (whose lifespan would otherwise perform this startup).
    _TMP.mkdir(parents=True, exist_ok=True)
    db_module.init_db()
    file_manager.startup_sweep()
    file_manager.start_reaper()
    yield
    file_manager.stop_reaper()


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def station_day(request):
    # Unique station per test so station/day state never leaks between tests.
    station = "TS" + re.sub(r"[^A-Za-z0-9]", "", request.node.name).upper()[:48]
    return station, "2026-11-12"


def make_csv(rows, station="ST001"):
    lines = ["station_id,obs_time,rain_mm"]
    for ts, rain in rows:
        lines.append(f"{station},{ts},{rain}")
    return ("\n".join(lines) + "\n").encode("utf-8")
