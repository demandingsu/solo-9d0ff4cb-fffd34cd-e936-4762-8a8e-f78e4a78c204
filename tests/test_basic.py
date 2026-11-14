"""End-to-end behavior: upload, query, merge, duplicate retention, reaping."""
from pathlib import Path

import pyarrow.parquet as pq

from app.storage import file_manager
from tests.conftest import make_csv


def _upload(client, station, day, rows):
    r = client.post(
        f"/stations/{station}/days/{day}/batches",
        files={"file": ("x.csv", make_csv(rows, station), "text/csv")},
    )
    assert r.status_code == 201, r.text
    return r.json()


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_upload_independent_batches_and_query_reads_parquet(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T00:00Z", 0.0), ("2026-11-12T00:01Z", 0.2)])
    b2 = _upload(client, station, day, [("2026-11-12T08:00Z", 1.1), ("2026-11-12T08:01Z", 2.4)])
    assert b1["batch_id"] != b2["batch_id"]
    assert b1["row_count"] == 2 and b2["row_count"] == 2

    r = client.get(f"/stations/{station}/days/{day}/records")
    assert r.status_code == 200
    body = r.json()
    assert body["file_count"] == 2
    assert set(body["batch_ids"]) == {b1["batch_id"], b2["batch_id"]}
    assert body["row_count"] == 4

    # Verify the query actually reads the published Parquet files on disk.
    for batch_id in body["batch_ids"]:
        path = file_manager.target_path(station, day, batch_id)
        assert path.exists()
        t = pq.read_table(path)
        assert str(t.schema.field("obs_time").type) == "timestamp[ms, tz=UTC]"
        assert t.column_names == ["station_id", "obs_time", "rain_mm", "source_batch_id"]
    assert body["records"][0]["source_batch_id"] == b1["batch_id"]
    assert body["records"][-1]["source_batch_id"] == b2["batch_id"]
    assert body["records"][0]["obs_time"].endswith("Z")


def test_duplicate_rows_are_preserved_through_merge(client, station_day):
    station, day = station_day
    _upload(client, station, day, [("2026-11-12T08:01Z", 2.4), ("2026-11-12T08:02Z", 3.7)])
    _upload(client, station, day, [("2026-11-12T08:01Z", 2.4), ("2026-11-12T08:03Z", 1.9)])

    before = client.get(f"/stations/{station}/days/{day}/records").json()
    assert before["row_count"] == 4
    dupes = [r for r in before["records"] if r["obs_time"] == "2026-11-12T08:01:00Z"]
    assert len(dupes) == 2

    r = client.post(f"/stations/{station}/days/{day}/merge")
    assert r.status_code == 200, r.text
    merged = r.json()
    assert merged["source_count"] == 2
    assert merged["row_count"] == 4

    after = client.get(f"/stations/{station}/days/{day}/records").json()
    assert after["file_count"] == 1
    assert after["batch_ids"] == [merged["merged_batch_id"]]
    assert after["row_count"] == 4
    dupes = [r for r in after["records"] if r["obs_time"] == "2026-11-12T08:01:00Z"]
    assert len(dupes) == 2  # identical rows survive the merge
    assert {r["source_batch_id"] for r in after["records"]} == set(
        merged["source_batch_ids"]
    )

    listing = client.get(f"/stations/{station}/days/{day}/batches").json()
    statuses = {b["batch_id"]: b["status"] for b in listing["batches"]}
    assert statuses[merged["merged_batch_id"]] == "published"
    for src in merged["source_batch_ids"]:
        assert statuses[src] == "superseded"
        assert all(p.exists() for p in [file_manager.target_path(station, day, src)])


def test_reap_deletes_only_unreferenced_superseded_files(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T00:00Z", 0.0)])
    b2 = _upload(client, station, day, [("2026-11-12T01:00Z", 0.5)])
    merged = client.post(f"/stations/{station}/days/{day}/merge").json()

    old_paths = [file_manager.target_path(station, day, b) for b in (b1["batch_id"], b2["batch_id"])]
    assert all(p.exists() for p in old_paths)

    reap = client.post("/admin/reap").json()
    reaped = set(reap["reaped_batch_ids"])
    assert {b1["batch_id"], b2["batch_id"]}.issubset(reaped)
    assert merged["merged_batch_id"] not in reaped
    assert all(not p.exists() for p in old_paths)
    assert file_manager.target_path(station, day, merged["merged_batch_id"]).exists()

    # Reaping again cannot touch this station/day; queries still work.
    second_reap = set(client.post("/admin/reap").json()["reaped_batch_ids"])
    assert second_reap.isdisjoint({b1["batch_id"], b2["batch_id"], merged["merged_batch_id"]})
    body = client.get(f"/stations/{station}/days/{day}/records").json()
    assert body["row_count"] == 2


def test_second_merge_compacts_without_losing_data(client, station_day):
    station, day = station_day
    _upload(client, station, day, [("2026-11-12T00:00Z", 0.1)])
    m1 = client.post(f"/stations/{station}/days/{day}/merge").json()
    late = _upload(client, station, day, [("2026-11-12T02:00Z", 0.9), ("2026-11-12T02:01Z", 0.7)])

    m2 = client.post(f"/stations/{station}/days/{day}/merge").json()
    assert set(m2["source_batch_ids"]) == {m1["merged_batch_id"], late["batch_id"]}

    body = client.get(f"/stations/{station}/days/{day}/records").json()
    assert body["file_count"] == 1
    assert body["row_count"] == 3
    assert body["batch_ids"] == [m2["merged_batch_id"]]


def test_chinese_header_sample_works(client):
    sample = Path(__file__).resolve().parents[1] / "samples" / "st001_2026-11-12_batch3_zh.csv"
    r = client.post(
        "/stations/ST001/days/2026-11-12/batches",
        files={"file": ("zh.csv", sample.read_bytes(), "text/csv")},
    )
    assert r.status_code == 201, r.text
    assert r.json()["row_count"] == 4


def test_query_unknown_station_day_is_empty(client):
    r = client.get("/stations/NOPE/days/2026-11-12/records")
    assert r.status_code == 200
    body = r.json()
    assert body["file_count"] == 0 and body["records"] == []


def test_invalid_csv_rejected_without_state(client, station_day):
    station, day = station_day
    bad = b"station_id,obs_time,rain_mm\nST001,not-a-time,1.0\n"
    r = client.post(
        f"/stations/{station}/days/{day}/batches",
        files={"file": ("bad.csv", bad, "text/csv")},
    )
    assert r.status_code == 400
    assert "CsvValidationError" in r.text

    wrong_day = make_csv([("2026-11-11T23:59Z", 1.0)])
    r = client.post(
        f"/stations/{station}/days/{day}/batches",
        files={"file": ("wd.csv", wrong_day, "text/csv")},
    )
    assert r.status_code == 400

    listing = client.get(f"/stations/{station}/days/{day}/batches").json()
    assert listing["batches"] == []
    # No temp leftovers
    assert list(file_manager.tmp_dir.glob("*.tmp-*")) == []


def test_bad_path_parameters_are_422(client):
    r = client.get("/stations/bad%20id/days/2026-11-12/records")
    assert r.status_code == 422
    r = client.get("/stations/ST001/days/20261112/records")
    assert r.status_code == 422
