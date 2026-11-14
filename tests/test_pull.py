"""Incremental pull API: ordered pages, cursor resume, merge/reap recovery."""
import threading
from concurrent.futures import ThreadPoolExecutor

from app import replay, services
from app.storage import file_manager
from tests.conftest import make_csv


def _upload(client, station, day, rows):
    r = client.post(
        f"/stations/{station}/days/{day}/batches",
        files={"file": ("x.csv", make_csv(rows, station), "text/csv")},
    )
    assert r.status_code == 201, r.text
    return r.json()


def _pull(client, station, cursor=None, limit=1):
    params = {"limit": limit}
    if cursor is not None:
        params["cursor"] = cursor
    r = client.get(f"/stations/{station}/pull", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _drain_round(client, station, cursor=None, limit=1):
    """Pull until the round completes; return (pages, final_cursor)."""
    pages = []
    while True:
        body = _pull(client, station, cursor=cursor, limit=limit)
        pages.extend(body["batches"])
        cursor = body["next_cursor"]
        if body["round_complete"]:
            return pages, cursor


def test_pull_pages_in_ingestion_order_and_resume(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T00:00Z", 0.0), ("2026-11-12T00:01Z", 0.2)])
    b2 = _upload(client, station, day, [("2026-11-12T08:00Z", 1.1)])
    b3 = _upload(client, station, day, [("2026-11-12T09:00Z", 2.2), ("2026-11-12T09:01Z", 0.0)])

    # Page 1: first batch only, cursor continues the round.
    p1 = _pull(client, station)
    assert p1["round_complete"] is False
    assert p1["batch_count"] == 1
    assert [b["batch_id"] for b in p1["batches"]] == [b1["batch_id"]]
    assert [r["obs_time"] for r in p1["batches"][0]["records"]] == [
        "2026-11-12T00:00:00Z",
        "2026-11-12T00:01:00Z",
    ]
    assert p1["batches"][0]["seq"] > 0

    # Pages 2-3 follow the cursor in ingestion order.
    p2 = _pull(client, station, cursor=p1["next_cursor"])
    assert [b["batch_id"] for b in p2["batches"]] == [b2["batch_id"]]
    p3 = _pull(client, station, cursor=p2["next_cursor"])
    assert [b["batch_id"] for b in p3["batches"]] == [b3["batch_id"]]

    # The bound is reached: round completes and hands over a new-round cursor.
    p4 = _pull(client, station, cursor=p3["next_cursor"])
    assert p4["round_complete"] is True
    assert p4["batches"] == []
    assert p4["round_bound"] == p1["round_bound"]

    # limit>1 packs several whole batches into one page, order preserved.
    pages, _ = _drain_round(client, station, limit=10)
    assert [b["batch_id"] for b in pages] == [b1["batch_id"], b2["batch_id"], b3["batch_id"]]


def test_same_cursor_retry_returns_identical_content(client, station_day):
    station, day = station_day
    _upload(client, station, day, [("2026-11-12T00:00Z", 0.0)])
    _upload(client, station, day, [("2026-11-12T01:00Z", 0.5)])

    first = _pull(client, station)
    again = _pull(client, station)
    assert again == first  # fresh round, same fixed content

    page = _pull(client, station, cursor=first["next_cursor"])
    # Even after more uploads land, the same page cursor replays identically.
    _upload(client, station, day, [("2026-11-12T02:00Z", 0.7)])
    retry = _pull(client, station, cursor=first["next_cursor"])
    assert retry == page
    assert retry["round_bound"] == first["round_bound"]


def test_round_bound_excludes_later_uploads_next_round_includes(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T00:00Z", 0.0)])
    b2 = _upload(client, station, day, [("2026-11-12T01:00Z", 0.5)])

    p1 = _pull(client, station)  # round bound fixed here
    late = _upload(client, station, day, [("2026-11-12T02:00Z", 0.9)])

    # The rest of the round only sees batches up to the fixed bound.
    pages, final_cursor = _drain_round(client, station, cursor=p1["next_cursor"])
    assert [b["batch_id"] for b in pages] == [b2["batch_id"]]

    # A new round from the confirmed position picks up the late upload only.
    pages2, final2 = _drain_round(client, station, cursor=final_cursor)
    assert [b["batch_id"] for b in pages2] == [late["batch_id"]]

    # And a further round is empty: nothing is delivered twice.
    pages3, _ = _drain_round(client, station, cursor=final2)
    assert pages3 == []
    assert b1["batch_id"] not in [b["batch_id"] for b in pages2]


def test_pull_recovers_original_rows_after_merge_and_reap(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T08:01Z", 2.4), ("2026-11-12T08:02Z", 3.7)])
    b2 = _upload(client, station, day, [("2026-11-12T08:01Z", 2.4), ("2026-11-12T08:03Z", 1.9)])

    merged = client.post(f"/stations/{station}/days/{day}/merge").json()
    client.post("/admin/reap")
    # Source Parquet files are really gone; only the merged file remains.
    for bid in (b1["batch_id"], b2["batch_id"]):
        assert not file_manager.target_path(station, day, bid).exists()
    assert file_manager.target_path(station, day, merged["merged_batch_id"]).exists()

    pages, _ = _drain_round(client, station)
    assert [b["batch_id"] for b in pages] == [b1["batch_id"], b2["batch_id"]]
    # Original rows recovered exactly, duplicates and order preserved.
    assert [(r["obs_time"], r["rain_mm"]) for r in pages[0]["records"]] == [
        ("2026-11-12T08:01:00Z", 2.4),
        ("2026-11-12T08:02:00Z", 3.7),
    ]
    assert [(r["obs_time"], r["rain_mm"]) for r in pages[1]["records"]] == [
        ("2026-11-12T08:01:00Z", 2.4),
        ("2026-11-12T08:03:00Z", 1.9),
    ]
    for page in pages:
        assert all(r["source_batch_id"] == page["batch_id"] for r in page["records"])


def test_merge_is_not_a_new_upload_in_pull_stream(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T00:00Z", 0.1)])
    b2 = _upload(client, station, day, [("2026-11-12T01:00Z", 0.2)])
    merged = client.post(f"/stations/{station}/days/{day}/merge").json()

    pages, _ = _drain_round(client, station)
    ids = [b["batch_id"] for b in pages]
    assert ids == [b1["batch_id"], b2["batch_id"]]
    assert merged["merged_batch_id"] not in ids


def test_pull_recovers_through_second_generation_merge(client, station_day):
    station, day = station_day
    b1 = _upload(client, station, day, [("2026-11-12T00:00Z", 0.1)])
    b2 = _upload(client, station, day, [("2026-11-12T01:00Z", 0.2)])
    m1 = client.post(f"/stations/{station}/days/{day}/merge").json()
    b3 = _upload(client, station, day, [("2026-11-12T02:00Z", 0.3)])
    m2 = client.post(f"/stations/{station}/days/{day}/merge").json()
    assert set(m2["source_batch_ids"]) == {m1["merged_batch_id"], b3["batch_id"]}
    client.post("/admin/reap")

    # Only the second-generation merged file survives.
    remaining = list(file_manager.station_day_dir(station, day).glob("*.parquet"))
    assert [p.name for p in remaining] == [f"{m2['merged_batch_id']}.parquet"]

    pages, _ = _drain_round(client, station)
    assert [b["batch_id"] for b in pages] == [b1["batch_id"], b2["batch_id"], b3["batch_id"]]
    assert [b["row_count"] for b in pages] == [1, 1, 1]
    assert pages[2]["records"][0]["obs_time"] == "2026-11-12T02:00:00Z"


def test_pull_unknown_station_is_an_empty_round(client):
    body = _pull(client, "NOSTATION")
    assert body["round_complete"] is True
    assert body["batches"] == []
    # The returned cursor is usable and stays empty.
    again = _pull(client, "NOSTATION", cursor=body["next_cursor"])
    assert again["batches"] == [] and again["round_complete"] is True


def test_malformed_and_foreign_cursors_are_rejected(client, station_day):
    station, day = station_day
    _upload(client, station, day, [("2026-11-12T00:00Z", 0.1)])

    assert client.get(f"/stations/{station}/pull", params={"cursor": "!!!"}).status_code == 400
    assert (
        client.get(f"/stations/{station}/pull", params={"cursor": "aGVsbG8"}).status_code == 400
    )
    foreign = replay.encode_cursor("OTHERSTATION", 0, None)
    r = client.get(f"/stations/{station}/pull", params={"cursor": foreign})
    assert r.status_code == 400 and "CursorError" in r.text
    backwards = replay.encode_cursor(station, 5, 2)  # bound before after
    assert client.get(f"/stations/{station}/pull", params={"cursor": backwards}).status_code == 400


def test_read_failure_returns_no_partial_page_and_cursor_still_works(client, station_day, monkeypatch):
    station, day = station_day
    _upload(client, station, day, [("2026-11-12T00:00Z", 0.0), ("2026-11-12T00:01Z", 0.2)])
    _upload(client, station, day, [("2026-11-12T01:00Z", 0.5)])

    clean_first = _pull(client, station)

    def boom(path):
        raise RuntimeError("injected read failure")

    monkeypatch.setattr(replay.pq, "read_table", boom)
    r = client.get(f"/stations/{station}/pull")
    assert r.status_code == 503
    body = r.json()
    assert body["error"] == "PullReadError"
    assert "next_cursor" not in body and "batches" not in body  # no partial page
    monkeypatch.undo()

    # Retrying the same request/cursor after the failure returns full content.
    retried = _pull(client, station)
    assert retried == clean_first
    second = _pull(client, station, cursor=retried["next_cursor"])
    assert second["batch_count"] == 1
    assert second["batches"][0]["row_count"] == 1


def test_concurrent_pull_upload_merge_reap_no_loss_no_duplication(client):
    station, day = "PULLCONC", "2026-11-12"
    seeded = [
        services.upload_csv(
            station, day, make_csv([(f"2026-11-12T0{i}:00Z", float(i))], station)
        )
        for i in range(4)
    ]
    first = replay.pull_batches(station, None, 1)
    bound = first["round_bound"]

    late_ids, pull_errors = [], []

    def worker():
        try:
            for i in range(4, 7):
                late = services.upload_csv(
                    station, day, make_csv([(f"2026-11-12T1{i}:00Z", float(i))], station)
                )
                late_ids.append(late["batch_id"])
                services.merge_station_day(station, day)
                file_manager.reap_once()
        except Exception as exc:  # noqa: BLE001
            pull_errors.append(exc)

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(worker)
        # Drain round 1 while uploads/merges/reaps race against the pulls.
        round1 = []
        cursor = None
        while True:
            body = replay.pull_batches(station, cursor, 1)
            round1.extend(body["batches"])
            cursor = body["next_cursor"]
            if body["round_complete"]:
                break
        round1_cursor = cursor
        fut.result(timeout=60)

    assert not pull_errors
    # Round 1 delivered exactly the batches inside its bound, in order, once.
    assert [b["batch_id"] for b in round1] == [b["batch_id"] for b in seeded]
    assert all(b["seq"] <= bound for b in round1)

    # Round 2 delivers exactly the late uploads, whatever merge generation
    # their rows currently live in.
    round2 = []
    cursor = round1_cursor
    while True:
        body = replay.pull_batches(station, cursor, 2)
        round2.extend(body["batches"])
        cursor = body["next_cursor"]
        if body["round_complete"]:
            break
    assert [b["batch_id"] for b in round2] == late_ids
    assert [r["obs_time"] for b in round2 for r in b["records"]] == [
        f"2026-11-12T1{i}:00:00Z" for i in range(4, 7)
    ]


def test_pull_pins_file_against_reaper_while_reading(client, station_day):
    """A pull in flight pins the file it reads, even once a newer merge
    supersedes that file mid-read; the reaper must wait for the pull."""
    station, day = station_day
    _upload(client, station, day, [("2026-11-12T00:00Z", 0.1)])
    _upload(client, station, day, [("2026-11-12T01:00Z", 0.2)])
    m1 = client.post(f"/stations/{station}/days/{day}/merge").json()
    client.post("/admin/reap")  # originals gone; only m1's file remains

    original_read = replay.pq.read_table
    inside_read = threading.Event()
    release_read = threading.Event()
    state = {"blocked": False}

    def slow_read(path):
        # Only the pull's read blocks; the second merge's source read passes.
        if not state["blocked"]:
            state["blocked"] = True
            inside_read.set()
            release_read.wait(timeout=10)
        return original_read(path)

    replay.pq.read_table = slow_read
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(replay.pull_batches, station, None, 1)
            assert inside_read.wait(timeout=10)  # m1's file is pinned now

            # A second merge supersedes the very file the pull is reading.
            m2 = client.post(f"/stations/{station}/days/{day}/merge").json()
            assert m2["source_batch_ids"] == [m1["merged_batch_id"]]
            reaped = file_manager.reap_once()
            assert m1["merged_batch_id"] not in reaped  # pinned by the pull
            assert file_manager.target_path(station, day, m1["merged_batch_id"]).exists()

            release_read.set()
            body = future.result(timeout=10)
    finally:
        replay.pq.read_table = original_read

    # The pull completed from the pinned superseded file, content intact.
    assert body["batch_count"] == 1
    assert body["batches"][0]["records"][0]["obs_time"] == "2026-11-12T00:00:00Z"

    # Once the pull finished, the reaper can collect the old merged file.
    assert m1["merged_batch_id"] in file_manager.reap_once()
    assert not file_manager.target_path(station, day, m1["merged_batch_id"]).exists()
