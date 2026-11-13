"""Service-level tests over the seeded streams and fixtures (temp SQLite DB)."""

import pytest

from app.db import connect
from app.repositories import projections as proj_repo
from app.services import project_service as svc


def stream_id(code):
    return next(s["id"] for s in svc.list_streams() if s["code"] == code)


def proj(code, entity):
    rows = svc.list_projections(stream_id=stream_id(code))
    return next((r for r in rows if r["entity_key"] == entity), None)


def test_member_profile_scrambled_arrival_orders_per_entity():
    b = svc.run_project(stream_id("member_profile"))
    assert b["source_events"] == 6
    assert b["applied_upsert"] == 4
    assert b["released_upsert"] == 1 and b["released_delete"] == 1
    assert b["buffered"] == 2
    assert b["buffer_open_after"] == 0
    assert b["reconciled"] is True

    m1 = proj("member_profile", "M1001")
    assert m1["applied_seq"] == 2 and m1["payload"]["tier"] == "gold"
    m2 = proj("member_profile", "M1002")
    assert m2["deleted"] is True and m2["applied_seq"] == 2
    m3 = proj("member_profile", "M1003")
    assert m3["applied_seq"] == 2 and m3["payload"]["tier"] == "gold"


def test_rerun_is_idempotent():
    svc.run_project(stream_id("store_archive"))
    b2 = svc.run_project(stream_id("store_archive"))
    assert b2["skipped_duplicate"] == 5
    assert b2["applied_upsert"] == 0 and b2["buffered"] == 0
    assert b2["projected_alive"] == 3


def test_lab_run_overflow_blocks_single_entity():
    b = svc.run_project(stream_id("reorder_lab"))
    assert b["source_events"] == 16
    # L-A: applied 1, buffered 1, released 1 (gap filled within the same run)
    # L-B: applied 1, buffered 3 -> overflow block, then held 1
    # L-C: applied 2 (unaffected by L-B's block)
    # L-D: applied 1, buffered 2 (gap, not overflow)
    # L-E: applied 2, stale 1
    assert b["applied_upsert"] == 8
    assert b["buffered"] == 6
    assert b["held_blocked"] == 1
    assert b["released_upsert"] == 1
    assert b["skipped_stale"] == 1
    assert b["overflow_blocks"] == 1
    assert b["dropped_overflow"] == 0
    assert b["buffer_open_after"] == 6
    assert b["blocked_entities_after"] == 1
    assert b["reconciled"] is True

    # blocked entity kept its last in-order value; others progressed
    assert proj("reorder_lab", "L-B")["applied_seq"] == 1
    assert proj("reorder_lab", "L-C")["applied_seq"] == 2
    # stale low seq must not overwrite the higher applied seq
    le = proj("reorder_lab", "L-E")
    assert le["applied_seq"] == 2 and le["payload"]["qty"] == 60

    summary = {s["code"]: s for s in svc.buffer_summary()}
    lab = {e["entity_key"]: e for e in summary["reorder_lab"]["entities"]}
    assert lab["L-B"]["blocked"] is True and lab["L-B"]["gap"] == [2]
    assert len(lab["L-B"]["entries"]) == 4
    assert lab["L-D"]["blocked"] is False and lab["L-D"]["gap"] == [2]
    assert "L-C" not in lab  # healthy entities are not on the buffer board


def test_gap_fill_run_releases_blocked_entity():
    svc.run_project(stream_id("reorder_lab"))
    b = svc.run_project(stream_id("reorder_lab"), fixture="reorder_lab_fill.json")
    assert b["applied_upsert"] == 2          # lab-f1 (gap filler), lab-f2
    assert b["released_upsert"] == 4         # L-B seq 3,4,5,6 drain continuously
    assert b["blocked_entities_after"] == 0
    assert b["buffer_open_after"] == 2       # only L-D's gap remains
    assert b["reconciled"] is True
    lb = proj("reorder_lab", "L-B")
    assert lb["applied_seq"] == 6 and lb["payload"]["qty"] == 106
    assert proj("reorder_lab", "L-C")["applied_seq"] == 3


def test_manual_flush_releases_with_gap_counted():
    svc.run_project(stream_id("reorder_lab"))
    b = svc.flush_entity_buffer(stream_id("reorder_lab"), "L-D")
    assert b["kind"] == "flush"
    assert b["gap_skipped"] == 1             # seq 2 explicitly skipped
    assert b["flushed_upsert"] == 2          # seq 3, 4 released in order
    assert b["buffer_open_after"] == 4       # L-B's held entries untouched
    assert b["reconciled"] is True
    ld = proj("reorder_lab", "L-D")
    assert ld["applied_seq"] == 4 and ld["payload"]["qty"] == 10


def test_flush_without_buffer_rejected():
    svc.run_project(stream_id("reorder_lab"))
    with pytest.raises(ValueError):
        svc.flush_entity_buffer(stream_id("reorder_lab"), "L-C")


def test_full_flow_all_batches_reconcile():
    svc.run_project(stream_id("member_profile"))
    svc.run_project(stream_id("reorder_lab"))
    svc.run_project(stream_id("reorder_lab"), fixture="reorder_lab_fill.json")
    svc.flush_entity_buffer(stream_id("reorder_lab"), "L-D")
    batches = svc.list_batches(limit=50)
    assert len(batches) == 4
    assert all(b["reconciled"] for b in batches)
    # after everything, lab buffer is empty and nothing is blocked
    stats = svc.dashboard_stats()
    lab_open = [e for s in svc.buffer_summary() if s["code"] == "reorder_lab" for e in s["entities"]]
    assert lab_open == []
    assert stats["blocked_entities"] == 0


def test_projection_storage_layer_rejects_lower_seq():
    conn = connect()
    try:
        proj_repo.upsert_projection_row(
            conn,
            stream_id=1,
            row={"entity_key": "X", "payload": {"v": "high"}, "applied_seq": 5,
                 "applied_event_id": "e5", "deleted": False},
            updated_at="2026-01-01T00:00:00Z",
        )
        proj_repo.upsert_projection_row(
            conn,
            stream_id=1,
            row={"entity_key": "X", "payload": {"v": "low"}, "applied_seq": 3,
                 "applied_event_id": "e3", "deleted": False},
            updated_at="2026-01-01T00:00:01Z",
        )
        conn.commit()
        rows = proj_repo.list_projections(conn, stream_id=1)
        assert rows[0]["applied_seq"] == 5 and rows[0]["payload"] == {"v": "high"}
    finally:
        conn.close()


def test_stream_config_update_and_validation():
    sid = stream_id("reorder_lab")
    updated = svc.update_stream_config(
        sid, {"buffer_max_events": 4, "buffer_max_wait_seconds": 120, "start_seq": 1}
    )
    assert updated["buffer_max_events"] == 4
    with pytest.raises(ValueError):
        svc.update_stream_config(
            sid, {"buffer_max_events": -1, "buffer_max_wait_seconds": 0, "start_seq": 1}
        )


def test_bad_fixture_name_rejected():
    with pytest.raises(ValueError):
        svc.run_project(stream_id("reorder_lab"), fixture="../seed.py")
    with pytest.raises(ValueError):
        svc.run_project(stream_id("reorder_lab"), fixture="nope.json")
