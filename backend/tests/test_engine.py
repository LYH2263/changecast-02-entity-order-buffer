"""Pure engine tests: per-entity ordering, buffering, overflow-block, flush."""

from datetime import datetime, timedelta, timezone

from app.engines.order_buffer import EntityOrderEngine

NOW = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)
CONFIG = {"buffer_max_events": 2, "buffer_max_wait_seconds": 600, "start_seq": 1}


def make_engine(states=None, buffer=None, known=None, config=None, now=NOW):
    return EntityOrderEngine(
        states=states if states is not None else {},
        buffer_entries=buffer if buffer is not None else [],
        known_event_ids=known if known is not None else set(),
        config=config or dict(CONFIG),
        now=now,
    )


def ev(eid, key, seq, op="upsert", payload=None):
    return {
        "event_id": eid,
        "entity_key": key,
        "op": op,
        "seq": seq,
        "event_time": "2026-04-01T00:00:00Z",
        "payload": payload if payload is not None else {"v": seq},
    }


def results(engine):
    return [d["result"] for d in engine.decisions]


def test_in_order_events_apply_immediately():
    eng = make_engine()
    out = eng.process([ev("e1", "A", 1), ev("e2", "B", 1), ev("e3", "A", 2)])
    assert results(eng) == ["applied_upsert", "applied_upsert", "applied_upsert"]
    assert out["counts"]["applied_upsert"] == 3
    assert eng.states["A"]["expected_seq"] == 3
    assert eng.states["B"]["expected_seq"] == 2
    assert not eng.buffer


def test_ahead_event_buffers_and_releases_on_gap_fill():
    eng = make_engine()
    out = eng.process([ev("e2", "A", 2), ev("e1", "A", 1)])
    assert results(eng) == ["buffered", "applied_upsert", "released_upsert"]
    c = out["counts"]
    assert c["buffered"] == 1 and c["released_upsert"] == 1
    # projection saw seq1 then seq2, in seq order despite arrival order
    assert [u["applied_seq"] for u in out["projection_updates"]] == [1, 2]
    assert eng.states["A"]["expected_seq"] == 3
    assert not eng.buffer


def test_entities_are_independent():
    eng = make_engine()
    eng.process([ev("a2", "A", 2), ev("b1", "B", 1), ev("b2", "B", 2)])
    # A#2 waits in buffer; B applies freely
    assert results(eng) == ["buffered", "applied_upsert", "applied_upsert"]
    assert eng.states["B"]["expected_seq"] == 3
    assert eng.states["A"]["expected_seq"] == 1


def test_stale_late_event_never_overwrites():
    eng = make_engine()
    out = eng.process([ev("e1", "A", 1, payload={"v": "old"}), ev("e2", "A", 2, payload={"v": "new"}),
                       ev("e3", "A", 1, payload={"v": "stale"})])
    assert results(eng)[-1] == "skipped_stale"
    assert out["counts"]["skipped_stale"] == 1
    last = out["projection_updates"][-1]
    assert last["applied_seq"] == 2 and last["payload"] == {"v": "new"}


def test_duplicate_event_id_skipped():
    eng = make_engine()
    out = eng.process([ev("e1", "A", 1), ev("e1", "A", 1)])
    assert results(eng) == ["applied_upsert", "skipped_duplicate"]
    assert out["counts"]["skipped_duplicate"] == 1


def test_redundant_seq_in_buffer_skipped():
    eng = make_engine()
    out = eng.process([ev("e2", "A", 2), ev("e2b", "A", 2)])
    assert results(eng) == ["buffered", "skipped_redundant"]
    assert out["counts"]["skipped_redundant"] == 1
    assert len(eng.buffer) == 1


def test_count_overflow_blocks_only_that_entity():
    eng = make_engine()
    out = eng.process([
        ev("a1", "A", 1),
        ev("a3", "A", 3),          # buffer 1
        ev("a4", "A", 4),          # buffer 2
        ev("a5", "A", 5),          # buffer 3 > max 2 -> block A
        ev("a6", "A", 6),          # held while blocked
        ev("b1", "B", 1),          # other entity unaffected
    ])
    assert results(eng) == [
        "applied_upsert", "buffered", "buffered", "buffered", "held_blocked", "applied_upsert",
    ]
    c = out["counts"]
    assert c["overflow_blocks"] == 1
    assert c["held_blocked"] == 1
    assert c["dropped_overflow"] == 0  # block policy never drops
    assert eng.states["A"]["blocked"] is True
    assert eng.states["B"]["blocked"] is False
    assert all(e["status"] == "held" for e in eng.buffer if e["entity_key"] == "A")


def test_blocked_entity_releases_continuously_after_gap_fill():
    eng = make_engine()
    eng.process([ev("a1", "A", 1), ev("a3", "A", 3), ev("a4", "A", 4), ev("a5", "A", 5)])
    assert eng.states["A"]["blocked"] is True
    out = eng.process([ev("a2", "A", 2)])  # the gap-filler
    assert results(eng)[-4:] == ["applied_upsert", "released_upsert", "released_upsert", "released_upsert"]
    assert [u["applied_seq"] for u in out["projection_updates"][-4:]] == [2, 3, 4, 5]
    assert eng.states["A"]["blocked"] is False
    assert eng.states["A"]["expected_seq"] == 6
    assert not eng.buffer


def test_wait_overflow_blocks_on_age():
    old = (NOW - timedelta(seconds=700)).isoformat().replace("+00:00", "Z")
    buffer = [dict(ev("a2", "A", 2), status="open", buffered_at=old)]
    eng = make_engine(buffer=buffer)
    eng.process([])  # no new events; sweep alone must block
    assert eng.states["A"]["blocked"] is True
    assert "wait_overflow" in (eng.states["A"]["blocked_reason"] or "")


def test_flush_allows_new_start_seq_and_counts_gap():
    buffer = [
        dict(ev("a3", "A", 3), status="open", buffered_at="2026-04-01T00:00:00Z"),
        dict(ev("a4", "A", 4), status="open", buffered_at="2026-04-01T00:00:01Z"),
    ]
    eng = make_engine(states={"A": {"expected_seq": 2, "blocked": False, "blocked_reason": None}},
                      buffer=buffer)
    out = eng.flush("A")
    c = out["counts"]
    assert c["gap_skipped"] == 1
    assert c["flushed_upsert"] == 2
    assert eng.states["A"]["expected_seq"] == 5
    assert not eng.buffer


def test_flush_unblocks_and_keeps_internal_gap_held():
    buffer = [
        dict(ev("a3", "A", 3), status="held", buffered_at="2026-04-01T00:00:00Z"),
        dict(ev("a5", "A", 5), status="held", buffered_at="2026-04-01T00:00:01Z"),
    ]
    eng = make_engine(
        states={"A": {"expected_seq": 2, "blocked": True, "blocked_reason": "count_overflow:2>1"}},
        buffer=buffer,
    )
    eng.flush("A")
    # 3 released; 5 still waits for 4 -> entity stays blocked
    assert eng.states["A"]["expected_seq"] == 4
    assert eng.states["A"]["blocked"] is True
    assert [e["seq"] for e in eng.buffer if e["status"] == "held"] == [5]


def test_flush_empty_buffer_raises():
    eng = make_engine()
    try:
        eng.flush("NOPE")
    except ValueError as e:
        assert "NOPE" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_delete_op_applies_and_releases_as_delete():
    eng = make_engine()
    out = eng.process([ev("e2", "A", 2, op="delete"), ev("e1", "A", 1)])
    assert results(eng) == ["buffered", "applied_upsert", "released_delete"]
    last = out["projection_updates"][-1]
    assert last["deleted"] is True and last["payload"] is None
