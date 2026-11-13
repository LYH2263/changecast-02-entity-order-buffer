from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.order_buffer import EntityOrderEngine
from app.repositories import batches as batch_repo
from app.repositories import buffer as buffer_repo
from app.repositories import entity_states as state_repo
from app.repositories import projections as proj_repo
from app.repositories import streams as stream_repo


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_fixture(name: str) -> list[dict[str, Any]]:
    # fixture must be a plain file name inside the fixtures dir
    if Path(name).name != name:
        raise ValueError(f"非法夹具名: {name}")
    path = FIXTURES_DIR / name
    if not path.is_file():
        raise ValueError(f"夹具不存在: {name}")
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"fixture {name} must be a JSON array")
    return data


def _stream_config(stream: dict[str, Any]) -> dict[str, int]:
    return {
        "buffer_max_events": int(stream["buffer_max_events"]),
        "buffer_max_wait_seconds": int(stream["buffer_max_wait_seconds"]),
        "start_seq": int(stream["start_seq"]),
    }


def _reconciled(batch: dict[str, Any]) -> bool:
    """Every source event of a batch must be accounted for by its decisions."""
    if batch.get("kind", "run") == "flush":
        decided = (
            batch["flushed_upsert"] + batch["flushed_delete"] + batch["skipped_stale"]
        )
        return batch["source_events"] == decided
    decided = (
        batch["applied_upsert"]
        + batch["applied_delete"]
        + batch["buffered"]
        + batch["held_blocked"]
        + batch["skipped_duplicate"]
        + batch["skipped_stale"]
        + batch["skipped_redundant"]
    )
    return batch["source_events"] == decided


def _persist_result(
    conn,
    *,
    stream: dict[str, Any],
    result: dict[str, Any],
    kind: str,
    note: str | None,
    created_at: str,
) -> tuple[int, dict[str, int], list[str]]:
    """Persist one engine result (run or flush) atomically; return (batch_id, counts, reasons)."""
    stream_id = stream["id"]
    active_before = buffer_repo.count_active(conn, stream_id)
    for row in result["projection_updates"]:
        proj_repo.upsert_projection_row(conn, stream_id=stream_id, row=row, updated_at=created_at)
    for entry in result["buffer_inserts"]:
        buffer_repo.insert_entry(conn, stream_id=stream_id, entry=entry)

    counts = dict(result["counts"])
    counts["projected_alive"] = proj_repo.count_projections(conn, stream_id, deleted=False)
    counts["projected_deleted"] = proj_repo.count_projections(conn, stream_id, deleted=True)
    deactivated = len(
        [u for u in result["buffer_status_updates"] if u["status"] in ("released", "flushed", "stale")]
    )
    counts["buffer_open_after"] = active_before + len(result["buffer_inserts"]) - deactivated
    counts["blocked_entities_after"] = sum(
        1 for st in result["states"].values() if st["blocked"]
    )

    reasons = list(result["reasons"])
    full_note = "；".join([p for p in [note, *reasons] if p]) or None
    seq = conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"] + 1
    batch_no = f"PB-{created_at[:10].replace('-', '')}-{seq:04d}"
    batch_id = batch_repo.insert_batch(
        conn,
        batch_no=batch_no,
        stream_id=stream_id,
        kind=kind,
        status="completed",
        counts=counts,
        note=full_note,
        created_at=created_at,
    )

    for upd in result["buffer_status_updates"]:
        buffer_repo.set_status(
            conn,
            stream_id=stream_id,
            event_id=upd["event_id"],
            status=upd["status"],
            released_batch_id=batch_id if upd["status"] in ("released", "flushed", "stale") else None,
            released_at=created_at if upd["status"] in ("released", "flushed", "stale") else None,
        )
    for entity_key, st in result["states"].items():
        state_repo.upsert_state(
            conn,
            stream_id=stream_id,
            entity_key=entity_key,
            expected_seq=int(st["expected_seq"]),
            blocked=bool(st["blocked"]),
            blocked_reason=st["blocked_reason"],
            updated_at=created_at,
        )
    for ev in result["decisions"]:
        batch_repo.insert_applied_event(
            conn, batch_id=batch_id, stream_id=stream_id, event=ev, created_at=created_at
        )
    if counts["max_seq"] > int(stream["watermark_seq"]):
        stream_repo.set_watermark(conn, stream_id, counts["max_seq"])
    return batch_id, counts, reasons


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        return {
            "stream_count": conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"],
            "enabled_streams": conn.execute(
                "SELECT COUNT(*) c FROM project_streams WHERE enabled=1"
            ).fetchone()["c"],
            "batch_count": conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"],
            "projection_alive": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=0"
            ).fetchone()["c"],
            "projection_deleted": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=1"
            ).fetchone()["c"],
            "buffer_open": buffer_repo.count_by_status(conn, "open"),
            "buffer_held": buffer_repo.count_by_status(conn, "held"),
            "blocked_entities": state_repo.count_blocked(conn),
            "stale_skipped": batch_repo.count_events_by_result(conn, "skipped_stale"),
            "overflow_blocks_total": conn.execute(
                "SELECT COALESCE(SUM(overflow_blocks),0) c FROM project_batches"
            ).fetchone()["c"],
            "apply_order": conn.execute(
                "SELECT value FROM settings WHERE key='apply_order'"
            ).fetchone()["value"],
            "overflow_policy": conn.execute(
                "SELECT value FROM settings WHERE key='overflow_policy'"
            ).fetchone()["value"],
        }
    finally:
        conn.close()


def list_streams() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return stream_repo.list_streams(conn)
    finally:
        conn.close()


def update_stream_config(stream_id: int, config: dict[str, Any]) -> dict[str, Any]:
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        max_events = int(config["buffer_max_events"])
        max_wait = int(config["buffer_max_wait_seconds"])
        start_seq = int(config["start_seq"])
        if max_events < 0 or max_wait < 0 or start_seq < 0:
            raise ValueError("缓冲上限、等待上限、起始 seq 均须为非负整数")
        stream_repo.update_config(
            conn,
            stream_id,
            buffer_max_events=max_events,
            buffer_max_wait_seconds=max_wait,
            start_seq=start_seq,
        )
        conn.commit()
        updated = stream_repo.get_stream(conn, stream_id)
        assert updated is not None
        return updated
    finally:
        conn.close()


def list_batches(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = batch_repo.list_batches(conn, limit=limit)
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
            r["reconciled"] = _reconciled(r)
        return rows
    finally:
        conn.close()


def list_projections(stream_id: int | None = None, include_deleted: bool = True) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = proj_repo.list_projections(
            conn, stream_id=stream_id, include_deleted=include_deleted
        )
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_events(
    batch_id: int | None = None, stream_id: int | None = None, result: str | None = None
) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return batch_repo.list_applied_events(
            conn, batch_id=batch_id, stream_id=stream_id, result=result
        )
    finally:
        conn.close()


def list_ooo_buffer(stream_id: int | None = None, status: str = "active") -> list[dict[str, Any]]:
    conn = connect()
    try:
        return buffer_repo.list_buffer(conn, stream_id=stream_id, status=status)
    finally:
        conn.close()


def buffer_summary() -> list[dict[str, Any]]:
    """Active buffer grouped by stream -> entity, joined with per-entity order state."""
    conn = connect()
    try:
        streams = stream_repo.list_streams(conn)
        out: list[dict[str, Any]] = []
        for s in streams:
            states = state_repo.list_states(conn, s["id"])
            state_by_key = {st["entity_key"]: st for st in states}
            entries = buffer_repo.list_buffer(conn, stream_id=s["id"], status="active")
            by_entity: dict[str, list[dict[str, Any]]] = {}
            for e in entries:
                by_entity.setdefault(e["entity_key"], []).append(e)
            entities: list[dict[str, Any]] = []
            keys = set(by_entity) | {k for k, st in state_by_key.items() if st["blocked"]}
            for key in sorted(keys):
                st = state_by_key.get(key)
                ent_entries = sorted(by_entity.get(key, []), key=lambda e: (e["seq"], e["id"]))
                expected = st["expected_seq"] if st else int(s["start_seq"])
                if ent_entries:
                    min_buf = min(e["seq"] for e in ent_entries)
                    gap = list(range(expected, min(min_buf, expected + 8)))
                else:
                    gap = []
                entities.append(
                    {
                        "entity_key": key,
                        "expected_seq": expected,
                        "blocked": bool(st["blocked"]) if st else False,
                        "blocked_reason": st["blocked_reason"] if st else None,
                        "gap": gap[:8],
                        "entries": ent_entries,
                    }
                )
            out.append(
                {
                    "stream_id": s["id"],
                    "code": s["code"],
                    "title": s["title"],
                    "config": {
                        "buffer_max_events": s["buffer_max_events"],
                        "buffer_max_wait_seconds": s["buffer_max_wait_seconds"],
                        "start_seq": s["start_seq"],
                        "overflow_policy": s["overflow_policy"],
                    },
                    "fill_fixture": s["fixture"].replace(".json", "_fill.json"),
                    "entities": entities,
                }
            )
        return out
    finally:
        conn.close()


def list_settings() -> dict[str, str]:
    conn = connect()
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


def run_project(
    stream_id: int, note: str | None = None, fixture: str | None = None
) -> dict[str, Any]:
    """Run one batch: read a CDC fixture in ARRIVAL order and apply per-entity ordering.

    Runs are incremental: projection, per-entity expected seq, buffer and
    blocked flags all persist across runs; already-seen event_ids are skipped
    as duplicates.
    """
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")
        fixture_name = fixture or stream["fixture"]
        events = _load_fixture(fixture_name)

        engine = EntityOrderEngine(
            states=state_repo.get_states(conn, stream_id),
            buffer_entries=buffer_repo.list_active(conn, stream_id),
            known_event_ids=batch_repo.list_known_event_ids(conn, stream_id),
            config=_stream_config(stream),
            now=datetime.now(timezone.utc),
        )
        result = engine.process(events)
        created_at = _now()
        batch_id, counts, reasons = _persist_result(
            conn, stream=stream, result=result, kind="run", note=note, created_at=created_at
        )
        conn.commit()

        batch = batch_repo.get_batch(conn, batch_id)
        assert batch is not None
        batch["stream_code"] = stream["code"]
        batch["stream_title"] = stream["title"]
        batch["fixture"] = fixture_name
        batch["counts"] = counts
        batch["reasons"] = reasons
        batch["reconciled"] = _reconciled(batch)
        batch["projection_preview"] = proj_repo.list_projections(conn, stream_id=stream_id)[:20]
        batch["event_preview"] = batch_repo.list_applied_events(conn, batch_id=batch_id, limit=30)
        return batch
    finally:
        conn.close()


def flush_entity_buffer(
    stream_id: int, entity_key: str, note: str | None = None
) -> dict[str, Any]:
    """人工清缓冲: explicitly allow the lowest buffered seq as the new starting
    seq, then release the entity's buffer in seq-continuous order."""
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        engine = EntityOrderEngine(
            states=state_repo.get_states(conn, stream_id),
            buffer_entries=buffer_repo.list_active(conn, stream_id),
            known_event_ids=batch_repo.list_known_event_ids(conn, stream_id),
            config=_stream_config(stream),
            now=datetime.now(timezone.utc),
        )
        result = engine.flush(entity_key)
        created_at = _now()
        batch_id, counts, reasons = _persist_result(
            conn,
            stream=stream,
            result=result,
            kind="flush",
            note=note or f"人工清缓冲 {entity_key}",
            created_at=created_at,
        )
        conn.commit()

        batch = batch_repo.get_batch(conn, batch_id)
        assert batch is not None
        batch["stream_code"] = stream["code"]
        batch["stream_title"] = stream["title"]
        batch["counts"] = counts
        batch["reasons"] = reasons
        batch["reconciled"] = _reconciled(batch)
        return batch
    finally:
        conn.close()
