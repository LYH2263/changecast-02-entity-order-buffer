from __future__ import annotations

from typing import Any

import sqlite3

COUNT_COLUMNS = [
    "source_events",
    "applied_upsert",
    "applied_delete",
    "released_upsert",
    "released_delete",
    "flushed_upsert",
    "flushed_delete",
    "buffered",
    "held_blocked",
    "skipped_duplicate",
    "skipped_stale",
    "skipped_redundant",
    "overflow_blocks",
    "dropped_overflow",
    "gap_skipped",
    "buffer_open_after",
    "blocked_entities_after",
    "projected_alive",
    "projected_deleted",
    "max_seq",
]

# decision results that consume source events of a run batch
RUN_DECISION_RESULTS = [
    "applied_upsert",
    "applied_delete",
    "buffered",
    "held_blocked",
    "skipped_duplicate",
    "skipped_stale",
    "skipped_redundant",
]


def insert_batch(
    conn: sqlite3.Connection,
    *,
    batch_no: str,
    stream_id: int,
    kind: str,
    status: str,
    counts: dict[str, int],
    note: str | None,
    created_at: str,
) -> int:
    columns = ["batch_no", "stream_id", "kind", "status", *COUNT_COLUMNS, "note", "created_at"]
    values = [batch_no, stream_id, kind, status]
    values += [int(counts.get(c, 0)) for c in COUNT_COLUMNS]
    values += [note, created_at]
    placeholders = ",".join("?" for _ in columns)
    cur = conn.execute(
        f"INSERT INTO project_batches({','.join(columns)}) VALUES ({placeholders})",
        values,
    )
    return int(cur.lastrowid)


def list_batches(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM project_batches ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM project_batches WHERE id=?", (batch_id,)).fetchone()
    return dict(r) if r else None


def insert_applied_event(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    stream_id: int,
    event: dict[str, Any],
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO applied_events(
            batch_id, stream_id, event_id, entity_key, op, seq, event_time, result, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            batch_id,
            stream_id,
            event["event_id"],
            event["entity_key"],
            event["op"],
            event["seq"],
            event["event_time"],
            event["result"],
            created_at,
        ),
    )


def list_applied_events(
    conn: sqlite3.Connection,
    *,
    batch_id: int | None = None,
    stream_id: int | None = None,
    result: str | None = None,
    limit: int = 300,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM applied_events WHERE 1=1"
    params: list[Any] = []
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    if result:
        sql += " AND result=?"
        params.append(result)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def list_known_event_ids(conn: sqlite3.Connection, stream_id: int) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT event_id FROM applied_events WHERE stream_id=?",
        (stream_id,),
    ).fetchall()
    return {r["event_id"] for r in rows}


def count_events_by_result(conn: sqlite3.Connection, result: str) -> int:
    r = conn.execute(
        "SELECT COUNT(*) c FROM applied_events WHERE result=?", (result,)
    ).fetchone()
    return int(r["c"])
