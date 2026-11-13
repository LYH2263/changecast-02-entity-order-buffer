from __future__ import annotations

from typing import Any

import sqlite3


def get_states(conn: sqlite3.Connection, stream_id: int) -> dict[str, dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM entity_order_state WHERE stream_id=?",
        (stream_id,),
    ).fetchall()
    return {
        r["entity_key"]: {
            "expected_seq": int(r["expected_seq"]),
            "blocked": bool(r["blocked"]),
            "blocked_reason": r["blocked_reason"],
        }
        for r in rows
    }


def upsert_state(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    entity_key: str,
    expected_seq: int,
    blocked: bool,
    blocked_reason: str | None,
    updated_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO entity_order_state(stream_id, entity_key, expected_seq, blocked, blocked_reason, updated_at)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(stream_id, entity_key) DO UPDATE SET
            expected_seq=excluded.expected_seq,
            blocked=excluded.blocked,
            blocked_reason=excluded.blocked_reason,
            updated_at=excluded.updated_at
        """,
        (stream_id, entity_key, expected_seq, 1 if blocked else 0, blocked_reason, updated_at),
    )


def list_states(conn: sqlite3.Connection, stream_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM entity_order_state WHERE stream_id=? ORDER BY entity_key",
        (stream_id,),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["blocked"] = bool(d["blocked"])
        out.append(d)
    return out


def count_blocked(conn: sqlite3.Connection, stream_id: int | None = None) -> int:
    if stream_id is None:
        r = conn.execute("SELECT COUNT(*) c FROM entity_order_state WHERE blocked=1").fetchone()
    else:
        r = conn.execute(
            "SELECT COUNT(*) c FROM entity_order_state WHERE blocked=1 AND stream_id=?",
            (stream_id,),
        ).fetchone()
    return int(r["c"])
