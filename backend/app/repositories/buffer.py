from __future__ import annotations

import json
from typing import Any

import sqlite3

ACTIVE_STATUSES = ("open", "held")


def _row(r: sqlite3.Row) -> dict[str, Any]:
    d = dict(r)
    d["payload"] = json.loads(d["payload_json"]) if d["payload_json"] else None
    d.pop("payload_json", None)
    return d


def insert_entry(conn: sqlite3.Connection, *, stream_id: int, entry: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO ooo_buffer(
            stream_id, event_id, entity_key, op, seq, event_time,
            payload_json, buffered_at, status
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            stream_id,
            entry["event_id"],
            entry["entity_key"],
            entry["op"],
            int(entry["seq"]),
            entry.get("event_time", ""),
            json.dumps(entry.get("payload"), ensure_ascii=False)
            if entry.get("payload") is not None
            else None,
            entry["buffered_at"],
            entry["status"],
        ),
    )


def list_active(conn: sqlite3.Connection, stream_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT * FROM ooo_buffer
        WHERE stream_id=? AND status IN ('open','held')
        ORDER BY entity_key, seq, buffered_at, event_id
        """,
        (stream_id,),
    ).fetchall()
    return [_row(r) for r in rows]


def set_status(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    event_id: str,
    status: str,
    released_batch_id: int | None = None,
    released_at: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE ooo_buffer
        SET status=?, released_batch_id=COALESCE(?, released_batch_id),
            released_at=COALESCE(?, released_at)
        WHERE stream_id=? AND event_id=?
        """,
        (status, released_batch_id, released_at, stream_id, event_id),
    )


def list_buffer(
    conn: sqlite3.Connection,
    *,
    stream_id: int | None = None,
    status: str = "active",
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM ooo_buffer WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    if status == "active":
        sql += " AND status IN ('open','held')"
    elif status != "all":
        sql += " AND status=?"
        params.append(status)
    sql += " ORDER BY stream_id, entity_key, seq, id"
    return [_row(r) for r in conn.execute(sql, params).fetchall()]


def count_active(conn: sqlite3.Connection, stream_id: int | None = None) -> int:
    if stream_id is None:
        r = conn.execute(
            "SELECT COUNT(*) c FROM ooo_buffer WHERE status IN ('open','held')"
        ).fetchone()
    else:
        r = conn.execute(
            "SELECT COUNT(*) c FROM ooo_buffer WHERE status IN ('open','held') AND stream_id=?",
            (stream_id,),
        ).fetchone()
    return int(r["c"])


def count_by_status(conn: sqlite3.Connection, status: str) -> int:
    r = conn.execute("SELECT COUNT(*) c FROM ooo_buffer WHERE status=?", (status,)).fetchone()
    return int(r["c"])
