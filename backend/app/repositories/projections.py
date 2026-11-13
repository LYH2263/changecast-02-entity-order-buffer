from __future__ import annotations

import json
from typing import Any

import sqlite3


def upsert_projection_row(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    row: dict[str, Any],
    updated_at: str,
) -> None:
    """Upsert one entity's projection row.

    The WHERE guard makes a lower-seq write a no-op even if a caller slips
    past the engine: an already-applied higher seq can never be overwritten
    by a late low-seq event at the storage layer either.
    """
    conn.execute(
        """
        INSERT INTO projection_rows(
            stream_id, entity_key, payload_json, applied_seq, applied_event_id, deleted, updated_at
        ) VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(stream_id, entity_key) DO UPDATE SET
            payload_json=excluded.payload_json,
            applied_seq=excluded.applied_seq,
            applied_event_id=excluded.applied_event_id,
            deleted=excluded.deleted,
            updated_at=excluded.updated_at
        WHERE excluded.applied_seq > projection_rows.applied_seq
        """,
        (
            stream_id,
            row["entity_key"],
            json.dumps(row.get("payload"), ensure_ascii=False)
            if row.get("payload") is not None
            else None,
            row["applied_seq"],
            row["applied_event_id"],
            1 if row.get("deleted") else 0,
            updated_at,
        ),
    )


def list_projections(
    conn: sqlite3.Connection,
    *,
    stream_id: int | None = None,
    include_deleted: bool = True,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM projection_rows WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    if not include_deleted:
        sql += " AND deleted=0"
    sql += " ORDER BY stream_id, entity_key"
    out: list[dict[str, Any]] = []
    for r in conn.execute(sql, params).fetchall():
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json")) if d["payload_json"] else None
        d.pop("payload_json", None)
        d["deleted"] = bool(d["deleted"])
        out.append(d)
    return out


def count_projections(conn: sqlite3.Connection, stream_id: int, deleted: bool) -> int:
    r = conn.execute(
        "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=? AND deleted=?",
        (stream_id, 1 if deleted else 0),
    ).fetchone()
    return int(r["c"])
