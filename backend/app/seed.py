from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from app.db import connect

STREAM_SEEDS = [
    {
        "code": "member_profile",
        "title": "会员档案变更",
        "domain": "member",
        "entity_key_name": "member_id",
        "payload_fields": ["display_name", "tier", "region"],
        "fixture": "member_profile.json",
        "enabled": 1,
        "buffer_max_events": 5,
        "buffer_max_wait_seconds": 3600,
        "start_seq": 1,
    },
    {
        "code": "store_archive",
        "title": "门店档案变更",
        "domain": "store",
        "entity_key_name": "store_id",
        "payload_fields": ["name", "status", "city"],
        "fixture": "store_archive.json",
        "enabled": 1,
        "buffer_max_events": 5,
        "buffer_max_wait_seconds": 3600,
        "start_seq": 1,
    },
    {
        "code": "channel_attr",
        "title": "渠道属性变更",
        "domain": "channel",
        "entity_key_name": "channel_code",
        "payload_fields": ["label", "active"],
        "fixture": "channel_attr.json",
        "enabled": 1,
        "buffer_max_events": 5,
        "buffer_max_wait_seconds": 3600,
        "start_seq": 1,
    },
    {
        "code": "reorder_lab",
        "title": "乱序演练流",
        "domain": "lab",
        "entity_key_name": "item_id",
        "payload_fields": ["label", "qty"],
        "fixture": "reorder_lab.json",
        "enabled": 1,
        "buffer_max_events": 2,
        "buffer_max_wait_seconds": 600,
        "start_seq": 1,
    },
]

DEFAULT_SETTINGS = {
    "apply_order": "per_entity_seq",
    "duplicate_policy": "skip",
    "tombstone_mode": "soft_delete",
    "overflow_policy": "block",
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    import json

    conn = connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS project_streams(
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                domain TEXT NOT NULL,
                entity_key_name TEXT NOT NULL,
                payload_fields_json TEXT NOT NULL,
                fixture TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                watermark_seq INTEGER NOT NULL DEFAULT 0,
                buffer_max_events INTEGER NOT NULL DEFAULT 5,
                buffer_max_wait_seconds INTEGER NOT NULL DEFAULT 3600,
                start_seq INTEGER NOT NULL DEFAULT 1,
                overflow_policy TEXT NOT NULL DEFAULT 'block',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS project_batches(
                id INTEGER PRIMARY KEY,
                batch_no TEXT NOT NULL UNIQUE,
                stream_id INTEGER NOT NULL,
                kind TEXT NOT NULL DEFAULT 'run',
                status TEXT NOT NULL,
                source_events INTEGER NOT NULL,
                applied_upsert INTEGER NOT NULL,
                applied_delete INTEGER NOT NULL,
                released_upsert INTEGER NOT NULL DEFAULT 0,
                released_delete INTEGER NOT NULL DEFAULT 0,
                flushed_upsert INTEGER NOT NULL DEFAULT 0,
                flushed_delete INTEGER NOT NULL DEFAULT 0,
                buffered INTEGER NOT NULL DEFAULT 0,
                held_blocked INTEGER NOT NULL DEFAULT 0,
                skipped_duplicate INTEGER NOT NULL,
                skipped_stale INTEGER NOT NULL DEFAULT 0,
                skipped_redundant INTEGER NOT NULL DEFAULT 0,
                overflow_blocks INTEGER NOT NULL DEFAULT 0,
                dropped_overflow INTEGER NOT NULL DEFAULT 0,
                gap_skipped INTEGER NOT NULL DEFAULT 0,
                buffer_open_after INTEGER NOT NULL DEFAULT 0,
                blocked_entities_after INTEGER NOT NULL DEFAULT 0,
                projected_alive INTEGER NOT NULL,
                projected_deleted INTEGER NOT NULL,
                max_seq INTEGER NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS projection_rows(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                entity_key TEXT NOT NULL,
                payload_json TEXT,
                applied_seq INTEGER NOT NULL,
                applied_event_id TEXT NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(stream_id, entity_key),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS applied_events(
                id INTEGER PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                result TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(batch_id) REFERENCES project_batches(id),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS ooo_buffer(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                payload_json TEXT,
                buffered_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                released_batch_id INTEGER,
                released_at TEXT,
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS entity_order_state(
                stream_id INTEGER NOT NULL,
                entity_key TEXT NOT NULL,
                expected_seq INTEGER NOT NULL,
                blocked INTEGER NOT NULL DEFAULT 0,
                blocked_reason TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(stream_id, entity_key),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        # migrations for pre-cut databases
        for col, ddl in [
            ("buffer_max_events", "buffer_max_events INTEGER NOT NULL DEFAULT 5"),
            ("buffer_max_wait_seconds", "buffer_max_wait_seconds INTEGER NOT NULL DEFAULT 3600"),
            ("start_seq", "start_seq INTEGER NOT NULL DEFAULT 1"),
            ("overflow_policy", "overflow_policy TEXT NOT NULL DEFAULT 'block'"),
        ]:
            _ensure_column(conn, "project_streams", col, ddl)
        for col, ddl in [
            ("kind", "kind TEXT NOT NULL DEFAULT 'run'"),
            ("released_upsert", "released_upsert INTEGER NOT NULL DEFAULT 0"),
            ("released_delete", "released_delete INTEGER NOT NULL DEFAULT 0"),
            ("flushed_upsert", "flushed_upsert INTEGER NOT NULL DEFAULT 0"),
            ("flushed_delete", "flushed_delete INTEGER NOT NULL DEFAULT 0"),
            ("buffered", "buffered INTEGER NOT NULL DEFAULT 0"),
            ("held_blocked", "held_blocked INTEGER NOT NULL DEFAULT 0"),
            ("skipped_stale", "skipped_stale INTEGER NOT NULL DEFAULT 0"),
            ("skipped_redundant", "skipped_redundant INTEGER NOT NULL DEFAULT 0"),
            ("overflow_blocks", "overflow_blocks INTEGER NOT NULL DEFAULT 0"),
            ("dropped_overflow", "dropped_overflow INTEGER NOT NULL DEFAULT 0"),
            ("gap_skipped", "gap_skipped INTEGER NOT NULL DEFAULT 0"),
            ("buffer_open_after", "buffer_open_after INTEGER NOT NULL DEFAULT 0"),
            ("blocked_entities_after", "blocked_entities_after INTEGER NOT NULL DEFAULT 0"),
        ]:
            _ensure_column(conn, "project_batches", col, ddl)
        for col, ddl in [
            ("released_batch_id", "released_batch_id INTEGER"),
            ("released_at", "released_at TEXT"),
        ]:
            _ensure_column(conn, "ooo_buffer", col, ddl)

        now = _now()
        for s in STREAM_SEEDS:
            exists = conn.execute(
                "SELECT 1 FROM project_streams WHERE code=?", (s["code"],)
            ).fetchone()
            if not exists:
                conn.execute(
                    """
                    INSERT INTO project_streams(
                        code, title, domain, entity_key_name, payload_fields_json,
                        fixture, enabled, watermark_seq,
                        buffer_max_events, buffer_max_wait_seconds, start_seq,
                        overflow_policy, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        s["code"],
                        s["title"],
                        s["domain"],
                        s["entity_key_name"],
                        json.dumps(s["payload_fields"], ensure_ascii=False),
                        s["fixture"],
                        s["enabled"],
                        0,
                        s["buffer_max_events"],
                        s["buffer_max_wait_seconds"],
                        s["start_seq"],
                        "block",
                        now,
                    ),
                )
        for key, value in DEFAULT_SETTINGS.items():
            conn.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        conn.commit()
    finally:
        conn.close()
