"""Per-entity-key ordering buffer engine.

Core semantics (seq is scoped per (stream, entity_key); arrival order is the
fixture/feed order — events are NEVER re-sorted globally):

* An event may modify the projection only when its seq strictly equals the
  entity's expected next seq (a fresh entity starts at the stream's
  configured ``start_seq`` — the explicitly allowed starting seq).
* ``seq > expected`` -> the event goes into the auditable per-entity buffer
  (status ``open``) and must NOT touch the projection.
* ``seq < expected`` -> a stale late arrival; it is skipped and counted, and
  never overwrites a higher applied seq.
* Other entities are unaffected by one entity's buffer state.

Overflow policy (chosen once, applied consistently): **block**.
When an entity's local buffer exceeds its limit (buffered count >
``buffer_max_events``, or oldest buffered wait >= ``buffer_max_wait_seconds``)
the entity is *blocked*: its buffered events are held (status ``held``),
subsequent out-of-order events for it are held too, and nothing is dropped
(``dropped_overflow`` stays 0 — the drop-policy counter is kept for audit
symmetry only). A blocked entity still accepts the exact gap-filler
(``seq == expected``); it unblocks once its buffer fully drains — either by
filling the gap (补齐缺口) or by a manual flush (清缓冲, which explicitly
allows a new starting seq and counts the skipped gap). Release after either
path is always seq-continuous within the entity.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

ACTIVE_BUFFER_STATUSES = ("open", "held")


def parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def iso_now(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def new_counts() -> dict[str, int]:
    return {
        "source_events": 0,
        "applied_upsert": 0,
        "applied_delete": 0,
        "released_upsert": 0,
        "released_delete": 0,
        "flushed_upsert": 0,
        "flushed_delete": 0,
        "buffered": 0,
        "held_blocked": 0,
        "skipped_stale": 0,
        "skipped_duplicate": 0,
        "skipped_redundant": 0,
        "overflow_blocks": 0,
        "dropped_overflow": 0,  # always 0 under the chosen block policy
        "gap_skipped": 0,
        "max_seq": 0,
    }


def normalize_event(raw: dict[str, Any]) -> dict[str, Any]:
    try:
        event = {
            "event_id": str(raw["event_id"]),
            "entity_key": str(raw["entity_key"]),
            "op": str(raw.get("op", "upsert")),
            "seq": int(raw["seq"]),
            "event_time": str(raw.get("event_time", "")),
            "payload": raw.get("payload"),
        }
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(f"bad event: {raw!r}") from e
    if event["op"] not in ("upsert", "delete"):
        raise ValueError(f"bad op {event['op']!r} in event {event['event_id']!r}")
    return event


class EntityOrderEngine:
    """Stateful per-entity ordering over one stream.

    ``states``: {entity_key: {"expected_seq": int, "blocked": bool, "blocked_reason": str|None}}
    ``buffer_entries``: active (open/held) buffered events for the stream.
    The engine mutates its own copies; the service layer persists the diff.
    """

    def __init__(
        self,
        *,
        states: dict[str, dict[str, Any]],
        buffer_entries: list[dict[str, Any]],
        known_event_ids: set[str],
        config: dict[str, Any],
        now: datetime,
    ) -> None:
        self.states = states
        self.buffer = list(buffer_entries)
        self.known = set(known_event_ids)
        self.config = config
        self.now = now
        self.now_iso = iso_now(now)
        self.counts = new_counts()
        self.decisions: list[dict[str, Any]] = []
        self.projection_updates: list[dict[str, Any]] = []
        self.buffer_inserts: list[dict[str, Any]] = []
        self.buffer_status_updates: list[dict[str, Any]] = []
        self.reasons: list[str] = []

    # ---- entity state / buffer views -------------------------------------

    def _state(self, entity_key: str) -> dict[str, Any]:
        st = self.states.get(entity_key)
        if st is None:
            st = {
                "expected_seq": int(self.config.get("start_seq", 1)),
                "blocked": False,
                "blocked_reason": None,
            }
            self.states[entity_key] = st
        return st

    def _entries(self, entity_key: str) -> list[dict[str, Any]]:
        entries = [
            e
            for e in self.buffer
            if e["entity_key"] == entity_key and e["status"] in ACTIVE_BUFFER_STATUSES
        ]
        entries.sort(key=lambda e: (int(e["seq"]), str(e["buffered_at"]), str(e["event_id"])))
        return entries

    # ---- logging / applying ----------------------------------------------

    def _log(self, ev: dict[str, Any], result: str) -> None:
        self.decisions.append(
            {
                "event_id": ev["event_id"],
                "entity_key": ev["entity_key"],
                "op": ev["op"],
                "seq": int(ev["seq"]),
                "event_time": ev.get("event_time", ""),
                "result": result,
            }
        )

    def _apply(self, ev: dict[str, Any], prefix: str) -> None:
        deleted = ev["op"] == "delete"
        result = f"{prefix}_{'delete' if deleted else 'upsert'}"
        self.counts[result] += 1
        self.counts["max_seq"] = max(self.counts["max_seq"], int(ev["seq"]))
        self.projection_updates.append(
            {
                "entity_key": ev["entity_key"],
                "payload": None if deleted else (ev.get("payload") or {}),
                "applied_seq": int(ev["seq"]),
                "applied_event_id": ev["event_id"],
                "deleted": deleted,
            }
        )
        self._log(ev, result)

    # ---- overflow / blocking ----------------------------------------------

    def _block(self, entity_key: str, reason: str) -> None:
        st = self._state(entity_key)
        if st["blocked"]:
            return
        st["blocked"] = True
        st["blocked_reason"] = reason
        self.counts["overflow_blocks"] += 1
        self.reasons.append(f"实体 {entity_key} 缓冲逾限已阻塞（{reason}），后续该实体事件滞留，他实体不受影响")
        for e in self._entries(entity_key):
            if e["status"] == "open":
                e["status"] = "held"
                self.buffer_status_updates.append({"event_id": e["event_id"], "status": "held"})

    def _check_overflow(self, entity_key: str) -> None:
        entries = self._entries(entity_key)
        if not entries:
            return
        max_events = int(self.config.get("buffer_max_events", 5))
        if max_events >= 0 and len(entries) > max_events:
            self._block(entity_key, f"count_overflow:{len(entries)}>{max_events}")
            return
        max_wait = int(self.config.get("buffer_max_wait_seconds", 3600))
        if max_wait >= 0:
            oldest = min(parse_ts(e["buffered_at"]) for e in entries)
            age = (self.now - oldest).total_seconds()
            if age >= max_wait:
                self._block(entity_key, f"wait_overflow:{int(age)}s>={max_wait}s")

    def pre_run_sweep(self) -> None:
        """Re-check overflow limits against the already-persisted buffer."""
        for key in {e["entity_key"] for e in self.buffer if e["status"] in ACTIVE_BUFFER_STATUSES}:
            self._check_overflow(key)

    # ---- buffer drain -------------------------------------------------------

    def _set_buffer_status(self, entry: dict[str, Any], status: str) -> None:
        entry["status"] = status
        self.buffer_status_updates.append({"event_id": entry["event_id"], "status": status})
        if status not in ACTIVE_BUFFER_STATUSES and entry in self.buffer:
            self.buffer.remove(entry)

    def _drain(self, entity_key: str, prefix: str = "released") -> None:
        """Release buffered events while they are seq-continuous for the entity."""
        st = self._state(entity_key)
        while True:
            entries = self._entries(entity_key)
            if not entries:
                break
            first = entries[0]
            seq = int(first["seq"])
            if seq < st["expected_seq"]:
                # superseded while waiting (e.g. duplicate seq in buffer)
                self._set_buffer_status(first, "stale")
                self.counts["skipped_stale"] += 1
                self._log(first, "skipped_stale")
                continue
            if seq > st["expected_seq"]:
                break
            self._set_buffer_status(first, prefix)
            self._apply(first, prefix)
            st["expected_seq"] = seq + 1
        if st["blocked"] and not self._entries(entity_key):
            st["blocked"] = False
            st["blocked_reason"] = None
            self.reasons.append(f"实体 {entity_key} 缓冲已按 seq 连续排空，解除阻塞")

    # ---- main entry points ---------------------------------------------------

    def process(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """Process one run of source events in arrival order."""
        self.pre_run_sweep()
        self.counts["source_events"] = len(events)
        for raw in events:
            ev = normalize_event(raw)
            if ev["event_id"] in self.known:
                self.counts["skipped_duplicate"] += 1
                self._log(ev, "skipped_duplicate")
                continue
            self.known.add(ev["event_id"])
            st = self._state(ev["entity_key"])
            seq = ev["seq"]
            if seq < st["expected_seq"]:
                # stale late arrival: never overwrite a higher applied seq
                self.counts["skipped_stale"] += 1
                self._log(ev, "skipped_stale")
                continue
            if seq == st["expected_seq"]:
                # applies even when the entity is blocked: this is the gap-filler
                self._apply(ev, "applied")
                st["expected_seq"] = seq + 1
                self._drain(ev["entity_key"])
                continue
            # seq > expected: out-of-order, must not touch the projection
            if st["blocked"]:
                self._insert_buffer(ev, "held")
                self.counts["held_blocked"] += 1
                self._log(ev, "held_blocked")
                continue
            if any(int(e["seq"]) == seq for e in self._entries(ev["entity_key"])):
                # same entity+seq already waiting; redundant copy
                self.counts["skipped_redundant"] += 1
                self._log(ev, "skipped_redundant")
                continue
            self._insert_buffer(ev, "open")
            self.counts["buffered"] += 1
            self._log(ev, "buffered")
            self._check_overflow(ev["entity_key"])
        return self.result()

    def flush(self, entity_key: str) -> dict[str, Any]:
        """Manual 清缓冲: explicitly allow a new starting seq, release continuously."""
        entity_key = str(entity_key)
        entries = self._entries(entity_key)
        if not entries:
            raise ValueError(f"实体 {entity_key} 当前没有可清理的缓冲事件")
        st = self._state(entity_key)
        min_seq = int(entries[0]["seq"])
        gap = max(0, min_seq - st["expected_seq"])
        self.counts["gap_skipped"] = gap
        if min_seq > st["expected_seq"]:
            self.reasons.append(
                f"实体 {entity_key} 人工清缓冲：显式允许起始 seq {min_seq}，"
                f"跳过缺口 {gap} 条（seq {st['expected_seq']}..{min_seq - 1}）"
            )
            st["expected_seq"] = min_seq
        self._drain(entity_key, prefix="flushed")
        self.counts["source_events"] = (
            self.counts["flushed_upsert"] + self.counts["flushed_delete"] + self.counts["skipped_stale"]
        )
        return self.result()

    # ---- helpers -------------------------------------------------------------

    def _insert_buffer(self, ev: dict[str, Any], status: str) -> None:
        entry = {**ev, "status": status, "buffered_at": self.now_iso}
        self.buffer.append(entry)
        self.buffer_inserts.append(entry)

    def result(self) -> dict[str, Any]:
        return {
            "counts": self.counts,
            "decisions": self.decisions,
            "projection_updates": self.projection_updates,
            "buffer_inserts": self.buffer_inserts,
            "buffer_status_updates": self.buffer_status_updates,
            "states": self.states,
            "reasons": self.reasons,
        }
