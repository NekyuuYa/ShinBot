"""Session-scoped short alias allocation for context packing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

ALIAS_REBUILD_IDLE_MS = 10 * 60 * 1000
ALIAS_ACTIVE_WINDOW_MS = 24 * 60 * 60 * 1000
ALIAS_FREQUENT_LIMIT = 10


def _coerce_timestamp_ms(value: Any) -> int:
    if value is None:
        return 0
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return 0
    return int(raw if raw > 10_000_000_000 else raw * 1000)


@dataclass(slots=True)
class AliasEntry:
    alias: str = ""
    platform_id: str = ""
    display_name: str = ""
    message_count: int = 0
    last_seen_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "alias": self.alias,
            "platform_id": self.platform_id,
            "display_name": self.display_name,
            "message_count": self.message_count,
            "last_seen_ms": self.last_seen_ms,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AliasEntry:
        return cls(
            alias=str(payload.get("alias", "") or ""),
            platform_id=str(payload.get("platform_id", "") or ""),
            display_name=str(payload.get("display_name", "") or ""),
            message_count=int(payload.get("message_count", 0) or 0),
            last_seen_ms=int(payload.get("last_seen_ms", 0) or 0),
        )


@dataclass(slots=True)
class SessionAliasTable:
    session_id: str
    entries: dict[str, AliasEntry] = field(default_factory=dict)
    last_activity_ms: int = 0
    last_rebuild_ms: int = 0
    rebuilt_since_activity: bool = False

    def note_activity(self, created_at: Any) -> None:
        activity_ms = _coerce_timestamp_ms(created_at)
        if activity_ms <= 0:
            return
        if activity_ms >= self.last_activity_ms:
            self.last_activity_ms = activity_ms
            self.rebuilt_since_activity = False

    def should_rebuild(self, now_ms: int, *, idle_ms: int = ALIAS_REBUILD_IDLE_MS) -> bool:
        if self.last_activity_ms <= 0:
            return not self.rebuilt_since_activity
        if self.rebuilt_since_activity:
            return False
        return now_ms - self.last_activity_ms >= idle_ms

    def mark_rebuilt(self, now_ms: int) -> None:
        self.last_rebuild_ms = now_ms
        self.rebuilt_since_activity = True

    def rebuild_from_messages(
        self,
        messages: list[dict[str, Any]],
        *,
        now_ms: int,
        active_window_ms: int = ALIAS_ACTIVE_WINDOW_MS,
        frequent_limit: int = ALIAS_FREQUENT_LIMIT,
    ) -> bool:
        previous_entries = {
            platform_id: AliasEntry(
                alias=entry.alias,
                platform_id=entry.platform_id,
                display_name=entry.display_name,
                message_count=entry.message_count,
                last_seen_ms=entry.last_seen_ms,
            )
            for platform_id, entry in self.entries.items()
        }
        stats: dict[str, AliasEntry] = {}
        for message in messages:
            platform_id = str(message.get("sender_id", "") or "").strip()
            if not platform_id:
                continue
            entry = stats.get(platform_id)
            if entry is None:
                entry = AliasEntry(
                    platform_id=platform_id,
                    display_name=str(message.get("sender_name", "") or platform_id).strip()
                    or platform_id,
                )
                stats[platform_id] = entry

            entry.message_count += 1
            entry.last_seen_ms = max(
                entry.last_seen_ms,
                _coerce_timestamp_ms(message.get("created_at", message.get("_created_at"))),
            )
            sender_name = str(message.get("sender_name", "") or "").strip()
            if sender_name:
                entry.display_name = sender_name

        ordered = sorted(
            stats.values(),
            key=lambda item: (-item.message_count, -item.last_seen_ms, item.platform_id),
        )

        frequent_ids = [entry.platform_id for entry in ordered[:frequent_limit]]
        active_ids = [
            entry.platform_id
            for entry in ordered[frequent_limit:]
            if now_ms - entry.last_seen_ms <= active_window_ms
        ]

        self._assign_group_aliases(
            stats,
            member_ids=frequent_ids,
            prefix="A",
            previous_entries=previous_entries,
        )
        self._assign_group_aliases(
            stats,
            member_ids=active_ids,
            prefix="P",
            previous_entries=previous_entries,
        )

        next_entries = {entry.platform_id: entry for entry in ordered}
        changed = self._semantic_signature(
            previous_entries,
            now_ms=now_ms,
            active_window_ms=active_window_ms,
        ) != self._semantic_signature(
            next_entries,
            now_ms=now_ms,
            active_window_ms=active_window_ms,
        )

        self.entries = next_entries
        self.mark_rebuilt(now_ms)
        return changed

    def resolve(self, platform_id: str) -> AliasEntry | None:
        return self.entries.get(platform_id)

    def format_sender(self, platform_id: str) -> str:
        entry = self.resolve(platform_id)
        if entry is None:
            return platform_id
        return entry.alias or entry.platform_id

    def split_by_activity(
        self,
        *,
        now_ms: int,
        active_window_ms: int = ALIAS_ACTIVE_WINDOW_MS,
    ) -> tuple[list[AliasEntry], list[AliasEntry]]:
        inactive: list[AliasEntry] = []
        active: list[AliasEntry] = []
        for entry in self.entries.values():
            if now_ms - entry.last_seen_ms <= active_window_ms:
                active.append(entry)
            else:
                inactive.append(entry)
        active.sort(key=lambda item: (item.alias.startswith("P"), item.alias, item.platform_id))
        inactive.sort(key=lambda item: (item.alias.startswith("P"), item.alias, item.platform_id))
        return inactive, active

    @staticmethod
    def _assign_group_aliases(
        entries: dict[str, AliasEntry],
        *,
        member_ids: list[str],
        prefix: str,
        previous_entries: dict[str, AliasEntry],
    ) -> None:
        claimed: set[int] = set()
        remaining: list[str] = []

        for platform_id in member_ids:
            previous = previous_entries.get(platform_id)
            alias = previous.alias if previous is not None else ""
            if not alias.startswith(prefix):
                remaining.append(platform_id)
                continue

            suffix = alias[len(prefix) :]
            if not suffix.isdigit():
                remaining.append(platform_id)
                continue

            slot = int(suffix)
            if slot in claimed:
                remaining.append(platform_id)
                continue

            entries[platform_id].alias = f"{prefix}{slot}"
            claimed.add(slot)

        next_slot = 0
        for platform_id in remaining:
            while next_slot in claimed:
                next_slot += 1
            entries[platform_id].alias = f"{prefix}{next_slot}"
            claimed.add(next_slot)
            next_slot += 1

    @staticmethod
    def _semantic_signature(
        entries: dict[str, AliasEntry],
        *,
        now_ms: int,
        active_window_ms: int,
    ) -> tuple[tuple[str, str, str, bool], ...]:
        return tuple(
            sorted(
                (
                    platform_id,
                    entry.alias,
                    entry.display_name,
                    now_ms - entry.last_seen_ms <= active_window_ms,
                )
                for platform_id, entry in entries.items()
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "last_activity_ms": self.last_activity_ms,
            "last_rebuild_ms": self.last_rebuild_ms,
            "rebuilt_since_activity": self.rebuilt_since_activity,
            "entries": [entry.to_dict() for entry in self.entries.values()],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> SessionAliasTable:
        data = payload or {}
        entries = {
            entry.platform_id: entry
            for raw in data.get("entries", [])
            if isinstance(raw, dict)
            for entry in [AliasEntry.from_dict(raw)]
        }
        return cls(
            session_id=str(data.get("session_id", "") or ""),
            entries=entries,
            last_activity_ms=int(data.get("last_activity_ms", 0) or 0),
            last_rebuild_ms=int(data.get("last_rebuild_ms", 0) or 0),
            rebuilt_since_activity=bool(data.get("rebuilt_since_activity", False)),
        )
