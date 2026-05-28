"""Session-scoped short alias allocation for context packing."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.agent.services.identity.store import IdentityStore

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
    """A single alias mapping for a platform user.

    Tracks the user's display name, short alias, message count, and
    last-seen timestamp for context-building and alias allocation.
    """

    alias: str = ""
    platform_id: str = ""
    display_name: str = ""
    message_count: int = 0
    last_seen_ms: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize the entry to a plain dictionary."""
        return {
            "alias": self.alias,
            "platform_id": self.platform_id,
            "display_name": self.display_name,
            "message_count": self.message_count,
            "last_seen_ms": self.last_seen_ms,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AliasEntry:
        """Deserialize an AliasEntry from a dictionary.

        Missing or malformed fields fall back to their default values.
        """
        return cls(
            alias=str(payload.get("alias", "") or ""),
            platform_id=str(payload.get("platform_id", "") or ""),
            display_name=str(payload.get("display_name", "") or ""),
            message_count=int(payload.get("message_count", 0) or 0),
            last_seen_ms=int(payload.get("last_seen_ms", 0) or 0),
        )


@dataclass(slots=True)
class SessionAliasTable:
    """Session-scoped alias table that maps platform user IDs to short aliases.

    Users are sorted by message frequency and recency, then assigned
    single-letter aliases (``A``-prefixed for frequent senders,
    ``P``-prefixed for other active participants) to keep context
    messages compact.
    """

    session_id: str
    entries: dict[str, AliasEntry] = field(default_factory=dict)
    last_activity_ms: int = 0
    last_rebuild_ms: int = 0
    rebuilt_since_activity: bool = False
    pending_rebuild: bool = False

    def note_activity(self, created_at: Any) -> None:
        """Record the timestamp of the latest activity in this session.

        Args:
            created_at: Timestamp to coerce (epoch seconds or milliseconds,
                or an object that can be cast to float). Silently ignored if
                the coerced value is non-positive.
        """
        activity_ms = _coerce_timestamp_ms(created_at)
        if activity_ms <= 0:
            return
        if activity_ms >= self.last_activity_ms:
            self.last_activity_ms = activity_ms
            self.rebuilt_since_activity = False

    def should_rebuild(self, now_ms: int, *, idle_ms: int = ALIAS_REBUILD_IDLE_MS) -> bool:
        """Determine whether the alias table needs a rebuild.

        A rebuild is triggered when a pending rebuild has been requested,
        the table has never been rebuilt after the first activity, or the
        specified idle period has elapsed since the last activity.

        Args:
            now_ms: Current time in milliseconds since epoch.
            idle_ms: Minimum idle duration in ms before triggering a
                rebuild. Defaults to :data:`ALIAS_REBUILD_IDLE_MS`.

        Returns:
            ``True`` if the table should be rebuilt.
        """
        if self.pending_rebuild:
            return True
        if self.last_activity_ms <= 0:
            return not self.rebuilt_since_activity
        if self.rebuilt_since_activity:
            return False
        return now_ms - self.last_activity_ms >= idle_ms

    def mark_rebuilt(self, now_ms: int) -> None:
        """Mark the table as successfully rebuilt at the given timestamp.

        Args:
            now_ms: Current time in milliseconds since epoch.
        """
        self.last_rebuild_ms = now_ms
        self.rebuilt_since_activity = True
        self.pending_rebuild = False

    def request_rebuild(self) -> None:
        """Request an immediate rebuild, bypassing idle-period checks."""
        self.pending_rebuild = True

    def rebuild_from_messages(
        self,
        messages: list[dict[str, Any]],
        *,
        now_ms: int,
        identity_store: IdentityStore | None = None,
        active_window_ms: int = ALIAS_ACTIVE_WINDOW_MS,
        frequent_limit: int = ALIAS_FREQUENT_LIMIT,
    ) -> bool:
        """Rebuild the alias table from a list of messages.

        Scans non-assistant messages to compute per-user frequency and
        recency statistics, then assigns short aliases. Frequent senders
        receive ``A``-prefixed aliases; other recently-active users
        receive ``P``-prefixed aliases.

        Args:
            messages: Ordered list of message dicts (must include
                ``role``, ``sender_id``, ``sender_name``, ``platform``,
                and ``created_at`` keys).
            now_ms: Current time in milliseconds since epoch.
            identity_store: Optional identity store used to overlay
                canonical display names onto entries.
            active_window_ms: Duration in ms within which a user is
                considered active. Defaults to
                :data:`ALIAS_ACTIVE_WINDOW_MS`.
            frequent_limit: Number of top senders (by message count)
                that receive ``A``-prefixed aliases. Defaults to
                :data:`ALIAS_FREQUENT_LIMIT`.

        Returns:
            ``True`` if the semantic signature of the table changed
            compared to the previous state.
        """
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
        sender_platforms: dict[str, str] = {}
        for message in messages:
            role = str(message.get("role", "") or "").strip()
            if role == "assistant":
                continue
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

            platform = str(message.get("platform", "") or "").strip()
            if platform and platform_id not in sender_platforms:
                sender_platforms[platform_id] = platform

            entry.message_count += 1
            entry.last_seen_ms = max(
                entry.last_seen_ms,
                _coerce_timestamp_ms(message.get("created_at", message.get("_created_at"))),
            )
            sender_name = str(message.get("sender_name", "") or "").strip()
            if sender_name:
                entry.display_name = sender_name

        if identity_store is not None:
            self._overlay_identity_display_names(
                stats,
                identity_store=identity_store,
                sender_platforms=sender_platforms,
            )

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

    def apply_identity_display_name(
        self,
        platform_id: str,
        display_name: str,
        *,
        now_ms: int,
        active_window_ms: int = ALIAS_ACTIVE_WINDOW_MS,
    ) -> bool:
        """Update a single entry's display name from an identity store.

        The update only applies if the platform ID exists in the table,
        the entry is still within the active window, and the new name
        differs from the current one.

        Args:
            platform_id: Platform-specific user identifier.
            display_name: Canonical display name to apply.
            now_ms: Current time in milliseconds since epoch.
            active_window_ms: Maximum age in ms for the entry to be
                eligible for update. Defaults to
                :data:`ALIAS_ACTIVE_WINDOW_MS`.

        Returns:
            ``True`` if the display name was updated.
        """
        normalized_platform_id = str(platform_id or "").strip()
        normalized_display_name = str(display_name or "").strip()
        if not normalized_platform_id or not normalized_display_name:
            return False

        entry = self.entries.get(normalized_platform_id)
        if entry is None:
            return False
        if now_ms - entry.last_seen_ms > active_window_ms:
            return False
        if entry.display_name == normalized_display_name:
            return False

        entry.display_name = normalized_display_name
        return True

    def resolve(self, platform_id: str) -> AliasEntry | None:
        """Look up an alias entry by platform user ID.

        Args:
            platform_id: Platform-specific user identifier.

        Returns:
            The matching :class:`AliasEntry`, or ``None`` if not found.
        """
        return self.entries.get(platform_id)

    def format_sender(self, platform_id: str) -> str:
        """Return the short alias for a platform user ID.

        Falls back to the raw ``platform_id`` when no matching entry
        exists or the entry has no assigned alias.

        Args:
            platform_id: Platform-specific user identifier.

        Returns:
            The alias string (e.g. ``"A0"``) or the original ID.
        """
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
        """Split entries into inactive and active lists.

        Entries whose last-seen timestamp is within the active window
        are placed in the active list; the rest go into inactive.
        Both lists are sorted by alias then platform ID.

        Args:
            now_ms: Current time in milliseconds since epoch.
            active_window_ms: Duration in ms that defines the active
                window. Defaults to :data:`ALIAS_ACTIVE_WINDOW_MS`.

        Returns:
            A tuple of ``(inactive_entries, active_entries)``.
        """
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
        """Assign prefixed aliases to a group of users.

        Members that already hold a valid alias with the given prefix
        retain it when the slot is free. Remaining members receive the
        lowest available slot numbers.
        """
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
        """Compute a hashable snapshot of the table's semantic state.

        Two tables are considered equivalent when their signatures
        match, avoiding unnecessary re-packing of context.
        """
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

    @staticmethod
    def _overlay_identity_display_names(
        entries: dict[str, AliasEntry],
        *,
        identity_store: IdentityStore,
        sender_platforms: dict[str, str],
    ) -> None:
        """Overlay canonical display names from the identity store.

        For each entry, looks up the identity by platform ID (and
        optional platform hint) and replaces the display name if a
        non-empty name is found.
        """
        for platform_id, entry in entries.items():
            identity = identity_store.get_identity(
                platform_id,
                platform=sender_platforms.get(platform_id, ""),
            )
            if identity is None:
                continue
            identity_name = str(identity.get("name", "") or "").strip()
            if identity_name:
                entry.display_name = identity_name

    def to_dict(self) -> dict[str, Any]:
        """Serialize the session alias table to a plain dictionary."""
        return {
            "session_id": self.session_id,
            "last_activity_ms": self.last_activity_ms,
            "last_rebuild_ms": self.last_rebuild_ms,
            "rebuilt_since_activity": self.rebuilt_since_activity,
            "pending_rebuild": self.pending_rebuild,
            "entries": [entry.to_dict() for entry in self.entries.values()],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> SessionAliasTable:
        """Deserialize a SessionAliasTable from a dictionary payload.

        Args:
            payload: Dictionary produced by :meth:`to_dict`. A ``None``
                or empty dict yields an empty table with default values.
        """
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
            pending_rebuild=bool(data.get("pending_rebuild", False)),
        )
