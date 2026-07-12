"""Durable identity and aggregate models for per-session Agent actors."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from typing import Any

from shinbot.core.dispatch.agent_identity import SessionKey


class _FrozenDict(dict[str, Any]):
    """JSON-compatible dictionary that rejects in-place mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable aggregate mappings are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


class _FrozenList(list[Any]):
    """JSON-compatible list that rejects in-place mutation."""

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("durable aggregate lists are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable
    __imul__ = _immutable
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return _FrozenDict((str(key), _freeze_json(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return _FrozenList(_freeze_json(item) for item in value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("durable aggregate numbers must be finite")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"durable aggregate values must be JSON-compatible, got {type(value)!r}")


@dataclass(slots=True, frozen=True)
class AgentSessionAggregate:
    """Materialized state owned by exactly one logical session actor."""

    key: SessionKey
    ownership_generation: int = 0
    state: str = "idle"
    state_revision: int = 0
    event_sequence: int = 0
    activity_generation: int = 0
    active_epoch: int = 0
    current_plan_id: str = ""
    review_plan_revision: int = 0
    review_plan: dict[str, Any] = field(default_factory=dict)
    active_reply_resume: dict[str, Any] = field(default_factory=dict)
    active_chat_state: dict[str, Any] = field(default_factory=dict)
    review_operation_id: str = ""
    active_reply_operation_id: str = ""
    active_chat_round_operation_id: str = ""
    idle_planning_operation_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        """Validate monotonic counters stored with the aggregate."""

        counters = {
            "ownership_generation": self.ownership_generation,
            "state_revision": self.state_revision,
            "event_sequence": self.event_sequence,
            "activity_generation": self.activity_generation,
            "active_epoch": self.active_epoch,
            "review_plan_revision": self.review_plan_revision,
        }
        for name, value in counters.items():
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        updated_at = float(self.updated_at)
        if not math.isfinite(updated_at) or updated_at < 0:
            raise ValueError("updated_at must be finite and non-negative")
        object.__setattr__(self, "updated_at", updated_at)
        current_plan_id = str(self.current_plan_id or "").strip()
        if bool(current_plan_id) != (self.review_plan_revision > 0):
            raise ValueError(
                "current_plan_id must be present exactly when review_plan_revision is positive"
            )
        object.__setattr__(self, "current_plan_id", current_plan_id)
        for field_name in (
            "review_plan",
            "active_reply_resume",
            "active_chat_state",
            "data",
        ):
            object.__setattr__(self, field_name, _freeze_json(getattr(self, field_name)))

    @property
    def profile_id(self) -> str:
        """Return the owning runtime profile id."""

        return self.key.profile_id

    @property
    def session_id(self) -> str:
        """Return the bot-scoped session id."""

        return self.key.session_id

    @property
    def revision(self) -> int:
        """Compatibility alias for the authoritative state revision."""

        return self.state_revision

    def advance(
        self,
        *,
        state_changed: bool = True,
        updated_at: float | None = None,
        **changes: Any,
    ) -> AgentSessionAggregate:
        """Return the next aggregate revision for one handled mailbox event.

        Args:
            state_changed: Whether the event changed authoritative session state.
            updated_at: Optional transition timestamp.
            **changes: Additional aggregate fields to replace.

        Returns:
            A new aggregate with an incremented event sequence and, when
            requested, an incremented state revision.
        """

        if "key" in changes:
            raise ValueError("an aggregate transition cannot change its session key")
        if "state_revision" in changes or "event_sequence" in changes:
            raise ValueError("aggregate counters are managed by advance()")
        if not state_changed:
            changed_fields = [
                name
                for name, value in changes.items()
                if name != "updated_at" and value != getattr(self, name)
            ]
            if changed_fields:
                raise ValueError(
                    "state_changed=False cannot modify authoritative aggregate fields: "
                    + ", ".join(sorted(changed_fields))
                )
        for counter_name in ("activity_generation", "active_epoch"):
            if counter_name not in changes:
                continue
            if int(changes[counter_name]) < getattr(self, counter_name):
                raise ValueError(f"{counter_name} cannot move backwards")
        next_plan_id = str(changes.get("current_plan_id", self.current_plan_id) or "").strip()
        next_plan_revision = int(
            changes.get("review_plan_revision", self.review_plan_revision)
        )
        plan_changed = next_plan_id != self.current_plan_id
        plan_revision_changed = next_plan_revision != self.review_plan_revision
        if plan_changed != plan_revision_changed:
            raise ValueError(
                "current_plan_id and review_plan_revision must advance together"
            )
        if plan_revision_changed:
            if not next_plan_id:
                raise ValueError("current_plan_id must not be empty for a review plan")
            if next_plan_revision != self.review_plan_revision + 1:
                raise ValueError("review_plan_revision must advance exactly once")
        if updated_at is not None:
            changes["updated_at"] = updated_at
        next_updated_at = float(changes.get("updated_at", self.updated_at))
        if next_updated_at < self.updated_at:
            raise ValueError("updated_at cannot move backwards")
        return replace(
            self,
            state_revision=self.state_revision + (1 if state_changed else 0),
            event_sequence=self.event_sequence + 1,
            **changes,
        )


__all__ = ["AgentSessionAggregate", "SessionKey"]
