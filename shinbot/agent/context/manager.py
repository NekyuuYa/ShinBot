"""Active context pool and standardized retrieval manager."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.prompting.schema import ContextStrategy
from shinbot.persistence.records import MessageLogRecord
from shinbot.persistence.repos import ContextProvider

if TYPE_CHECKING:
    from shinbot.agent.identity import IdentityStore


def _estimate_single_turn_tokens(role: str, content: str) -> int:
    """Estimate tokens for a single turn using the standard heuristic."""
    text = f"{role}: {content}" if role else content
    if not text:
        return 0
    return max(len(text.split()), math.ceil(len(text) / 4))


def estimate_context_tokens(turns: list[dict[str, Any]], summary: str = "") -> int:
    """Estimate token usage using the same heuristic as prompt assembly."""
    text_parts = [summary] if summary else []
    text_parts.extend(
        f"{turn['role']}: {turn['content']}" if turn.get("role") else turn.get("content", "")
        for turn in turns
    )
    text = "\n".join(part for part in text_parts if part).strip()
    if not text:
        return 0
    word_estimate = len(text.split())
    char_estimate = math.ceil(len(text) / 4)
    return max(word_estimate, char_estimate)


def _record_to_turn(item: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a raw message record dict into a pre-processed turn dict.

    Returns None if the item has no usable content.
    """
    role = str(item.get("role", "") or "").strip()
    content = str(item.get("raw_text") or item.get("content") or "").strip()
    if not content:
        return None
    turn: dict[str, Any] = {"role": role, "content": content}
    sender_id = str(item.get("sender_id", "") or "").strip()
    if sender_id:
        turn["sender_id"] = sender_id
    sender_name = str(item.get("sender_name", "") or "").strip()
    if sender_name:
        turn["sender_name"] = sender_name
    platform = str(item.get("platform", "") or "").strip()
    if platform:
        turn["platform"] = platform
    # Preserve original record id for deduplication.
    record_id = item.get("id")
    if record_id is not None:
        turn["_record_id"] = record_id
    created_at = item.get("created_at")
    if created_at is not None:
        turn["_created_at"] = created_at
    return turn


@dataclass(slots=True)
class ActiveContextPool:
    """Hot in-memory context state for a single active session.

    Stores pre-processed turn dicts and maintains an incremental token
    estimate so that callers never pay O(n) on append or trim.
    """

    session_id: str
    max_messages: int = 50
    summary: str = ""
    messages: deque[dict[str, Any]] = field(default_factory=deque)
    token_estimate: int = 0
    _per_turn_tokens: deque[int] = field(default_factory=deque)

    def __post_init__(self) -> None:
        # Ensure deques have maxlen set from the start.
        if not self.messages.maxlen:
            self.messages = deque(self.messages, maxlen=self.max_messages)
            self._per_turn_tokens = deque(self._per_turn_tokens, maxlen=self.max_messages)

    def load(self, items: list[dict[str, Any]]) -> None:
        """Load from provider results.  Items are expected in chronological
        order (oldest first).  Only the last ``max_messages`` are retained."""
        turns: list[dict[str, Any]] = []
        for item in items:
            turn = _record_to_turn(item)
            if turn is not None:
                turns.append(turn)
        tail = turns[-self.max_messages :] if len(turns) > self.max_messages else turns
        self.messages = deque(tail, maxlen=self.max_messages)
        self._per_turn_tokens = deque(
            (_estimate_single_turn_tokens(t.get("role", ""), t["content"]) for t in tail),
            maxlen=self.max_messages,
        )
        self.token_estimate = sum(self._per_turn_tokens)

    def append(self, item: dict[str, Any]) -> None:
        turn = _record_to_turn(item)
        if turn is None:
            return

        # Deduplication against the tail.
        if self.messages:
            tail = self.messages[-1]
            record_id = turn.get("_record_id")
            if record_id is not None and tail.get("_record_id") == record_id:
                return
            if (
                record_id is None
                and tail.get("_record_id") is None
                and tail.get("role") == turn.get("role")
                and tail.get("content") == turn.get("content")
                and tail.get("_created_at") == turn.get("_created_at")
            ):
                return

        tokens = _estimate_single_turn_tokens(turn.get("role", ""), turn["content"])

        # If deque is at max capacity, the leftmost element is auto-evicted.
        if self.messages.maxlen and len(self.messages) >= self.messages.maxlen:
            self.token_estimate -= self._per_turn_tokens[0]
            # deque auto-pops from left; mirror in _per_turn_tokens
            self._per_turn_tokens.popleft()

        self.messages.append(turn)
        self._per_turn_tokens.append(tokens)
        self.token_estimate += tokens

    def export_turns(self) -> list[dict[str, Any]]:
        """Return turn dicts suitable for prompt assembly.

        Internal bookkeeping keys (``_record_id``, ``_created_at``) are
        stripped so the output is a clean list of turn dicts.
        """
        turns: list[dict[str, Any]] = []
        for item in self.messages:
            turn = {k: v for k, v in item.items() if not k.startswith("_")}
            turns.append(turn)
        return turns

    def trim_turns(self, count: int) -> int:
        removed = 0
        while removed < count and len(self.messages) > 1:
            self.token_estimate -= self._per_turn_tokens[0]
            self._per_turn_tokens.popleft()
            self.messages.popleft()
            removed += 1
        return removed

    def trim_ratio(self, ratio: float) -> int:
        if ratio <= 0:
            return 0
        n = len(self.messages)
        if n <= 1:
            return 0
        count = min(n - 1, max(1, math.floor(n * ratio)))
        return self.trim_turns(count)


class ContextManager:
    """Observer-backed session context manager with hot pools."""

    def __init__(
        self,
        provider: ContextProvider,
        *,
        preload_limit: int = 50,
        max_pool_messages: int = 200,
        identity_store: IdentityStore | None = None,
    ) -> None:
        self._provider = provider
        self._preload_limit = preload_limit
        self._max_pool_messages = max_pool_messages
        self._identity_store = identity_store
        self._pools: dict[str, ActiveContextPool] = {}
        self._session_policies: dict[str, dict[str, Any]] = {}

    def get_pool(self, session_id: str) -> ActiveContextPool:
        pool = self._pools.get(session_id)
        if pool is not None:
            return pool
        items = self._provider.get_recent(session_id, limit=self._preload_limit)
        pool = ActiveContextPool(session_id=session_id, max_messages=self._max_pool_messages)
        pool.load(items)
        self._pools[session_id] = pool
        return pool

    def track_message_record(self, record: MessageLogRecord, *, platform: str = "") -> None:
        if not record.session_id:
            return
        pool = self.get_pool(record.session_id)
        payload = {
            "id": record.id,
            "session_id": record.session_id,
            "role": record.role,
            "raw_text": record.raw_text,
            "created_at": record.created_at,
            "sender_id": record.sender_id,
            "sender_name": record.sender_name,
            "platform_msg_id": record.platform_msg_id,
            "platform": platform,
        }
        pool.append(payload)

        if self._identity_store is not None and record.role == "user" and record.sender_id.strip():
            self._identity_store.ensure_user(
                user_id=record.sender_id,
                suggested_name=record.sender_name,
                platform=platform,
            )

        self._apply_session_policy(record.session_id)

    def get_recent_messages(
        self, session_id: str, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        pool = self.get_pool(session_id)
        items = list(pool.messages)
        if limit is not None:
            items = items[-limit:]
        return items

    def get_context_inputs(
        self,
        session_id: str,
        *,
        fallback: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        payload = dict(fallback or {})
        if not session_id:
            return payload
        pool = self.get_pool(session_id)
        turns = pool.export_turns()
        if limit is not None:
            turns = turns[-limit:]
        payload["history_turns"] = turns
        payload["summary"] = payload.get("summary") or pool.summary
        payload["current_tokens"] = pool.token_estimate
        payload["context_source"] = "active_context_pool"
        return payload

    def set_session_policy(
        self,
        session_id: str,
        *,
        strategy: ContextStrategy,
        model_context_window: int | None,
    ) -> dict[str, Any]:
        if not session_id:
            return {"dropped_turns": 0, "remaining_turns": 0, "current_tokens": 0}
        self._session_policies[session_id] = {
            "strategy": strategy,
            "model_context_window": model_context_window,
        }
        return self._apply_session_policy(session_id)

    def apply_batch_ejection(
        self,
        session_id: str,
        *,
        strategy: ContextStrategy,
        model_context_window: int | None,
    ) -> dict[str, Any]:
        if not session_id:
            return {"dropped_turns": 0, "remaining_turns": 0, "current_tokens": 0}
        pool = self.get_pool(session_id)
        turns = pool.export_turns()
        current_tokens = pool.token_estimate
        trigger_ratio = strategy.budget.trigger_ratio
        max_context_tokens = strategy.budget.max_context_tokens or model_context_window
        if max_context_tokens is None:
            return {
                "dropped_turns": 0,
                "remaining_turns": len(turns),
                "current_tokens": current_tokens,
            }
        trigger_tokens = max(1, math.floor(max_context_tokens * trigger_ratio))
        if current_tokens < trigger_tokens or len(turns) <= 1:
            return {
                "dropped_turns": 0,
                "remaining_turns": len(turns),
                "current_tokens": current_tokens,
                "trigger_tokens": trigger_tokens,
            }

        trim_ratio = strategy.budget.trim_ratio
        trim_turns = strategy.budget.trim_turns
        if trim_ratio is not None:
            dropped = pool.trim_ratio(trim_ratio)
        else:
            dropped = pool.trim_turns(trim_turns)

        return {
            "dropped_turns": dropped,
            "remaining_turns": len(pool.export_turns()),
            "current_tokens": pool.token_estimate,
            "trigger_tokens": trigger_tokens,
            "trim_mode": "ratio" if trim_ratio is not None else "turns",
        }

    def _apply_session_policy(self, session_id: str) -> dict[str, Any]:
        policy = self._session_policies.get(session_id)
        if policy is None:
            pool = self.get_pool(session_id)
            return {
                "dropped_turns": 0,
                "remaining_turns": len(pool.export_turns()),
                "current_tokens": pool.token_estimate,
            }
        return self.apply_batch_ejection(
            session_id,
            strategy=policy["strategy"],
            model_context_window=policy["model_context_window"],
        )
