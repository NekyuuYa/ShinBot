"""Active context pool and standardized retrieval manager."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.prompting.schema import ContextStrategy
from shinbot.persistence.records import MessageLogRecord
from shinbot.persistence.repos import ContextProvider


def estimate_context_tokens(turns: list[dict[str, str]], summary: str = "") -> int:
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


@dataclass(slots=True)
class ActiveContextPool:
    """Hot in-memory context state for a single active session."""

    session_id: str
    max_messages: int = 50
    summary: str = ""
    messages: deque[dict[str, Any]] = field(default_factory=deque)
    token_estimate: int = 0

    def load(self, items: list[dict[str, Any]]) -> None:
        self.messages = deque(items[-self.max_messages :], maxlen=self.max_messages)
        self._recalculate_tokens()

    def append(self, item: dict[str, Any]) -> None:
        if self.messages:
            tail = self.messages[-1]
            if item.get("id") is not None and tail.get("id") == item.get("id"):
                return
            if (
                item.get("id") is None
                and tail.get("id") is None
                and tail.get("role") == item.get("role")
                and tail.get("raw_text") == item.get("raw_text")
                and tail.get("created_at") == item.get("created_at")
            ):
                return
        self.messages.append(item)
        self._recalculate_tokens()

    def export_turns(self) -> list[dict[str, str]]:
        turns: list[dict[str, str]] = []
        for item in self.messages:
            role = str(item.get("role", "") or "").strip()
            content = str(item.get("raw_text") or item.get("content") or "").strip()
            if not content:
                continue
            turns.append({"role": role, "content": content})
        return turns

    def trim_turns(self, count: int) -> int:
        removed = 0
        while removed < count and len(self.messages) > 1:
            self.messages.popleft()
            removed += 1
        if removed:
            self._recalculate_tokens()
        return removed

    def trim_ratio(self, ratio: float) -> int:
        if ratio <= 0:
            return 0
        turns = self.export_turns()
        if len(turns) <= 1:
            return 0
        count = min(len(turns) - 1, max(1, math.floor(len(turns) * ratio)))
        return self.trim_turns(count)

    def _recalculate_tokens(self) -> None:
        self.token_estimate = estimate_context_tokens(self.export_turns(), self.summary)


class ContextManager:
    """Observer-backed session context manager with hot pools."""

    def __init__(
        self,
        provider: ContextProvider,
        *,
        preload_limit: int = 50,
        max_pool_messages: int = 200,
    ) -> None:
        self._provider = provider
        self._preload_limit = preload_limit
        self._max_pool_messages = max_pool_messages
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

    def track_message_record(self, record: MessageLogRecord) -> None:
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
        }
        pool.append(payload)
        self._apply_session_policy(record.session_id)

    def get_recent_messages(self, session_id: str, *, limit: int | None = None) -> list[dict[str, Any]]:
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
