"""Hot in-memory context pool for active sessions."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.context.utils.token_utils import estimate_role_content_tokens


def record_to_turn(item: dict[str, Any]) -> dict[str, Any] | None:
    """Convert a raw message record dict into a pre-processed turn dict.

    Returns None if the item has no usable content.
    """
    role = str(item.get("role", "") or "").strip()
    content = str(item.get("content") or item.get("raw_text") or "").strip()
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
    if "is_read" in item:
        turn["_is_read"] = bool(item.get("is_read"))
    raw_text = item.get("raw_text")
    if raw_text is not None:
        turn["_raw_text"] = str(raw_text)
    content_json = item.get("content_json")
    if content_json is not None:
        turn["_content_json"] = str(content_json)
    platform_msg_id = item.get("platform_msg_id")
    if platform_msg_id is not None:
        turn["_platform_msg_id"] = str(platform_msg_id)
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
        """Load provider results in chronological order and keep only the tail."""
        turns: list[dict[str, Any]] = []
        for item in items:
            turn = record_to_turn(item)
            if turn is not None:
                turns.append(turn)
        tail = turns[-self.max_messages :] if len(turns) > self.max_messages else turns
        self.messages = deque(tail, maxlen=self.max_messages)
        self._per_turn_tokens = deque(
            (estimate_role_content_tokens(t.get("role", ""), t["content"]) for t in tail),
            maxlen=self.max_messages,
        )
        self.token_estimate = sum(self._per_turn_tokens)

    def append(self, item: dict[str, Any]) -> None:
        turn = record_to_turn(item)
        if turn is None:
            return

        # Deduplicate against the tail because live message delivery can replay.
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

        tokens = estimate_role_content_tokens(turn.get("role", ""), turn["content"])

        if self.messages.maxlen and len(self.messages) >= self.messages.maxlen:
            self.token_estimate -= self._per_turn_tokens[0]
            self._per_turn_tokens.popleft()

        self.messages.append(turn)
        self._per_turn_tokens.append(tokens)
        self.token_estimate += tokens

    def export_turns(self, *, read_only: bool = True) -> list[dict[str, Any]]:
        """Return clean turn dicts suitable for prompt assembly."""
        turns: list[dict[str, Any]] = []
        for item in self.messages:
            if read_only and not bool(item.get("_is_read", True)):
                continue
            turn = {k: v for k, v in item.items() if not k.startswith("_")}
            turns.append(turn)
        return turns

    def export_records(self, *, read_only: bool = True) -> list[dict[str, Any]]:
        """Return prompt-building records with the raw fields builders depend on."""
        records: list[dict[str, Any]] = []
        for item in self.messages:
            if read_only and not bool(item.get("_is_read", True)):
                continue
            record = {k: v for k, v in item.items() if not k.startswith("_")}
            record_id = item.get("_record_id")
            if record_id is not None:
                record["id"] = record_id
            created_at = item.get("_created_at")
            if created_at is not None:
                record["created_at"] = created_at
            raw_text = item.get("_raw_text")
            if raw_text is not None:
                record["raw_text"] = raw_text
            content_json = item.get("_content_json")
            if content_json is not None:
                record["content_json"] = content_json
            platform_msg_id = item.get("_platform_msg_id")
            if platform_msg_id is not None:
                record["platform_msg_id"] = platform_msg_id
            records.append(record)
        return records

    def mark_read_until(self, msg_id: int) -> None:
        """Mark buffered message turns as readable up to the consumed cursor."""
        for item in self.messages:
            record_id = item.get("_record_id")
            if isinstance(record_id, int) and record_id <= msg_id:
                item["_is_read"] = True
