from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from shinbot.agent.context import ContextPoolRuntime


class FakeProvider:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items

    def get_recent(self, session_id: str, *, limit: int) -> list[dict[str, Any]]:
        return self.items[-limit:]


class FakeMediaService:
    def summarize_message_media(self, item: dict[str, Any]) -> list[str]:
        if item.get("content_json") == "with-image":
            return ["[image: cat]"]
        return []


@dataclass(slots=True)
class FakeRecord:
    id: int
    session_id: str
    role: str = "user"
    raw_text: str = ""
    content_json: str = "[]"
    created_at: int = 0
    sender_id: str = ""
    sender_name: str = ""
    platform_msg_id: str = ""
    is_read: bool = True


def test_pool_runtime_preloads_provider_records_and_merges_media_notes() -> None:
    runtime = ContextPoolRuntime(
        provider=FakeProvider(
            [
                {
                    "id": 1,
                    "session_id": "s-pool",
                    "role": "user",
                    "raw_text": "hello",
                    "content_json": "with-image",
                    "created_at": 1,
                    "sender_id": "user-1",
                    "sender_name": "Alice",
                    "is_read": True,
                }
            ]
        ),
        media_service=FakeMediaService(),  # type: ignore[arg-type]
    )

    records = runtime.get_recent_messages("s-pool")
    turns = runtime.get_context_inputs("s-pool")["history_turns"]

    assert records[0]["content"] == "hello [image: cat]"
    assert turns[0]["content"] == "hello [image: cat]"


def test_pool_runtime_append_and_mark_read_until_updates_context_inputs() -> None:
    runtime = ContextPoolRuntime(provider=FakeProvider([]))
    runtime.append_record(
        FakeRecord(
            id=1,
            session_id="s-pool",
            raw_text="unread",
            created_at=1,
            sender_id="user-1",
            is_read=False,
        ),  # type: ignore[arg-type]
        platform="qq",
    )

    assert runtime.get_context_inputs("s-pool")["history_turns"] == []

    runtime.mark_read_until("s-pool", 1)

    turns = runtime.get_context_inputs("s-pool")["history_turns"]
    assert turns[0]["content"] == "unread"
    assert turns[0]["platform"] == "qq"
