"""Runtime wrapper for hot active context pools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shinbot.agent.context.state.active_pool import ActiveContextPool

if TYPE_CHECKING:
    from shinbot.agent.media import MediaService
    from shinbot.persistence.records import MessageLogRecord
    from shinbot.persistence.repos import ContextProvider


@dataclass(slots=True)
class ContextPoolRuntime:
    """Own active session pools and normalize records before they enter memory."""

    provider: ContextProvider
    preload_limit: int = 50
    max_pool_messages: int = 200
    media_service: MediaService | None = None
    pools: dict[str, ActiveContextPool] = field(default_factory=dict)

    def get_pool(self, session_id: str) -> ActiveContextPool:
        pool = self.pools.get(session_id)
        if pool is not None:
            return pool
        items = self.provider.get_recent(session_id, limit=self.preload_limit)
        pool = ActiveContextPool(session_id=session_id, max_messages=self.max_pool_messages)
        pool.load([self.build_pool_payload(item) for item in items])
        self.pools[session_id] = pool
        return pool

    def append_record(self, record: MessageLogRecord, *, platform: str = "") -> None:
        if not record.session_id:
            return
        pool = self.get_pool(record.session_id)
        pool.append(
            self.build_pool_payload(
                {
                    "id": record.id,
                    "session_id": record.session_id,
                    "role": record.role,
                    "raw_text": record.raw_text,
                    "content_json": record.content_json,
                    "created_at": record.created_at,
                    "sender_id": record.sender_id,
                    "sender_name": record.sender_name,
                    "platform_msg_id": record.platform_msg_id,
                    "platform": platform,
                    "is_read": record.is_read,
                }
            )
        )

    def build_pool_payload(self, item: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "id": item.get("id"),
            "session_id": item.get("session_id", ""),
            "role": item.get("role", ""),
            "raw_text": item.get("raw_text", ""),
            "created_at": item.get("created_at"),
            "sender_id": item.get("sender_id", ""),
            "sender_name": item.get("sender_name", ""),
            "platform_msg_id": item.get("platform_msg_id", ""),
            "platform": item.get("platform", ""),
            "is_read": bool(item.get("is_read", False)),
            "content_json": item.get("content_json", "[]"),
        }
        merged_content = self.compose_content(payload)
        if merged_content:
            payload["content"] = merged_content
        return payload

    def compose_content(self, item: dict[str, Any]) -> str:
        text = str(item.get("raw_text") or "").strip()
        if self.media_service is None:
            return text

        media_notes = self.media_service.summarize_message_media(item)
        if text and media_notes:
            return f"{text} {' '.join(media_notes)}"
        if media_notes:
            return " ".join(media_notes)
        return text

    def get_recent_messages(
        self,
        session_id: str,
        *,
        limit: int | None = None,
        read_only: bool = True,
    ) -> list[dict[str, Any]]:
        pool = self.get_pool(session_id)
        items = pool.export_records(read_only=read_only)
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

    def mark_read_until(self, session_id: str, msg_id: int) -> None:
        self.get_pool(session_id).mark_read_until(msg_id)
