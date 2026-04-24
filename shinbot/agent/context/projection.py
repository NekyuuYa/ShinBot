"""Prompt-facing context projection contracts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.context.state_store import ContextBlockState


@dataclass(slots=True)
class PromptMemoryProjectionRequest:
    """Inputs needed to project session memory into prompt stages."""

    session_id: str
    unread_records: list[dict[str, Any]] = field(default_factory=list)
    previous_summary: str = ""
    self_platform_id: str = ""
    now_ms: int | None = None


@dataclass(slots=True)
class PromptMemoryBundle:
    """Context layer output consumed by PromptRegistry."""

    context_messages: list[dict[str, Any]] = field(default_factory=list)
    instruction_blocks: list[dict[str, Any]] = field(default_factory=list)
    constraint_text: str = ""
    cacheable_message_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PromptBlockProjection:
    """Intermediate block projection before Chat Completions content shaping."""

    block_id: str
    kind: str = "context"
    text_parts: list[str] = field(default_factory=list)
    token_estimate: int = 0
    sealed: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_content_blocks(self) -> list[dict[str, Any]]:
        return [{"type": "text", "text": text} for text in self.text_parts]


@dataclass(slots=True)
class LegacyBlockAdapter:
    """Adapter around the legacy ContextBlockState prompt-shaped storage."""

    block: Any

    @classmethod
    def from_projection(cls, projection: PromptBlockProjection) -> ContextBlockState:
        return ContextBlockState(
            block_id=projection.block_id,
            kind=projection.kind,
            token_estimate=projection.token_estimate,
            sealed=projection.sealed,
            contents=projection.to_content_blocks(),
            metadata=dict(projection.metadata),
        )

    def content_blocks(self) -> list[dict[str, Any]]:
        return [dict(item) for item in getattr(self.block, "contents", []) if isinstance(item, dict)]

    def text_parts(self) -> list[str]:
        parts: list[str] = []
        for content_block in self.content_blocks():
            if str(content_block.get("type") or "") != "text":
                continue
            text = str(content_block.get("text") or "").strip()
            if text:
                parts.append(text)
        return parts

    def to_prompt_message(self) -> dict[str, Any]:
        role = "assistant" if getattr(self.block, "kind", "") == "assistant" else "user"
        return {"role": role, "content": self.content_blocks()}


def projection_to_context_block(projection: PromptBlockProjection) -> ContextBlockState:
    return LegacyBlockAdapter.from_projection(projection)


def block_content_blocks(block: Any) -> list[dict[str, Any]]:
    return LegacyBlockAdapter(block).content_blocks()


def block_text_parts(block: Any) -> list[str]:
    return LegacyBlockAdapter(block).text_parts()


def block_to_prompt_message(block: Any) -> dict[str, Any]:
    return LegacyBlockAdapter(block).to_prompt_message()


@dataclass(slots=True)
class MessageIdProjector:
    """Assign stable short message IDs for prompt references."""

    allocator: Any

    def assign(self, record: dict[str, Any]) -> str:
        numeric_id = self.allocator.assign(make_record_key(record))
        return f"{numeric_id:04d}"


@dataclass(slots=True)
class ImageReferenceProjector:
    """Resolve image references for prompt-facing context text."""

    session_state: Any
    image_registry: Any

    def resolve(
        self,
        *,
        raw_hash: str,
        strict_dhash: str,
        summary_text: str = "",
        kind: str = "",
        is_custom_emoji: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        return self.image_registry.get_or_create_reference(
            session_state=self.session_state,
            raw_hash=raw_hash,
            strict_dhash=strict_dhash,
            summary_text=summary_text,
            kind=kind,
            is_custom_emoji=is_custom_emoji,
            metadata=metadata,
        )


@dataclass(slots=True)
class ContextProjectionState:
    """Mutable resources required while rendering context into prompt views."""

    message_ids: MessageIdProjector
    image_refs: ImageReferenceProjector

    @classmethod
    def from_session_state(
        cls,
        *,
        session_state: Any,
        image_registry: Any,
    ) -> ContextProjectionState:
        return cls(
            message_ids=MessageIdProjector(session_state.message_ids),
            image_refs=ImageReferenceProjector(
                session_state=session_state,
                image_registry=image_registry,
            ),
        )

    def assign_message_id(self, record: dict[str, Any]) -> str:
        return self.message_ids.assign(record)

    def resolve_image_reference(
        self,
        *,
        raw_hash: str,
        strict_dhash: str,
        summary_text: str = "",
        kind: str = "",
        is_custom_emoji: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> Any:
        return self.image_refs.resolve(
            raw_hash=raw_hash,
            strict_dhash=strict_dhash,
            summary_text=summary_text,
            kind=kind,
            is_custom_emoji=is_custom_emoji,
            metadata=metadata,
        )


def make_record_key(record: dict[str, Any]) -> str:
    record_id = record.get("id")
    if isinstance(record_id, int):
        return f"record:{record_id}"
    platform_msg_id = str(record.get("platform_msg_id", "") or "").strip()
    if platform_msg_id:
        return f"platform:{platform_msg_id}"
    sender_id = str(record.get("sender_id", "") or "").strip()
    created_at = str(record.get("created_at", "") or "").strip()
    raw_text = str(record.get("raw_text", "") or "").strip()
    digest = hashlib.sha1(f"{sender_id}|{created_at}|{raw_text}".encode()).hexdigest()
    return f"synthetic:{digest}"
