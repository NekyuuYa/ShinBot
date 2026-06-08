"""Dynamic review-stage instruction prompt component."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.prompt_engine import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

if TYPE_CHECKING:
    from shinbot.agent.services.prompt_engine import PromptRegistry
    from shinbot.agent.services.prompt_engine.schema import PromptAssemblyRequest, PromptSource

logger = logging.getLogger(__name__)

REVIEW_STAGE_INSTRUCTION_RESOLVER = "review.stage.instruction"
REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS = {
    "overflow_compression": "review.overflow_compression.instruction",
    "review_scan": "review.review_scan.instruction",
    "block_digest": "review.block_digest.instruction",
    "reply_decision": "review.reply_decision.instruction",
    "active_chat_bootstrap": "review.active_chat_bootstrap.instruction",
    "idle_review_planning": "review.idle_review_planning.instruction",
}


def register_review_stage_instruction_components(registry: PromptRegistry) -> None:
    """Register dynamic instruction components shared by review runners."""

    for purpose, component_id in REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS.items():
        registry.upsert_component(
            PromptComponent(
                id=component_id,
                stage=PromptStage.INSTRUCTIONS,
                kind=PromptComponentKind.RESOLVER,
                resolver_ref=REVIEW_STAGE_INSTRUCTION_RESOLVER,
                priority=10,
                enabled=True,
                metadata={
                    "builtin": True,
                    "display_name": f"Review {purpose} Runtime Instruction",
                    "description": "Inject review-stage metadata and source messages.",
                    "review_stage": purpose,
                },
            )
        )
    if REVIEW_STAGE_INSTRUCTION_RESOLVER not in getattr(registry, "_resolvers", {}):
        registry.register_resolver(
            REVIEW_STAGE_INSTRUCTION_RESOLVER,
            resolve_review_stage_instruction,
        )


def review_stage_instruction_component_id(purpose: str) -> str:
    """Return the dynamic instruction component id for one review stage."""

    normalized = str(purpose or "").strip()
    return REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS.get(
        normalized,
        f"review.{normalized or 'stage'}.instruction",
    )


def resolve_review_stage_instruction(
    request: PromptAssemblyRequest,
    component: PromptComponent,
    _source: PromptSource,
) -> dict[str, Any]:
    """Render review-stage metadata and source message content."""

    purpose = str(
        component.metadata.get("review_stage")
        or request.metadata.get("review_stage")
        or request.metadata.get("stage_id")
        or ""
    ).strip()
    metadata = _mapping(request.metadata.get("review_stage_metadata"))
    if not metadata:
        metadata = {
            key: value
            for key, value in request.metadata.items()
            if key
            not in {
                "workflow_id",
                "stage_id",
                "review_stage",
                "review_stage_metadata",
                "review_instruction_content",
                "review_source_messages",
                "review_source_messages_text",
            }
        }
    metadata_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
    instruction = (
        f"Stage purpose: {purpose or request.metadata.get('stage_id', '')}\n"
        f"Metadata JSON: {metadata_json}"
    )
    content_blocks: list[dict[str, Any]] = [{"type": "text", "text": instruction}]

    instruction_content = _content_blocks(request.metadata.get("review_instruction_content"))
    if instruction_content:
        content_blocks.extend(instruction_content)
    else:
        formatted_text = str(request.metadata.get("review_source_messages_text") or "").strip()
        if formatted_text:
            content_blocks.append(
                {"type": "text", "text": "Source messages:\n" + formatted_text}
            )
        else:
            source_messages = _list(request.metadata.get("review_source_messages"))
            content_blocks.append(
                {
                    "type": "text",
                    "text": "Source messages JSON:\n"
                    + json.dumps(source_messages, ensure_ascii=False),
                }
            )

    return {
        "text": instruction,
        "content_blocks": content_blocks,
        "review_stage": purpose,
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _content_blocks(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    blocks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            blocks.append(dict(item))
    return blocks


__all__ = [
    "REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS",
    "REVIEW_STAGE_INSTRUCTION_RESOLVER",
    "register_review_stage_instruction_components",
    "resolve_review_stage_instruction",
    "review_stage_instruction_component_id",
]
