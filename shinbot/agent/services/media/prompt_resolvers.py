"""Dynamic prompt resolvers for media inspection workflows."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shinbot.agent.services.media.prompt_building import (
    build_media_data_url,
    build_media_inspection_instruction_text,
    build_media_question_text,
    build_sticker_summary_instruction_text,
)
from shinbot.agent.services.prompt_engine import PromptComponent, PromptComponentKind, PromptStage
from shinbot.agent.services.prompt_engine.dynamic_components import (
    media_instruction_component_id,
)

if TYPE_CHECKING:
    from shinbot.agent.services.prompt_engine import PromptRegistry
    from shinbot.agent.services.prompt_engine.schema import PromptAssemblyRequest, PromptSource


MEDIA_PROMPT_RESOLVER = "media.dynamic.instruction"


def register_media_instruction_components(registry: PromptRegistry) -> None:
    """Register media instruction resolver components."""

    for trigger in ("media_inspection", "sticker_summary", "media_reanalysis"):
        component_id = media_instruction_component_id(trigger)
        registry.upsert_component(
            PromptComponent(
                id=component_id,
                stage=PromptStage.INSTRUCTIONS,
                kind=PromptComponentKind.RESOLVER,
                resolver_ref=MEDIA_PROMPT_RESOLVER,
                priority=10,
                enabled=True,
                metadata={
                    "builtin": True,
                    "display_name": f"Media {trigger} Instruction",
                    "description": "Inject media instruction text and image block.",
                    "media_trigger": trigger,
                },
            )
        )
    if MEDIA_PROMPT_RESOLVER not in getattr(registry, "_resolvers", {}):
        registry.register_resolver(MEDIA_PROMPT_RESOLVER, resolve_media_instruction)


def resolve_media_instruction(
    request: PromptAssemblyRequest,
    component: PromptComponent,
    _source: PromptSource,
) -> dict[str, Any]:
    """Render the dynamic media instruction block for one media workflow."""

    trigger = str(
        component.metadata.get("media_trigger")
        or request.metadata.get("media_trigger")
        or request.stage_id
        or ""
    ).strip()
    raw_hash = str(request.metadata.get("raw_hash") or "").strip()
    session_id = str(request.session_id or "").strip()
    instance_id = str(request.instance_id or "").strip()
    asset = _mapping(request.metadata.get("asset"))
    occurrence = _mapping(request.metadata.get("occurrence"))
    instruction_text = str(
        request.metadata.get("instruction_text")
        or request.template_inputs.get("message_text")
        or ""
    ).strip()
    if not instruction_text:
        if trigger == "media_reanalysis":
            question = str(request.metadata.get("question") or "").strip()
            instruction_text = build_media_question_text(
                session_id=session_id,
                raw_hash=raw_hash,
                asset=asset,
                question=question,
            )
        elif trigger == "sticker_summary":
            instruction_text = build_sticker_summary_instruction_text(
                session_id=session_id,
                raw_hash=raw_hash,
                asset=asset,
                occurrence=occurrence,
            )
        else:
            instruction_text = build_media_inspection_instruction_text(
                session_id=session_id,
                raw_hash=raw_hash,
                asset=asset,
                occurrence=occurrence,
            )

    image_block = {
        "type": "image_url",
        "image_url": {"url": build_media_data_url(asset)},
    }
    content_blocks = [
        {"type": "text", "text": instruction_text},
        image_block,
    ]
    metadata = {
        "media_trigger": trigger,
        "raw_hash": raw_hash,
        "session_id": session_id,
        "instance_id": instance_id,
    }
    if question := str(request.metadata.get("question") or "").strip():
        metadata["question"] = question
    return {
        "text": instruction_text,
        "content_blocks": content_blocks,
        **metadata,
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


__all__ = [
    "MEDIA_PROMPT_RESOLVER",
    "register_media_instruction_components",
    "resolve_media_instruction",
]
