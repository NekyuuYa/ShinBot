"""Built-in prompt components for Agent active chat workflows."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.prompt_engine.dynamic_components import (
    ACTIVE_CHAT_FAST_MODE_BATCH_COMPONENT_ID,
)
from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig, register_prompt_files
from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

if TYPE_CHECKING:
    from shinbot.agent.services.prompt_engine import PromptRegistry
    from shinbot.agent.services.prompt_engine.schema import PromptAssemblyRequest, PromptSource

ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE: dict[str, dict[PromptStage, list[str]]] = {
    "fast_mode": {
        PromptStage.SYSTEM_BASE: ["active_chat.fast_mode.system"],
        PromptStage.CONSTRAINTS: ["active_chat.fast_mode.constraints"],
    },
}

# Actor v3 has a deliberately narrower terminal-action grammar than the legacy
# fast-mode workflow. Keep its component set separate so an actor-native round
# cannot accidentally inherit the legacy multi-action, optional-quote contract.
ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS: dict[str, dict[PromptStage, list[str]]] = {
    "round": {
        PromptStage.SYSTEM_BASE: ["active_chat.actor_v3.round.system"],
        PromptStage.CONSTRAINTS: ["active_chat.actor_v3.round.constraints"],
    },
}


def register_active_chat_prompt_components(
    registry: PromptRegistry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register built-in active chat workflow prompt components."""

    registry.upsert_component(
        PromptComponent(
            id=ACTIVE_CHAT_FAST_MODE_BATCH_COMPONENT_ID,
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.RESOLVER,
            resolver_ref="active_chat.fast_mode.batch",
            priority=10,
            enabled=True,
            metadata={
                "builtin": True,
                "display_name": "Active Chat Fast Mode Batch",
                "description": "Inject active chat batch metadata and source messages.",
                "active_chat_stage": "fast_mode",
            },
        )
    )

    register_prompt_files(
        registry,
        package=__package__,
        file_config=prompt_file_config,
        prompt_ids=[
            "active_chat.fast_mode.system",
            "active_chat.fast_mode.constraints",
            "active_chat.actor_v3.round.system",
            "active_chat.actor_v3.round.constraints",
            "active_chat.handoff.overflow",
            "active_chat.handoff.digest",
            "active_chat.handoff.legacy",
            "active_chat.fast_mode.repair",
            "active_chat.fast_mode.conversation_summary",
        ],
    )

    if not registry.has_resolver("active_chat.fast_mode.batch"):
        registry.register_resolver(
            "active_chat.fast_mode.batch",
            resolve_active_chat_fast_mode_batch,
        )


__all__ = [
    "ACTIVE_CHAT_PROMPT_COMPONENT_IDS_BY_STAGE",
    "ACTOR_ACTIVE_CHAT_V3_PROMPT_COMPONENT_IDS",
    "register_active_chat_prompt_components",
]


def resolve_active_chat_fast_mode_batch(
    request: PromptAssemblyRequest,
    component: PromptComponent,
    _source: PromptSource,
) -> dict[str, Any]:
    """Render active chat fast-mode batch instruction content."""

    batch_metadata = _filtered_metadata(
        request.metadata,
        exclude={
            "review_stage",
            "review_stage_metadata",
            "active_chat_instruction_content",
            "active_chat_source_messages",
            "active_chat_source_messages_text",
        },
    )
    batch_metadata_json = json.dumps(batch_metadata, ensure_ascii=False, sort_keys=True)
    instruction = (
        "主动聊天快速模式批次。通过工具决定一个即时动作。\n"
        f"会话 ID: {request.session_id}\n"
        f"消息日志 ID 列表: {json.dumps(request.metadata.get('message_log_ids', []), ensure_ascii=False)}\n"
        f"当前兴趣值: {float(request.metadata.get('interest_value', 0.0) or 0.0):.2f}\n"
        f"Metadata JSON: {batch_metadata_json}"
    )
    content_blocks = [{"type": "text", "text": instruction}]
    instruction_content = _content_blocks(request.metadata.get("active_chat_instruction_content"))
    if instruction_content:
        content_blocks.extend(instruction_content)
    else:
        formatted_text = str(request.metadata.get("active_chat_source_messages_text") or "").strip()
        if formatted_text:
            content_blocks.append({"type": "text", "text": "原始消息文本：\n" + formatted_text})
        else:
            source_messages = _list(request.metadata.get("active_chat_source_messages"))
            content_blocks.append(
                {
                    "type": "text",
                    "text": "原始消息 JSON:\n"
                    + json.dumps(source_messages, ensure_ascii=False),
                }
            )
    return {
        "text": instruction,
        "content_blocks": content_blocks,
        "active_chat_stage": "fast_mode",
    }


def _content_blocks(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    blocks: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            blocks.append(dict(item))
    return blocks


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _filtered_metadata(
    metadata: dict[str, Any],
    *,
    exclude: set[str],
) -> dict[str, Any]:
    return {key: value for key, value in metadata.items() if key not in exclude}
