"""Model-facing message layout helpers for attention workflow runs."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from shinbot.agent.services.prompt_engine import PromptBuildResult, PromptRegistry, PromptStage

WORKFLOW_CONTROL_PROMPT = """
### Workflow 控制协议
- 用户看不见裸文本 assistant 输出；需要回复时必须调用 send_reply。
- 决定不回复时必须调用 no_reply，并可用 internal_summary 保留观察摘要。
- 需要轻量互动时可以调用 send_poke。
- 终局工具 no_reply / send_reply / send_poke 必须单独调用，不要和普通工具放在同一批。
- 回复具体消息时优先引用上下文里的 [msgid: 数字]，用 send_reply.quote_message_log_id 填数字。
""".strip()

FINAL_TAIL_REMINDER = (
    "现在请基于最新未读消息和 workflow 过程继续决策。不要输出裸文本；"
    "需要结束本轮时调用且只调用一个终局工具：send_reply、no_reply 或 send_poke。"
)


class AttentionWorkflowMessageLayout:
    """Build cache-friendly messages from assembled prompt material."""

    def build_initial(
        self,
        assembly: PromptBuildResult,
        *,
        explicit_prompt_cache_enabled: bool,
    ) -> list[dict[str, Any]]:
        stage_by_name = {stage.stage: stage for stage in assembly.stages}
        system_message = (
            deepcopy(assembly.messages[0])
            if assembly.messages
            else {"role": "system", "content": []}
        )
        messages: list[dict[str, Any]] = [system_message]

        context_stage = stage_by_name.get(PromptStage.CONTEXT)
        if context_stage is not None:
            messages.extend(deepcopy(context_stage.messages))

        control_blocks = self._collect_workflow_control_blocks(stage_by_name)
        control_blocks.append({"type": "text", "text": WORKFLOW_CONTROL_PROMPT})
        control_message = {"role": "user", "content": control_blocks}
        if explicit_prompt_cache_enabled:
            control_message = mark_cache_boundary(control_message)
        messages.append(control_message)

        unread_blocks = self._collect_unread_instruction_blocks(stage_by_name)
        if unread_blocks:
            messages.append({"role": "user", "content": unread_blocks})

        return messages

    def build_model_call(
        self,
        conversation_messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        messages = deepcopy(conversation_messages)
        messages.append(
            {
                "role": "user",
                "content": [{"type": "text", "text": FINAL_TAIL_REMINDER}],
            }
        )
        return messages

    def _collect_workflow_control_blocks(
        self,
        stage_by_name: dict[PromptStage, Any],
    ) -> list[dict[str, Any]]:
        blocks: list[dict[str, Any]] = []
        for stage in (
            PromptStage.COMPATIBILITY,
            PromptStage.INSTRUCTIONS,
            PromptStage.CONSTRAINTS,
        ):
            stage_block = stage_by_name.get(stage)
            if stage_block is None:
                continue
            for record in stage_block.components:
                if record.component_id == PromptRegistry.BUILTIN_INSTRUCTION_UNREAD_COMPONENT_ID:
                    continue
                blocks.extend(_record_to_content_blocks(record))
        return blocks

    def _collect_unread_instruction_blocks(
        self,
        stage_by_name: dict[PromptStage, Any],
    ) -> list[dict[str, Any]]:
        instruction_stage = stage_by_name.get(PromptStage.INSTRUCTIONS)
        if instruction_stage is None:
            return []
        for record in instruction_stage.components:
            if record.component_id == PromptRegistry.BUILTIN_INSTRUCTION_UNREAD_COMPONENT_ID:
                return deepcopy(record.rendered_content_blocks or [])
        return []


def mark_latest_workflow_segment_boundary(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    updated = list(messages)
    for index in range(len(updated) - 1, -1, -1):
        message = updated[index]
        if not isinstance(message, dict):
            continue
        marked = mark_cache_boundary(message)
        if marked != message:
            updated[index] = marked
            return updated
    return messages


def mark_cache_boundary(message: dict[str, Any]) -> dict[str, Any]:
    updated_message = deepcopy(message)
    content = updated_message.get("content")
    if isinstance(content, list):
        for block_index in range(len(content) - 1, -1, -1):
            block = content[block_index]
            if not isinstance(block, dict):
                continue
            if str(block.get("type") or "text") != "text":
                continue
            if not str(block.get("text", "") or "").strip():
                continue
            updated_block = dict(block)
            updated_block["cache_control"] = {"type": "ephemeral"}
            updated_content = list(content)
            updated_content[block_index] = updated_block
            updated_message["content"] = updated_content
            return updated_message
        return message
    if isinstance(content, str) and content.strip():
        updated_message["cache_control"] = {"type": "ephemeral"}
        return updated_message
    return message


def _record_to_content_blocks(record: Any) -> list[dict[str, Any]]:
    rendered_blocks = getattr(record, "rendered_content_blocks", None)
    if rendered_blocks:
        return deepcopy(rendered_blocks)
    rendered_text = str(getattr(record, "rendered_text", "") or "").strip()
    if rendered_text:
        return [{"type": "text", "text": rendered_text}]
    return []


__all__ = [
    "AttentionWorkflowMessageLayout",
    "FINAL_TAIL_REMINDER",
    "WORKFLOW_CONTROL_PROMPT",
    "mark_cache_boundary",
    "mark_latest_workflow_segment_boundary",
]
