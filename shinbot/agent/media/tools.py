"""Media-related tools exposed to workflow agents."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shinbot.agent.tools.schema import (
    ToolDefinition,
    ToolExecutionContext,
    ToolOwnerType,
    ToolVisibility,
)

if TYPE_CHECKING:
    from shinbot.agent.media.inspection import MediaInspectionRunner
    from shinbot.agent.media.service import MediaService
    from shinbot.agent.tools.registry import ToolRegistry

_OWNER_TYPE = ToolOwnerType.BUILTIN_MODULE
_OWNER_ID = "shinbot.agent.media"


def register_media_tools(
    registry: ToolRegistry,
    media_service: MediaService,
    inspection_runner: MediaInspectionRunner,
) -> None:
    """Register media inspection tools into the tool registry."""

    async def _inspect_original(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        instance_id = ctx.instance_id
        if not session_id:
            return {"error": "session_id not available in execution context"}
        if not instance_id:
            return {"error": "instance_id not available in execution context"}

        question = str(arguments.get("question", "")).strip()
        if not question:
            return {"error": "question is required"}

        raw_hash = media_service.resolve_message_raw_hash(
            session_id=session_id,
            raw_hash=str(arguments.get("raw_hash", "")).strip(),
            message_log_id=arguments.get("message_log_id"),
            platform_msg_id=str(arguments.get("platform_msg_id", "")).strip(),
            fallback_to_latest=bool(arguments.get("fallback_to_latest", True)),
        )
        if not raw_hash:
            return {"error": "unable to resolve a target image from the current session"}

        result = await inspection_runner.answer_question(
            instance_id=instance_id,
            session_id=session_id,
            raw_hash=raw_hash,
            question=question,
        )
        if result is None:
            return {"error": "media reanalysis failed"}

        return {
            "raw_hash": result["raw_hash"],
            "answer": result["answer"],
            "inspection_agent_ref": result["inspection_agent_ref"],
            "inspection_llm_ref": result["inspection_llm_ref"],
        }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.inspect_original",
            name="media.inspect_original",
            description=(
                "回看当前会话中的原始图片，并把它当作普通图像重新分析。\n"
                "适用于用户追问“这张图里是谁”“图上写了什么”“这张表情包是什么人物”等情况。\n"
                "默认会回看当前会话最近一张带图片的消息，也可以显式提供 raw_hash、message_log_id 或 platform_msg_id。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "要针对该图片提出的具体问题",
                    },
                    "raw_hash": {
                        "type": "string",
                        "description": "可选，直接指定媒体 raw_hash",
                    },
                    "message_log_id": {
                        "type": "integer",
                        "description": "可选，指定某条消息日志 ID，从该消息里定位图片",
                    },
                    "platform_msg_id": {
                        "type": "string",
                        "description": "可选，指定平台消息 ID，从该消息里定位图片",
                    },
                    "fallback_to_latest": {
                        "type": "boolean",
                        "description": "未指定目标时，是否回退到当前会话最近一张图片",
                    },
                },
                "required": ["question"],
            },
            handler=_inspect_original,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=["media", "attention"],
        )
    )
