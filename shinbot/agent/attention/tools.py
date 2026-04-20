"""Attention tools — LLM-callable tools for attention governance."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

from shinbot.agent.tools.schema import (
    ToolDefinition,
    ToolExecutionContext,
    ToolOwnerType,
    ToolVisibility,
)

if TYPE_CHECKING:
    from shinbot.agent.attention.engine import AttentionEngine
    from shinbot.agent.context import ContextManager
    from shinbot.agent.tools.registry import ToolRegistry
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.persistence.engine import DatabaseManager

_OWNER_TYPE = ToolOwnerType.BUILTIN_MODULE
_OWNER_ID = "shinbot.agent.attention"
_TAG = "attention"


def register_attention_tools(
    registry: ToolRegistry,
    engine: AttentionEngine,
    adapter_manager: AdapterManager,
    database: DatabaseManager | None = None,
    context_manager: ContextManager | None = None,
) -> None:
    """Register all attention-related tools into the tool registry."""

    # ── attention.inspect_state ─────────────────────────────────────

    def _inspect_state(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        if not session_id:
            return {"error": "session_id not available in execution context"}
        return engine.inspect_state(session_id)

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.inspect_state",
            name="attention.inspect_state",
            description=(
                "查看当前会话的注意力状态。"
                "返回注意力水位区间（low/neutral/high/very_high）、有效阈值、"
                "冷却状态和各发言者权重分布摘要。"
                "无需参数，自动使用当前会话上下文。"
            ),
            input_schema={
                "type": "object",
                "properties": {},
                "required": [],
            },
            handler=_inspect_state,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=[_TAG],
        )
    )

    # ── attention.adjust_sender_weight ──────────────────────────────

    def _adjust_sender_weight(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        if not session_id:
            return {"error": "session_id not available in execution context"}
        sender_id = str(arguments.get("sender_id", ""))
        if not sender_id:
            return {"error": "sender_id is required"}
        return engine.adjust_sender_weight(
            session_id,
            sender_id,
            stable_delta=float(arguments.get("stable_delta", 0)),
            runtime_delta=float(arguments.get("runtime_delta", 0)),
        )

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.adjust_sender_weight",
            name="attention.adjust_sender_weight",
            description=(
                "调整某个发言者在当前会话中的权重。\n"
                "stable_delta: 长期权重增量，不随时间衰减。建议范围 ±0.1 ~ ±0.5，"
                "过大会导致响应偏置失控。\n"
                "runtime_delta: 短期权重增量，会平滑回归到 0。建议范围 ±0.2 ~ ±0.8。\n"
                "正值增强该用户对注意力的贡献，负值减弱。\n"
                "必须提供 reason 说明调整原因。\n"
                "返回值会明确说明是否发生了 clamp（权重是否已达上下限）。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sender_id": {
                        "type": "string",
                        "description": "要调整权重的发言者 ID",
                    },
                    "stable_delta": {
                        "type": "number",
                        "description": "长期权重增量 (建议 ±0.1~±0.5)",
                    },
                    "runtime_delta": {
                        "type": "number",
                        "description": "短期权重增量 (建议 ±0.2~±0.8)",
                    },
                    "reason": {
                        "type": "string",
                        "description": "调整原因（用于审计）",
                    },
                },
                "required": ["sender_id", "reason"],
            },
            handler=_adjust_sender_weight,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=[_TAG],
        )
    )

    # ── attention.adjust_session_threshold ───────────────────────────

    def _adjust_session_threshold(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        if not session_id:
            return {"error": "session_id not available in execution context"}
        return engine.adjust_session_threshold(
            session_id,
            offset_delta=float(arguments.get("offset_delta", 0)),
        )

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.adjust_session_threshold",
            name="attention.adjust_session_threshold",
            description=(
                "调整当前会话的注意力触发阈值。\n"
                "offset_delta: 阈值偏移增量。正值提高门槛（Bot 更沉默），"
                "负值降低门槛（Bot 更活跃）。建议范围 ±0.5 ~ ±2.0。\n"
                "偏移值会随时间自动回归到 0。\n"
                "必须提供 reason 说明调整原因。\n"
                "返回值会明确说明是否触及阈值边界。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "offset_delta": {
                        "type": "number",
                        "description": "阈值偏移增量 (建议 ±0.5~±2.0)",
                    },
                    "reason": {
                        "type": "string",
                        "description": "调整原因（用于审计）",
                    },
                },
                "required": ["offset_delta", "reason"],
            },
            handler=_adjust_session_threshold,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=[_TAG],
        )
    )

    # ── no_reply ────────────────────────────────────────────────────

    def _no_reply(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        if not session_id:
            return {"error": "session_id not available in execution context"}

        internal_summary = str(arguments.get("internal_summary", ""))
        if internal_summary:
            engine.repo.set_metadata_key(
                session_id,
                "internal_summary",
                internal_summary,
            )

        return {
            "action": "no_reply",
            "summary_stored": bool(internal_summary),
            "hint": "已记录观察摘要，下次触发时将作为短期记忆提供。",
        }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.no_reply",
            name="no_reply",
            description=(
                "选择不回复当前批次消息。\n"
                "可附带 internal_summary 记录对本次观察的摘要和不回复的原因。\n"
                "该摘要将在下一轮 workflow 触发时作为短期记忆提供给你，"
                "帮助你保持对话连贯性。\n"
                "使用场景：话题与你无关、不需要插嘴、暂时观望等。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "internal_summary": {
                        "type": "string",
                        "description": "对本次观察的摘要和不回复的原因",
                    },
                },
                "required": [],
            },
            handler=_no_reply,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=[_TAG],
        )
    )

    # ── send_reply ──────────────────────────────────────────────────

    async def _send_reply(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        instance_id = ctx.instance_id
        if not session_id:
            return {"error": "session_id not available in execution context"}
        if not instance_id:
            return {"error": "instance_id not available in execution context"}

        text = str(arguments.get("text", "")).strip()
        if not text:
            return {"error": "text is required and must not be empty"}
        terminate_round = bool(arguments.get("terminate_round", True))

        adapter = adapter_manager.get_instance(instance_id)
        if adapter is None:
            return {
                "error": f"Adapter not found for instance {instance_id}",
            }

        from shinbot.schema.elements import MessageElement

        elements = [MessageElement.text(text)]
        handle = await adapter.send(session_id, elements)

        assistant_log_id = None
        if database is not None:
            content_json = json.dumps(
                [element.model_dump(mode="json") for element in elements],
                ensure_ascii=False,
            )
            from shinbot.persistence.records import MessageLogRecord

            record = MessageLogRecord(
                session_id=session_id,
                platform_msg_id=handle.message_id if handle is not None else "",
                sender_id=adapter.instance_id,
                sender_name="",
                content_json=content_json,
                raw_text=text,
                role="assistant",
                is_read=True,
                is_mentioned=False,
                created_at=time.time() * 1000,
            )
            assistant_log_id = database.message_logs.insert(record)
            record.id = assistant_log_id
            if context_manager is not None:
                context_manager.track_message_record(record, platform=adapter.platform)

        return {
            "action": "send_reply",
            "sent": True,
            "length": len(text),
            "platform_msg_id": handle.message_id if handle is not None else "",
            "message_log_id": assistant_log_id,
            "terminate_round": terminate_round,
            "hint": "消息已发送至会话。",
        }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.send_reply",
            name="send_reply",
            description=(
                "向当前会话发送一条回复消息。\n"
                "这是你在注意力工作流中回复用户的唯一方式。\n"
                "若你不调用该工具，而是使用裸文本直接回复，用户将不会收到任何消息"
                "text: 要发送的回复文本内容。\n"
                "terminate_round: 是否在发送后立即结束当前 workflow 轮次；"
                "默认 true。若为 false，发送后允许模型继续后续推理或工具调用。\n"
                "注意：每次调用都会实际发送消息，请确保内容准确后再调用。\n"
                "如果你决定不回复，请使用 no_reply 工具代替。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "要发送的回复文本",
                    },
                    "terminate_round": {
                        "type": "boolean",
                        "description": (
                            "是否在发送后立即结束当前 workflow 轮次。"
                            "默认 true；若为 false，则允许模型继续本轮后续步骤。"
                        ),
                    },
                },
                "required": ["text"],
            },
            handler=_send_reply,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=[_TAG],
        )
    )

    # ── send_poke / poke_user ───────────────────────────────────────

    async def _send_poke(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        session_id = ctx.session_id
        instance_id = ctx.instance_id
        if not session_id:
            return {"error": "session_id not available in execution context"}
        if not instance_id:
            return {"error": "instance_id not available in execution context"}

        user_id = str(arguments.get("user_id", "") or "").strip()
        if not user_id:
            return {"error": "user_id is required"}
        terminate_round = bool(arguments.get("terminate_round", True))

        adapter = adapter_manager.get_instance(instance_id)
        if adapter is None:
            return {
                "error": f"Adapter not found for instance {instance_id}",
            }

        params: dict[str, Any] = {"user_id": user_id}
        session_type = _session_type(session_id)
        group_id = _group_id_from_session(session_id)
        if group_id:
            params["group_id"] = group_id

        result = await adapter.call_api(f"internal.{adapter.platform}.poke", params)

        return {
            "action": "send_poke",
            "sent": True,
            "user_id": user_id,
            "session_type": session_type or "unknown",
            "terminate_round": terminate_round,
            "adapter_result": result,
            "hint": "戳一戳已发送。",
        }

    poke_description = (
        "向当前会话中的某个用户发送一次“戳一戳”互动。\n"
        "适合轻量调侃、回应对方戳你、或用非文本方式做简短互动。\n"
        "user_id: 目标用户 ID，必须使用上下文中出现的原始用户 ID；"
        "不要把昵称填到 user_id。\n"
        "terminate_round: 是否在发送后结束当前 workflow 轮次，默认 true。\n"
        "注意：不要连续或无理由地戳同一个人；如果需要正常表达内容，请使用 send_reply。"
    )
    poke_schema = {
        "type": "object",
        "properties": {
            "user_id": {
                "type": "string",
                "description": "要戳一戳的目标用户 ID",
            },
            "terminate_round": {
                "type": "boolean",
                "description": "是否在发送后立即结束当前 workflow 轮次，默认 true",
            },
        },
        "required": ["user_id"],
    }

    for tool_name in ("send_poke", "poke_user"):
        registry.register_tool(
            ToolDefinition(
                id=f"{_OWNER_ID}.{tool_name}",
                name=tool_name,
                description=poke_description,
                input_schema=poke_schema,
                handler=_send_poke,
                owner_type=_OWNER_TYPE,
                owner_id=_OWNER_ID,
                visibility=ToolVisibility.PUBLIC,
                tags=[_TAG],
            )
        )


def _session_type(session_id: str) -> str:
    rest = _session_rest(session_id)
    if ":" not in rest:
        return rest
    return rest.split(":", 1)[0]


def _group_id_from_session(session_id: str) -> str:
    rest = _session_rest(session_id)
    if not rest.startswith("group:"):
        return ""
    group_part = rest[len("group:") :]
    return group_part.rsplit(":", 1)[-1].strip()


def _session_rest(session_id: str) -> str:
    colon_pos = session_id.find(":")
    if colon_pos == -1:
        return session_id
    return session_id[colon_pos + 1 :]
