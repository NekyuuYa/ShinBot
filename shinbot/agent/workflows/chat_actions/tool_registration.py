"""Chat action tools — LLM-callable actions for Agent chat workflows."""

from __future__ import annotations

import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from shinbot.agent.services.tools.schema import (
    ToolDefinition,
    ToolExecutionContext,
    ToolOwnerType,
    ToolVisibility,
)

if TYPE_CHECKING:
    from shinbot.agent.services.context import ContextManager
    from shinbot.agent.services.tools.registry import ToolRegistry
    from shinbot.core.platform.adapter_manager import AdapterManager
    from shinbot.persistence.engine import DatabaseManager

_OWNER_TYPE = ToolOwnerType.BUILTIN_MODULE
_OWNER_ID = "shinbot.agent.chat_actions"
CHAT_ACTION_TOOL_TAG = "chat_action"


@dataclass(slots=True, frozen=True)
class SendReplyIdempotencyClaim:
    """Result of checking whether one send_reply call may proceed."""

    accepted: bool
    deduplicated_reason: str = ""


class SendReplyIdempotencyStore:
    """Bounded in-memory idempotency guard for system-injected reply keys."""

    def __init__(
        self,
        *,
        ttl_seconds: float = 600.0,
        max_entries: int = 2048,
        now: Any | None = None,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._max_entries = max(1, max_entries)
        self._now = now or time.time
        self._completed: OrderedDict[str, float] = OrderedDict()
        self._in_flight: set[str] = set()

    def begin(self, key: str) -> SendReplyIdempotencyClaim:
        """Claim a key before sending; duplicate completed/in-flight keys are rejected."""

        key = key.strip()
        if not key:
            return SendReplyIdempotencyClaim(accepted=True)
        self._prune()
        if key in self._in_flight:
            return SendReplyIdempotencyClaim(
                accepted=False,
                deduplicated_reason="in_flight",
            )
        if key in self._completed:
            self._completed.move_to_end(key)
            return SendReplyIdempotencyClaim(
                accepted=False,
                deduplicated_reason="completed",
            )
        self._in_flight.add(key)
        return SendReplyIdempotencyClaim(accepted=True)

    def finish(self, key: str) -> None:
        """Mark a successfully sent key as completed."""

        key = key.strip()
        if not key:
            return
        self._in_flight.discard(key)
        self._completed[key] = float(self._now())
        self._completed.move_to_end(key)
        self._prune()

    def release(self, key: str) -> None:
        """Release an in-flight key after a failed send so it can be retried."""

        key = key.strip()
        if key:
            self._in_flight.discard(key)

    def _prune(self) -> None:
        now = float(self._now())
        if self._ttl_seconds > 0:
            expired = [
                key
                for key, created_at in self._completed.items()
                if now - created_at > self._ttl_seconds
            ]
            for key in expired:
                self._completed.pop(key, None)
        while len(self._completed) > self._max_entries:
            self._completed.popitem(last=False)


def register_chat_action_tools(
    registry: ToolRegistry,
    *,
    adapter_manager: AdapterManager,
    database: DatabaseManager | None = None,
    context_manager: ContextManager | None = None,
    send_reply_idempotency_store: SendReplyIdempotencyStore | None = None,
) -> None:
    """Register shared chat action tools (send_reply, no_reply, send_poke)."""

    idempotency_store = send_reply_idempotency_store or SendReplyIdempotencyStore()

    # ── no_reply ────────────────────────────────────────────────────

    def _no_reply(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        return {
            "action": "no_reply",
            "summary_stored": False,
            "hint": "已记录观察摘要，下次触发时将作为短期记忆提供。",
        }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.no_reply",
            name="no_reply",
            description=(
                "选择不回复当前批次消息。\n"
                "可附带 internal_summary 记录对本次观察的摘要和不回复的原因。\n"
                "该摘要将在下一轮聊天 workflow 触发时作为短期记忆提供给你，"
                "帮助你保持对话连贯性。"
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
            tags=[CHAT_ACTION_TOOL_TAG],
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

        idempotency_key = str(arguments.get("idempotency_key") or "").strip()
        text = str(arguments.get("text", "")).strip()
        if not text:
            return {"error": "text is required and must not be empty"}
        terminate_round = bool(arguments.get("terminate_round", True))
        quote_message_id = _resolve_quote_message_id(
            arguments,
            database=database,
            session_id=session_id,
        )
        if isinstance(quote_message_id, dict):
            raise ValueError(str(quote_message_id.get("error", "invalid quote target")))

        adapter = adapter_manager.get_instance(instance_id)
        if adapter is None:
            return {
                "error": f"Adapter not found for instance {instance_id}",
            }
        if not adapter_manager.is_connected(instance_id):
            raise RuntimeError(f"Platform adapter {instance_id} is offline")

        idempotency_claim = idempotency_store.begin(idempotency_key)
        if not idempotency_claim.accepted:
            return {
                "action": "send_reply",
                "sent": False,
                "deduplicated": True,
                "deduplicated_reason": idempotency_claim.deduplicated_reason,
                "idempotency_key": idempotency_key,
                "hint": "此回复已通过相同 idempotency_key 发送或正在发送，跳过重复发送。",
            }

        from shinbot.schema.elements import MessageElement

        try:
            elements = []
            if quote_message_id:
                elements.append(MessageElement.quote(quote_message_id))
            elements.append(MessageElement.text(text))
            handle = await adapter.send(session_id, elements)
        except Exception:
            idempotency_store.release(idempotency_key)
            raise
        idempotency_store.finish(idempotency_key)

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
            "quote_message_id": quote_message_id,
            "idempotency_key": idempotency_key,
            "terminate_round": terminate_round,
            "hint": "消息已发送至会话。",
        }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.send_reply",
            name="send_reply",
            description=(
                "向当前会话发送一条文本回复。这是 Agent 聊天 workflow 中让用户看见"
                "回复的唯一方式；裸文本 assistant 输出不会发送。\n"
                "必填 text：实际发送给用户的文本。\n"
                "引用回复：当你是在回答某一条具体消息，尤其是纠正、逐条回应、回答"
                "问题、接梗或避免上下文歧义时，应优先引用该消息。\n"
                "优先使用 quote_message_log_id：填写上下文里 [msgid:123] 的数字 123，"
                "不要带中括号、前缀、昵称或消息正文。\n"
                "如果上下文明确给出了原平台 platform_msg_id，也可以改用 "
                "quote_message_id；不要同时填写两个引用字段。\n"
                "terminate_round 默认 true：发送后结束本次聊天 workflow。只有确实需要"
                "继续调用工具或继续多步行动时才设为 false。\n"
                "如果决定不回复，请调用 no_reply；不要用空文本或裸文本代替。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "要实际发送给用户的文本；不能为空。",
                    },
                    "quote_message_id": {
                        "type": "string",
                        "description": (
                            "可选。要引用回复的原平台消息 ID，通常来自上下文明确提供的 "
                            "platform_msg_id。若使用 quote_message_log_id，则不要填写此字段。"
                        ),
                    },
                    "quote_message_log_id": {
                        "type": "integer",
                        "description": (
                            "可选，推荐用于引用。填写上下文里 [msgid:123] 的数字 123；"
                            "系统会自动解析对应 platform_msg_id。若使用 quote_message_id，"
                            "则不要填写此字段。"
                        ),
                    },
                    "terminate_round": {
                        "type": "boolean",
                        "description": (
                            "是否在发送后立即结束当前聊天 workflow 轮次。"
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
            tags=[CHAT_ACTION_TOOL_TAG],
        )
    )

    # ── send_poke ───────────────────────────────────────────────────

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
        if not adapter_manager.is_connected(instance_id):
            raise RuntimeError(f"Platform adapter {instance_id} is offline")

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
        "向当前会话中的某个用户发送一次平台「戳一戳」互动。\n"
        "适用场景：回应对方戳你、轻量调侃、或用非文本方式做极短互动。\n"
        "必填 user_id：目标用户的原始 sender_id/user_id，必须来自上下文，"
        "不要填写昵称、群名、@展示名或 message id。\n"
        "terminate_round 默认 true：发送后结束本次聊天 workflow。只有确实需要继续"
        "调用工具或继续多步行动时才设为 false。\n"
        "如果需要表达具体内容、回答问题或引用某条消息，请使用 send_reply。"
    )
    poke_schema = {
        "type": "object",
        "properties": {
            "user_id": {
                "type": "string",
                "description": "要戳一戳的目标用户原始 ID，必须来自上下文的 sender_id/user_id。",
            },
            "terminate_round": {
                "type": "boolean",
                "description": "是否在发送后立即结束当前聊天 workflow 轮次，默认 true",
            },
        },
        "required": ["user_id"],
    }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.send_poke",
            name="send_poke",
            description=poke_description,
            input_schema=poke_schema,
            handler=_send_poke,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=[CHAT_ACTION_TOOL_TAG],
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
    group_part = rest[len("group:"):]
    return group_part.rsplit(":", 1)[-1].strip()


def _session_rest(session_id: str) -> str:
    colon_pos = session_id.find(":")
    if colon_pos == -1:
        return session_id
    return session_id[colon_pos + 1:]


def _resolve_quote_message_id(
    arguments: dict[str, Any],
    *,
    database: DatabaseManager | None,
    session_id: str,
) -> str | dict[str, str]:
    quote_message_id = _first_non_empty_str(
        arguments,
        "quote_message_id",
        "reply_to_message_id",
        "quote_platform_msg_id",
    )
    if quote_message_id:
        return quote_message_id

    raw_log_id = _first_present(
        arguments,
        "quote_message_log_id",
        "reply_to_message_log_id",
    )
    if raw_log_id in (None, ""):
        return ""
    if database is None:
        return {"error": "database not available to resolve quote_message_log_id"}

    try:
        message_log_id = int(raw_log_id)
    except (TypeError, ValueError):
        return {"error": "quote_message_log_id must be an integer"}
    if message_log_id <= 0:
        return {"error": "quote_message_log_id must be positive"}

    record = database.message_logs.get(message_log_id)
    if record is None:
        return {"error": f"message_log_id {message_log_id} not found"}
    if str(record.get("session_id") or "") != session_id:
        return {"error": f"message_log_id {message_log_id} is not in current session"}

    platform_msg_id = str(record.get("platform_msg_id") or "").strip()
    if not platform_msg_id:
        return {
            "error": (
                f"message_log_id {message_log_id} has no platform_msg_id "
                "and cannot be quoted"
            )
        }
    return platform_msg_id


def _first_non_empty_str(arguments: dict[str, Any], *keys: str) -> str:
    value = _first_present(arguments, *keys)
    return str(value or "").strip()


def _first_present(arguments: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in arguments:
            return arguments[key]
    return None


__all__ = [
    "CHAT_ACTION_TOOL_TAG",
    "SendReplyIdempotencyClaim",
    "SendReplyIdempotencyStore",
    "register_chat_action_tools",
]
