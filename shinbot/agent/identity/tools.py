"""LLM-callable tools for maintaining participant identities."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from shinbot.agent.tools.schema import (
    ToolDefinition,
    ToolExecutionContext,
    ToolOwnerType,
    ToolVisibility,
)

if TYPE_CHECKING:
    from shinbot.agent.context.manager import ContextManager
    from shinbot.agent.identity.store import IdentityStore
    from shinbot.agent.tools.registry import ToolRegistry

_OWNER_TYPE = ToolOwnerType.BUILTIN_MODULE
_OWNER_ID = "shinbot.agent.identity"


def register_identity_tools(
    registry: ToolRegistry,
    identity_store: IdentityStore,
    context_manager: ContextManager | None = None,
) -> None:
    """Register identity maintenance tools into the tool registry."""

    def _set_nickname(arguments: dict[str, Any], ctx: ToolExecutionContext) -> Any:
        user_id = str(arguments.get("user_id", "") or "").strip()
        nickname = str(arguments.get("nickname", "") or "").strip()
        if not user_id:
            return {"error": "user_id is required"}
        if not nickname:
            return {"error": "nickname is required"}

        aliases_raw = arguments.get("aliases", [])
        if isinstance(aliases_raw, str):
            aliases = [aliases_raw]
        elif isinstance(aliases_raw, list):
            aliases = [str(item) for item in aliases_raw]
        else:
            aliases = []

        identity = identity_store.set_nickname(
            user_id=user_id,
            nickname=nickname,
            aliases=aliases,
            locked=True,
        )
        if identity is None:
            return {"error": "nickname could not be saved"}

        cache_status = "deferred"
        if context_manager is not None and ctx.session_id:
            synced = context_manager.sync_identity_display_name(
                ctx.session_id,
                user_id=identity["user_id"],
                now_ms=int(time.time() * 1000),
            )
            cache_status = "immediate" if synced else "deferred"

        return {
            "action": "identity.set_nickname",
            "user_id": identity["user_id"],
            "nickname": identity["name"],
            "aliases": identity["aname"],
            "locked": identity["locked"],
            "cache_status": cache_status,
            "hint": (
                "称呼已保存并锁定；当前活跃别名映射已刷新。"
                if cache_status == "immediate"
                else "称呼已保存并锁定；较冷的别名映射会在下次重建时更新。"
            ),
        }

    registry.register_tool(
        ToolDefinition(
            id=f"{_OWNER_ID}.set_nickname",
            name="identity.set_nickname",
            description=(
                "为当前对话中的某个用户设置更合适、更短的称呼。\n"
                "当平台昵称过长、含噪声、或用户/上下文明确暗示更自然的叫法时使用。\n"
                "user_id 必须使用身份参考表或上下文里出现的原始用户 ID，不能填昵称。\n"
                "nickname 是之后应优先使用的称呼，建议简短自然，避免带 ID 或奇怪符号。\n"
                "aliases 可填其他可接受叫法。\n"
                "保存后该称呼会被锁定，不再被平台默认昵称自动覆盖。"
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "user_id": {
                        "type": "string",
                        "description": "要设置称呼的用户 ID，必须是原始用户 ID",
                    },
                    "nickname": {
                        "type": "string",
                        "description": "新的首选称呼，建议短且自然",
                    },
                    "aliases": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "可选的其他可接受称呼",
                    },
                    "reason": {
                        "type": "string",
                        "description": "为什么要改这个称呼，用于备注和审计",
                    },
                },
                "required": ["user_id", "nickname"],
            },
            handler=_set_nickname,
            owner_type=_OWNER_TYPE,
            owner_id=_OWNER_ID,
            visibility=ToolVisibility.PUBLIC,
            tags=["identity", "attention"],
        )
    )
