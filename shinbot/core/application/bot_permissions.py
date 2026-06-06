"""Helpers for applying bot-scoped default permission bindings."""

from __future__ import annotations

from collections.abc import Iterable

from shinbot.core.application.bots_config import BotServiceConfig
from shinbot.core.security.permission import BOT_ADMIN_BINDING_PREFIX, PermissionEngine


def apply_bot_admin_bindings(
    permission_engine: PermissionEngine,
    bots: Iterable[BotServiceConfig],
) -> None:
    """Refresh bot-scoped default admin bindings from bot configs."""

    for key in list(permission_engine.binding_keys()):
        if key.startswith(BOT_ADMIN_BINDING_PREFIX):
            permission_engine.unbind(key)

    for bot in bots:
        for admin_id in bot.administrators:
            normalized = str(admin_id).strip()
            if not normalized:
                continue
            permission_engine.bind(
                f"{BOT_ADMIN_BINDING_PREFIX}{bot.id}:{normalized}",
                "admin",
            )
