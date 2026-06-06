"""Helpers for removing deprecated bot-scoped default permission bindings."""

from __future__ import annotations

from collections.abc import Iterable

from shinbot.core.application.bots_config import BotServiceConfig
from shinbot.core.security.permission import PermissionEngine

_DEPRECATED_BOT_ADMIN_BINDING_PREFIX = "__bot_admin__:"


def apply_bot_admin_bindings(
    permission_engine: PermissionEngine,
    _bots: Iterable[BotServiceConfig],
) -> None:
    """Remove deprecated bot-scoped admin bindings from the runtime engine."""

    for key in list(permission_engine.binding_keys()):
        if key.startswith(_DEPRECATED_BOT_ADMIN_BINDING_PREFIX):
            permission_engine.unbind(key)
