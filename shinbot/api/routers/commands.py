"""Command management router: /api/v1/commands"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shinbot.admin.command_admin import (
    CommandAdminError,
    command_dict,
    set_command_enabled_or_raise,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

router = APIRouter(
    prefix="/commands",
    tags=["commands"],
    dependencies=AuthRequired,
)


class CommandData(BaseModel):
    name: str
    aliases: list[str]
    triggers: list[str]
    description: str
    usage: str
    permission: str
    mode: str
    priority: int
    priorityLabel: str
    pattern: str
    owner: str
    enabled: bool


class CommandEnabledPayload(BaseModel):
    enabled: bool


def _raise_admin_http_error(exc: CommandAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("", response_model=Envelope[list[CommandData]])
async def list_commands(bot=BotDep):
    """List all registered runtime commands for dashboard management."""
    registry = getattr(bot, "command_registry", None)
    if registry is None:
        return ok([])
    commands = sorted(registry.all_commands, key=lambda item: item.name)
    return ok([command_dict(item) for item in commands])


@router.patch("/{command_name}", response_model=Envelope[CommandData])
async def update_command(
    command_name: str,
    payload: CommandEnabledPayload,
    bot=BotDep,
    boot=BootDep,
):
    """Update command runtime management state."""
    registry = getattr(bot, "command_registry", None)
    if registry is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "COMMAND_NOT_FOUND", "message": f"Command {command_name!r} not found"},
        )
    try:
        return ok(
            set_command_enabled_or_raise(
                registry,
                boot,
                command_name,
                enabled=payload.enabled,
            )
        )
    except CommandAdminError as exc:
        _raise_admin_http_error(exc)
