"""Instance management router: /api/v1/instances"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import ok
from shinbot.core.instance_admin import (
    InstanceAdminError,
    bot_config_by_instance_id,
    control_instance_runtime,
    create_instance_runtime,
    delete_instance_runtime,
    list_instance_payloads,
    serialize_instance_record,
    update_instance_runtime,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/instances",
    tags=["instances"],
    dependencies=AuthRequired,
)


# ── Request / response schemas ───────────────────────────────────────


class CreateInstanceRequest(BaseModel):
    name: str | None = None
    adapterType: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    id: str | None = None
    platform: str | None = None

    model_config = {"extra": "allow"}


class PatchInstanceRequest(BaseModel):
    name: str | None = None
    adapterType: str | None = None
    config: dict[str, Any] | None = None

    model_config = {"extra": "allow"}


class ControlRequest(BaseModel):
    action: Literal["start", "stop"]


def _raise_admin_http_error(exc: InstanceAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_instances(bot=BotDep, boot=BootDep):
    return ok(list_instance_payloads(bot=bot, boot=boot))


@router.post("", status_code=201)
async def create_instance(body: CreateInstanceRequest, bot=BotDep, boot=BootDep):
    instance_id = body.id or body.name or body.adapterType
    platform = body.adapterType or body.platform or "satori"
    try:
        inst_entry = create_instance_runtime(
            bot=bot,
            boot=boot,
            instance_id=instance_id or "",
            platform=platform,
            name=body.name or (instance_id or ""),
            config=dict(body.config),
        )
    except InstanceAdminError as exc:
        _raise_admin_http_error(exc)
    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after create_instance: %s", e)

    return ok(
        serialize_instance_record(
            inst_entry,
            bot.adapter_manager,
            bot_config_by_instance_id(bot.database),
        )
    )


@router.patch("/{instance_id}")
async def update_instance(instance_id: str, body: PatchInstanceRequest, bot=BotDep, boot=BootDep):
    try:
        inst = update_instance_runtime(
            bot=bot,
            boot=boot,
            instance_id=instance_id,
            body=body,
        )
    except InstanceAdminError as exc:
        _raise_admin_http_error(exc)

    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after update_instance: %s", e)

    return ok(
        serialize_instance_record(
            inst,
            bot.adapter_manager,
            bot_config_by_instance_id(bot.database),
        )
    )


@router.delete("/{instance_id}")
async def delete_instance(instance_id: str, bot=BotDep, boot=BootDep):
    try:
        await delete_instance_runtime(bot=bot, boot=boot, instance_id=instance_id)
    except InstanceAdminError as exc:
        _raise_admin_http_error(exc)

    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after delete_instance: %s", e)

    return ok({"id": instance_id, "deleted": True})


@router.post("/{instance_id}/control")
async def control_instance(instance_id: str, body: ControlRequest, bot=BotDep):
    try:
        state = await control_instance_runtime(
            mgr=bot.adapter_manager,
            instance_id=instance_id,
            action=body.action,
        )
    except InstanceAdminError as exc:
        _raise_admin_http_error(exc)
    return ok({"id": instance_id, "state": state})
