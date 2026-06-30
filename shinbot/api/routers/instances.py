"""Instance management router: /api/v1/instances"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.instance_admin import (
    InstanceAdminError,
    control_instance_runtime,
    create_instance_runtime,
    delete_instance_runtime,
    instance_config_by_instance_id,
    list_instance_payloads,
    serialize_instance_record,
    update_instance_runtime,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/instances",
    tags=["instances"],
    dependencies=AuthRequired,
)


# ── Request / response schemas ───────────────────────────────────────


class CreateInstanceRequest(BaseModel):
    name: str | None = None
    adapter: str | None = None
    adapterType: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    id: str | None = None

    model_config = {"extra": "allow"}


class PatchInstanceRequest(BaseModel):
    name: str | None = None
    adapter: str | None = None
    adapterType: str | None = None
    config: dict[str, Any] | None = None

    model_config = {"extra": "allow"}


class ControlRequest(BaseModel):
    action: Literal["start", "stop"]


class InstanceConfigSummaryData(BaseModel):
    """Nested config summary within an instance response."""

    uuid: str
    mainLlm: str
    explicitPromptCacheEnabled: bool
    mediaInspectionLlm: str | None = None
    mediaInspectionPrompt: str | None = None
    stickerSummaryLlm: str | None = None
    stickerSummaryPrompt: str | None = None
    contextCompressionLlm: str | None = None
    maxContextTokens: int | None = None
    contextEvictRatio: float | None = None
    contextCompressionMaxChars: int | None = None
    responseProfile: str | None = None
    responseProfilePrivate: str | None = None
    responseProfilePriority: str | None = None
    responseProfileGroup: str | None = None
    tags: list[str]


class InstanceData(BaseModel):
    """Response data model for a single instance."""

    id: str
    name: str
    adapter: str
    status: str
    running: bool = False
    connected: bool = False
    available: bool = False
    config: dict[str, Any]
    instanceConfig: InstanceConfigSummaryData | None = None
    createdAt: Any
    lastModified: Any


class InstanceDeletedData(BaseModel):
    """Response data model for instance deletion confirmation."""

    id: str
    deleted: bool


class InstanceControlData(BaseModel):
    """Response data model for instance start/stop control."""

    id: str
    state: str


def _raise_admin_http_error(exc: InstanceAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


# ── Routes ───────────────────────────────────────────────────────────


@router.get("", response_model=Envelope[list[InstanceData]])
async def list_instances(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """List all registered bot instances with their status."""
    return ok(list_instance_payloads(bot=bot, boot=boot))


@router.post("", status_code=201, response_model=Envelope[InstanceData])
async def create_instance(body: CreateInstanceRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Create a new adapter instance and persist the configuration."""
    adapter = body.adapter or body.adapterType or ""
    instance_id = body.id or body.name or adapter
    try:
        inst_entry = create_instance_runtime(
            bot=bot,
            boot=boot,
            instance_id=instance_id or "",
            platform=adapter,
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
            instance_config_by_instance_id(bot.database),
        )
    )


@router.patch("/{instance_id}", response_model=Envelope[InstanceData])
async def update_instance(instance_id: str, body: PatchInstanceRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Update an existing adapter instance configuration."""
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
            instance_config_by_instance_id(bot.database),
        )
    )


@router.delete("/{instance_id}", response_model=Envelope[InstanceDeletedData])
async def delete_instance(instance_id: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Delete a bot instance and remove it from the configuration."""
    try:
        await delete_instance_runtime(bot=bot, boot=boot, instance_id=instance_id)
    except InstanceAdminError as exc:
        _raise_admin_http_error(exc)

    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after delete_instance: %s", e)

    return ok({"id": instance_id, "deleted": True})


@router.post("/{instance_id}/control", response_model=Envelope[InstanceControlData])
async def control_instance(instance_id: str, body: ControlRequest, bot: Any = BotDep) -> dict[str, Any]:
    """Start or stop a bot instance at runtime."""
    try:
        state = await control_instance_runtime(
            mgr=bot.adapter_manager,
            instance_id=instance_id,
            action=body.action,
        )
    except InstanceAdminError as exc:
        _raise_admin_http_error(exc)
    return ok({"id": instance_id, "state": state})
