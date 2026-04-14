"""Instance management router: /api/v1/instances"""

from __future__ import annotations

import logging
import time
from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, ok

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/instances",
    tags=["instances"],
    dependencies=AuthRequired,
)


# ── Request / response schemas ───────────────────────────────────────


class SatoriCfg(BaseModel):
    host: str = "localhost:5140"
    token: str = ""
    reconnect_delay: float = 5.0


class CreateInstanceRequest(BaseModel):
    name: str | None = None
    adapterType: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    id: str | None = None
    platform: str | None = None
    satori: SatoriCfg | None = None

    model_config = {"extra": "allow"}


class PatchInstanceRequest(BaseModel):
    name: str | None = None
    adapterType: str | None = None
    config: dict[str, Any] | None = None
    satori: SatoriCfg | None = None

    model_config = {"extra": "allow"}


class ControlRequest(BaseModel):
    action: Literal["start", "stop"]


# ── Helpers ──────────────────────────────────────────────────────────


def _instance_dict(adapter: Any, mgr: Any) -> dict:
    return {
        "id": adapter.instance_id,
        "name": getattr(adapter, "instance_id", adapter.instance_id),
        "platform": adapter.platform,
        "running": mgr.is_running(adapter.instance_id),
    }


def _persist_instance_record(boot: Any, record: dict[str, Any]) -> None:
    instances = boot.config.setdefault("instances", [])
    for index, item in enumerate(instances):
        if item.get("id") == record["id"]:
            instances[index] = record
            break
    else:
        instances.append(record)


def _resolve_config(body: CreateInstanceRequest | PatchInstanceRequest) -> dict[str, Any]:
    if body.config:
        return dict(body.config)
    if body.satori is not None:
        return body.satori.model_dump(exclude_none=True)
    return {}


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_instances(bot=BotDep, boot=BootDep):
    mgr = bot.adapter_manager
    runtime = {a.instance_id: a for a in mgr.all_instances}
    records: list[dict[str, Any]] = []

    for item in boot.config.get("instances", []):
        instance_id = item.get("id")
        adapter = runtime.get(instance_id)
        status = "running" if adapter is not None and mgr.is_running(instance_id) else "stopped"
        records.append(
            {
                "id": instance_id,
                "name": item.get("name", instance_id),
                "adapterType": item.get("adapterType") or item.get("platform", "satori"),
                "status": status,
                "config": item.get("config") or item.get("satori", {}),
                "createdAt": item.get("createdAt", 0),
                "lastModified": item.get("lastModified", item.get("createdAt", 0)),
            }
        )

    # Include any runtime adapters not yet persisted in config.
    seen_ids = {item["id"] for item in records}
    for adapter in mgr.all_instances:
        if adapter.instance_id in seen_ids:
            continue
        records.append(
            {
                "id": adapter.instance_id,
                "name": adapter.instance_id,
                "adapterType": adapter.platform,
                "status": "running" if mgr.is_running(adapter.instance_id) else "stopped",
                "config": getattr(adapter, "config", {}).model_dump()
                if hasattr(getattr(adapter, "config", None), "model_dump")
                else {},
                "createdAt": 0,
                "lastModified": 0,
            }
        )

    return ok(records)


@router.post("", status_code=201)
async def create_instance(body: CreateInstanceRequest, bot=BotDep, boot=BootDep):
    instance_id = body.id or body.name or body.adapterType
    platform = body.adapterType or body.platform or "satori"
    if not instance_id:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Instance id/name is required"},
        )

    if bot.adapter_manager.get_instance(instance_id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.INSTANCE_ALREADY_EXISTS,
                "message": f"Instance {instance_id!r} already exists",
            },
        )
    if platform not in bot.adapter_manager.registered_platforms:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.UNSUPPORTED_PLATFORM,
                "message": f"Platform {platform!r} is not registered",
            },
        )

    config_kwargs = _resolve_config(body)

    bot.add_adapter(
        instance_id=instance_id,
        platform=platform,
        **config_kwargs,
    )

    # Persist to config
    inst_entry: dict = {
        "id": instance_id,
        "name": body.name or instance_id,
        "adapterType": platform,
        "platform": platform,
        "config": config_kwargs,
        "createdAt": int(time.time()),
        "lastModified": int(time.time()),
    }
    _persist_instance_record(boot, inst_entry)
    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after create_instance: %s", e)

    return ok(
        {
            "id": instance_id,
            "name": body.name or instance_id,
            "adapterType": platform,
            "status": "stopped",
            "config": config_kwargs,
            "createdAt": inst_entry["createdAt"],
            "lastModified": inst_entry["lastModified"],
        }
    )


@router.patch("/{instance_id}")
async def update_instance(instance_id: str, body: PatchInstanceRequest, bot=BotDep, boot=BootDep):
    adapter = bot.adapter_manager.get_instance(instance_id)
    if adapter is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.INSTANCE_NOT_FOUND,
                "message": f"Instance {instance_id!r} not found",
            },
        )

    # Apply to in-memory config dict
    for inst in boot.config.get("instances", []):
        if inst.get("id") == instance_id:
            if body.name is not None:
                inst["name"] = body.name
            if body.adapterType is not None:
                inst["adapterType"] = body.adapterType
                inst["platform"] = body.adapterType

            config_patch = _resolve_config(body)
            if config_patch:
                inst_config = inst.setdefault("config", {})
                inst_config.update(config_patch)
                if body.satori is not None and not body.config:
                    inst["satori"] = body.satori.model_dump(exclude_none=True)

            if body.satori is not None:
                satori_section = inst.setdefault("satori", {})
                patch = body.satori.model_dump(exclude_none=True)
                satori_section.update(patch)
            break

    if adapter is not None and hasattr(adapter, "config") and hasattr(adapter.config, "model_copy"):
        config_patch = _resolve_config(body)
        if config_patch:
            adapter.config = adapter.config.model_copy(update=config_patch)

    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after update_instance: %s", e)

    return ok({"id": instance_id, "updated": True})


@router.post("/{instance_id}/control")
async def control_instance(instance_id: str, body: ControlRequest, bot=BotDep):
    mgr = bot.adapter_manager
    adapter = mgr.get_instance(instance_id)
    if adapter is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.INSTANCE_NOT_FOUND,
                "message": f"Instance {instance_id!r} not found",
            },
        )

    if body.action == "start":
        if mgr.is_running(instance_id):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": EC.INSTANCE_ALREADY_RUNNING,
                    "message": f"Instance {instance_id!r} is already running",
                },
            )
        await mgr.start_instance(instance_id)
        return ok({"id": instance_id, "state": "running"})

    else:  # stop
        if not mgr.is_running(instance_id):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": EC.INSTANCE_NOT_RUNNING,
                    "message": f"Instance {instance_id!r} is not running",
                },
            )
        await mgr.stop_instance(instance_id)
        return ok({"id": instance_id, "state": "stopped"})
