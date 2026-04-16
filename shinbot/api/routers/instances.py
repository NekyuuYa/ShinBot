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


def _persist_instance_record(boot: Any, record: dict[str, Any]) -> None:
    instances = boot.config.setdefault("instances", [])
    for index, item in enumerate(instances):
        if item.get("id") == record["id"]:
            instances[index] = record
            break
    else:
        instances.append(record)


def _find_instance_record(
    boot: Any, instance_id: str
) -> tuple[int, dict[str, Any]] | tuple[None, None]:
    for index, item in enumerate(boot.config.get("instances", [])):
        if item.get("id") == instance_id:
            return index, item
    return None, None


def _resolve_config(body: CreateInstanceRequest | PatchInstanceRequest) -> dict[str, Any]:
    if body.config:
        return dict(body.config)
    if body.satori is not None:
        return body.satori.model_dump(exclude_none=True)
    return {}


def _runtime_config(adapter: Any) -> dict[str, Any]:
    config = getattr(adapter, "config", None)
    if hasattr(config, "model_dump"):
        return config.model_dump()
    if isinstance(config, dict):
        return dict(config)
    return {}


def _serialize_bot_config_summary(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    return {
        "uuid": payload["uuid"],
        "defaultAgentUuid": payload["default_agent_uuid"],
        "mainLlm": payload["main_llm"],
        "tags": payload["tags"],
    }


def _serialize_instance_record(
    item: dict[str, Any],
    mgr: Any,
    bot_config_by_instance_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    instance_id = item.get("id")
    adapter = mgr.get_instance(instance_id) if instance_id else None
    status = (
        "running"
        if instance_id and adapter is not None and mgr.is_running(instance_id)
        else "stopped"
    )
    config = item.get("config") or item.get("satori", {})
    if not config and adapter is not None:
        config = _runtime_config(adapter)

    return {
        "id": instance_id,
        "name": item.get("name", instance_id),
        "adapterType": item.get("adapterType")
        or item.get("platform", getattr(adapter, "platform", "satori")),
        "status": status,
        "config": config,
        "botConfig": _serialize_bot_config_summary(bot_config_by_instance_id.get(str(instance_id))),
        "createdAt": item.get("createdAt", 0),
        "lastModified": item.get("lastModified", item.get("createdAt", 0)),
    }


def _serialize_runtime_instance(
    adapter: Any,
    mgr: Any,
    bot_config_by_instance_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "id": adapter.instance_id,
        "name": adapter.instance_id,
        "adapterType": adapter.platform,
        "status": "running" if mgr.is_running(adapter.instance_id) else "stopped",
        "config": _runtime_config(adapter),
        "botConfig": _serialize_bot_config_summary(
            bot_config_by_instance_id.get(adapter.instance_id)
        ),
        "createdAt": 0,
        "lastModified": 0,
    }


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_instances(bot=BotDep, boot=BootDep):
    mgr = bot.adapter_manager
    bot_config_by_instance_id = {
        item["instance_id"]: item for item in bot.database.bot_configs.list()
    }
    records: list[dict[str, Any]] = []

    for item in boot.config.get("instances", []):
        records.append(_serialize_instance_record(item, mgr, bot_config_by_instance_id))

    # Include any runtime adapters not yet persisted in config.
    seen_ids = {item["id"] for item in records}
    for adapter in mgr.all_instances:
        if adapter.instance_id in seen_ids:
            continue
        records.append(_serialize_runtime_instance(adapter, mgr, bot_config_by_instance_id))

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

    if (
        bot.adapter_manager.get_instance(instance_id) is not None
        or _find_instance_record(boot, instance_id)[1] is not None
    ):
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
    now = int(time.time())
    inst_entry: dict = {
        "id": instance_id,
        "name": body.name or instance_id,
        "adapterType": platform,
        "platform": platform,
        "config": config_kwargs,
        "createdAt": now,
        "lastModified": now,
    }
    _persist_instance_record(boot, inst_entry)
    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after create_instance: %s", e)

    return ok(
        _serialize_instance_record(
            inst_entry,
            bot.adapter_manager,
            {
                item["instance_id"]: item
                for item in bot.database.bot_configs.list()
            },
        )
    )


@router.patch("/{instance_id}")
async def update_instance(instance_id: str, body: PatchInstanceRequest, bot=BotDep, boot=BootDep):
    adapter = bot.adapter_manager.get_instance(instance_id)
    index, inst = _find_instance_record(boot, instance_id)
    if adapter is None and inst is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.INSTANCE_NOT_FOUND,
                "message": f"Instance {instance_id!r} not found",
            },
        )

    if inst is None and adapter is not None:
        inst = {
            "id": instance_id,
            "name": instance_id,
            "adapterType": adapter.platform,
            "platform": adapter.platform,
            "config": _runtime_config(adapter),
            "createdAt": 0,
            "lastModified": 0,
        }
        boot.config.setdefault("instances", []).append(inst)
        index = len(boot.config["instances"]) - 1

    assert inst is not None
    assert index is not None

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

    inst["lastModified"] = int(time.time())
    boot.config["instances"][index] = inst

    if adapter is not None and hasattr(adapter, "config") and hasattr(adapter.config, "model_copy"):
        if config_patch:
            adapter.config = adapter.config.model_copy(update=config_patch)

    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after update_instance: %s", e)

    return ok(
        _serialize_instance_record(
            inst,
            bot.adapter_manager,
            {
                item["instance_id"]: item
                for item in bot.database.bot_configs.list()
            },
        )
    )


@router.delete("/{instance_id}")
async def delete_instance(instance_id: str, bot=BotDep, boot=BootDep):
    mgr = bot.adapter_manager
    adapter = mgr.get_instance(instance_id)
    index, _inst = _find_instance_record(boot, instance_id)

    if adapter is None and index is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.INSTANCE_NOT_FOUND,
                "message": f"Instance {instance_id!r} not found",
            },
        )

    if adapter is not None:
        await mgr.delete_instance(instance_id)

    if index is not None:
        del boot.config["instances"][index]

    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after delete_instance: %s", e)

    return ok({"id": instance_id, "deleted": True})


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
