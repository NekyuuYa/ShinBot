"""Instance management router: /api/v1/instances"""

from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

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
    id: str
    platform: str = "satori"
    satori: SatoriCfg = SatoriCfg()


class PatchInstanceRequest(BaseModel):
    satori: SatoriCfg | None = None


class ControlRequest(BaseModel):
    action: Literal["start", "stop"]


# ── Helpers ──────────────────────────────────────────────────────────


def _instance_dict(adapter: Any, mgr: Any) -> dict:
    return {
        "id": adapter.instance_id,
        "platform": adapter.platform,
        "running": mgr.is_running(adapter.instance_id),
    }


# ── Routes ───────────────────────────────────────────────────────────


@router.get("")
async def list_instances(bot=BotDep):
    mgr = bot.adapter_manager
    return ok([_instance_dict(a, mgr) for a in mgr.all_instances])


@router.post("", status_code=201)
async def create_instance(body: CreateInstanceRequest, bot=BotDep, boot=BootDep):
    if bot.adapter_manager.get_instance(body.id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.INSTANCE_ALREADY_EXISTS,
                "message": f"Instance {body.id!r} already exists",
            },
        )
    if body.platform not in bot.adapter_manager.registered_platforms:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.UNSUPPORTED_PLATFORM,
                "message": f"Platform {body.platform!r} is not registered",
            },
        )

    bot.add_adapter(
        instance_id=body.id,
        platform=body.platform,
        host=body.satori.host,
        token=body.satori.token,
        reconnect_delay=body.satori.reconnect_delay,
    )

    # Persist to config
    inst_entry: dict = {
        "id": body.id,
        "platform": body.platform,
        "satori": {
            "host": body.satori.host,
            "token": body.satori.token,
            "reconnect_delay": body.satori.reconnect_delay,
        },
    }
    boot.config.setdefault("instances", []).append(inst_entry)
    try:
        boot.save_config()
    except Exception as e:
        logger.warning("Failed to persist config after create_instance: %s", e)

    return ok({"id": body.id, "platform": body.platform, "running": False})


@router.patch("/{instance_id}")
async def update_instance(instance_id: str, body: PatchInstanceRequest, bot=BotDep, boot=BootDep):
    adapter = bot.adapter_manager.get_instance(instance_id)
    if adapter is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.INSTANCE_NOT_FOUND, "message": f"Instance {instance_id!r} not found"},
        )

    # Apply to in-memory config dict
    for inst in boot.config.get("instances", []):
        if inst.get("id") == instance_id:
            if body.satori is not None:
                satori_section = inst.setdefault("satori", {})
                patch = body.satori.model_dump(exclude_none=True)
                satori_section.update(patch)
            break

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
            detail={"code": EC.INSTANCE_NOT_FOUND, "message": f"Instance {instance_id!r} not found"},
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
