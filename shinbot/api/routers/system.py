"""System control router: restart and runtime control actions."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shinbot.api.deps import AuthRequired, RuntimeControlDep
from shinbot.api.models import EC, ok
from shinbot.core.application.runtime_control import RestartReason

router = APIRouter(
    prefix="/system",
    tags=["system"],
    dependencies=AuthRequired,
)


class RestartRuntimeRequest(BaseModel):
    reason: Literal["manual", "update"] = "manual"
    requestedBy: str = ""


@router.get("/runtime")
async def get_runtime_state(runtime_control=RuntimeControlDep):
    return ok(
        {
            "restartRequested": runtime_control.restart_requested,
            "restartRequest": runtime_control.snapshot(),
        }
    )


@router.post("/restart")
async def request_restart(body: RestartRuntimeRequest, runtime_control=RuntimeControlDep):
    try:
        request = runtime_control.request_restart(
            reason=RestartReason(body.reason),
            requested_by=body.requestedBy,
            source="api.system.restart",
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.RESTART_ALREADY_REQUESTED,
                "message": str(exc),
            },
        ) from exc

    return ok(
        {
            "accepted": True,
            "restartRequested": True,
            "restartRequest": request.to_payload(),
        }
    )
