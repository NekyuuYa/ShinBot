"""System control router: restart and runtime control actions."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shinbot.api.deps import AuthConfigDep, AuthRequired, RuntimeControlDep, SystemUpdateDep
from shinbot.api.models import EC, ok
from shinbot.core.application.runtime_control import RestartReason
from shinbot.core.application.system_update import SystemUpdateError

router = APIRouter(
    prefix="/system",
    tags=["system"],
    dependencies=AuthRequired,
)


class RestartRuntimeRequest(BaseModel):
    reason: Literal["manual", "update"] = "manual"
    requestedBy: str = ""


def _apply_update_guards(
    status: dict[str, object],
    *,
    credentials_change_required: bool,
    restart_request: dict[str, object] | None,
) -> dict[str, object]:
    guarded = dict(status)
    guarded["credentialsChangeRequired"] = credentials_change_required
    guarded["restartRequested"] = restart_request is not None
    guarded["restartRequest"] = restart_request

    if credentials_change_required:
        guarded["canUpdate"] = False
        guarded["blockCode"] = "default_credentials"
        guarded["blockMessage"] = "Update is disabled while using default admin credentials"
        return guarded

    if restart_request is not None and guarded.get("canUpdate"):
        guarded["canUpdate"] = False
        guarded["blockCode"] = "restart_pending"
        guarded["blockMessage"] = "A restart request is already pending"

    return guarded


@router.get("/runtime")
async def get_runtime_state(runtime_control=RuntimeControlDep):
    return ok(
        {
            "restartRequested": runtime_control.restart_requested,
            "restartRequest": runtime_control.snapshot(),
        }
    )


@router.get("/update")
async def get_update_state(
    auth_config: AuthConfigDep,
    runtime_control=RuntimeControlDep,
    system_update=SystemUpdateDep,
):
    try:
        status = await system_update.inspect()
    except SystemUpdateError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={
                "code": EC.UPDATE_FAILED,
                "message": exc.message,
            },
        ) from exc
    restart_request = runtime_control.snapshot()
    guarded = _apply_update_guards(
        status,
        credentials_change_required=auth_config.is_using_default_credentials(),
        restart_request=restart_request,
    )
    return ok(guarded)


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


@router.post("/update")
async def pull_update_and_restart(
    auth_config: AuthConfigDep,
    runtime_control=RuntimeControlDep,
    system_update=SystemUpdateDep,
):
    if auth_config.is_using_default_credentials():
        raise HTTPException(
            status_code=403,
            detail={
                "code": EC.UPDATE_NOT_ALLOWED,
                "message": "Update is disabled while using default admin credentials",
            },
        )

    try:
        result = await system_update.pull_and_request_restart(
            runtime_control=runtime_control,
            requested_by=auth_config.username,
        )
    except SystemUpdateError as exc:
        code = exc.code
        if code == "UPDATE_ALREADY_RUNNING":
            code = EC.UPDATE_ALREADY_RUNNING
        elif code == "UPDATE_FAILED":
            code = EC.UPDATE_FAILED
        elif code in {"UPDATE_NOT_ALLOWED", "RESTART_ALREADY_REQUESTED"}:
            code = EC.RESTART_ALREADY_REQUESTED if code == "RESTART_ALREADY_REQUESTED" else EC.UPDATE_NOT_ALLOWED
        message = exc.message
        if exc.output:
            message = f"{message}: {exc.output}"
        raise HTTPException(
            status_code=exc.status_code,
            detail={
                "code": code,
                "message": message,
            },
        ) from exc

    return ok(result)
