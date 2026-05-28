"""System control router: restart and runtime control actions."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shinbot.api.deps import (
    AuthConfigDep,
    AuthRequired,
    BootDep,
    DashboardBuildDep,
    FrameworkUpdateDep,
    RuntimeControlDep,
)
from shinbot.api.models import EC, Envelope, ok
from shinbot.core.application.runtime_control import RestartReason
from shinbot.core.application.system_update import SystemUpdateError
from shinbot.utils.logger import apply_logging_runtime_config, logging_runtime_snapshot

router = APIRouter(
    prefix="/system",
    tags=["system"],
    dependencies=AuthRequired,
)


class RestartRuntimeRequest(BaseModel):
    reason: Literal["manual", "update"] = "manual"
    requestedBy: str = ""


class UpdateLoggingRuntimeRequest(BaseModel):
    level: str | None = None
    thirdPartyNoise: str | None = None
    persist: bool = False


class RestartRequestData(BaseModel):
    reason: str
    requested_at: int
    requested_by: str
    source: str


class RuntimeStateData(BaseModel):
    restartRequested: bool
    restartRequest: RestartRequestData | None = None


class LoggingStateData(BaseModel):
    level: str
    effectiveLevel: str
    thirdPartyNoise: str
    sourceWidth: int
    availableLevels: list[str]
    availableThirdPartyNoise: list[str]
    availableColors: list[str]
    sources: list[dict[str, Any]]
    handlers: list[dict[str, Any]]


class RestartAcceptedData(BaseModel):
    accepted: bool
    restartRequested: bool
    restartRequest: RestartRequestData | None = None


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


@router.get("/runtime", response_model=Envelope[RuntimeStateData])
async def get_runtime_state(runtime_control=RuntimeControlDep):
    """Return the current runtime state including pending restart requests."""
    return ok(
        {
            "restartRequested": runtime_control.restart_requested,
            "restartRequest": runtime_control.snapshot(),
        }
    )


@router.get("/logging", response_model=Envelope[LoggingStateData])
async def get_logging_state():
    """Return the current logging runtime configuration snapshot."""
    return ok(logging_runtime_snapshot())


@router.patch("/logging", response_model=Envelope[LoggingStateData])
async def update_logging_state(body: UpdateLoggingRuntimeRequest, boot=BootDep):
    """Update the live logging configuration.

    Args:
        body: Logging configuration update request.
        boot: Application boot context.
    """
    try:
        state = apply_logging_runtime_config(
            level_name=body.level,
            third_party_noise=body.thirdPartyNoise,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": str(exc),
            },
        ) from exc

    if body.persist:
        logging_cfg = boot.config.get("logging")
        if not isinstance(logging_cfg, dict):
            logging_cfg = {}
            boot.config["logging"] = logging_cfg
        if body.level is not None:
            logging_cfg["level"] = state["level"]
        if body.thirdPartyNoise is not None:
            logging_cfg["third_party_noise"] = state["thirdPartyNoise"]
        if not boot.save_config():
            raise HTTPException(
                status_code=500,
                detail={
                    "code": EC.CONFIG_WRITE_FAILED,
                    "message": "Failed to persist logging configuration",
                },
            )

    return ok(state)


@router.get("/update", response_model=Envelope[dict[str, Any]])
async def get_update_state(
    auth_config: AuthConfigDep,
    runtime_control=RuntimeControlDep,
    framework_update=FrameworkUpdateDep,
):
    """Return the current framework update state with guard checks.

    Args:
        auth_config: Authentication configuration dependency.
        runtime_control: Runtime control service dependency.
        framework_update: Framework update service dependency.
    """
    try:
        status = await framework_update.inspect()
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


@router.post("/restart", response_model=Envelope[RestartAcceptedData])
async def request_restart(body: RestartRuntimeRequest, runtime_control=RuntimeControlDep):
    """Request a runtime restart.

    Args:
        body: Restart request with reason and requester info.
        runtime_control: Runtime control service dependency.
    """
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


@router.post("/update", response_model=Envelope[dict[str, Any]])
async def pull_update_and_restart(
    auth_config: AuthConfigDep,
    runtime_control=RuntimeControlDep,
    framework_update=FrameworkUpdateDep,
):
    """Pull a framework update and schedule a restart.

    Args:
        auth_config: Authentication configuration dependency.
        runtime_control: Runtime control service dependency.
        framework_update: Framework update service dependency.
    """
    if auth_config.is_using_default_credentials():
        raise HTTPException(
            status_code=403,
            detail={
                "code": EC.UPDATE_NOT_ALLOWED,
                "message": "Update is disabled while using default admin credentials",
            },
        )

    try:
        result = await framework_update.run_and_request_restart(
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


@router.get("/dashboard-build", response_model=Envelope[dict[str, Any]])
async def get_dashboard_build_state(
    auth_config: AuthConfigDep,
    dashboard_build=DashboardBuildDep,
):
    """Return the current dashboard build state."""
    status = await dashboard_build.inspect()

    guarded = dict(status)
    guarded["credentialsChangeRequired"] = auth_config.is_using_default_credentials()
    if auth_config.is_using_default_credentials():
        guarded["canBuild"] = False
        guarded["blockCode"] = "default_credentials"
        guarded["blockMessage"] = "Dashboard build is disabled while using default admin credentials"
    return ok(guarded)


@router.post("/dashboard-build", response_model=Envelope[dict[str, Any]])
async def build_dashboard(
    auth_config: AuthConfigDep,
    dashboard_build=DashboardBuildDep,
):
    """Trigger a dashboard build.

    Args:
        auth_config: Authentication configuration dependency.
        dashboard_build: Dashboard build service dependency.
    """
    if auth_config.is_using_default_credentials():
        raise HTTPException(
            status_code=403,
            detail={
                "code": EC.UPDATE_NOT_ALLOWED,
                "message": "Dashboard build is disabled while using default admin credentials",
            },
        )

    try:
        result = await dashboard_build.build()
    except SystemUpdateError as exc:
        code = exc.code
        if code == "UPDATE_ALREADY_RUNNING":
            code = EC.UPDATE_ALREADY_RUNNING
        elif code == "UPDATE_FAILED":
            code = EC.UPDATE_FAILED
        elif code == "UPDATE_NOT_ALLOWED":
            code = EC.UPDATE_NOT_ALLOWED
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
