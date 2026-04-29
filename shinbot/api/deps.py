"""FastAPI dependency injection helpers."""

from __future__ import annotations

from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from shinbot.api.models import EC

_bearer = HTTPBearer(auto_error=False)


# ── State accessors ──────────────────────────────────────────────────


async def _auth_config(request: Request):
    return request.app.state.auth_config


async def _bot(request: Request):
    return request.app.state.bot


async def _boot_controller(request: Request):
    return request.app.state.boot_controller


async def _runtime_control(request: Request):
    return request.app.state.runtime_control


async def _system_update_service(request: Request):
    return request.app.state.system_update_service


async def _dashboard_dist_update_service(request: Request):
    return request.app.state.dashboard_dist_update_service


# ── Auth dependency ──────────────────────────────────────────────────


AuthConfigDep = Annotated[object, Depends(_auth_config)]
CredentialsDep = Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)]


def _resolve_auth_token(
    request: Request,
    auth_config: AuthConfigDep,
    credentials: CredentialsDep,
) -> str | None:
    cookie_token = request.cookies.get(auth_config.session_cookie_name)
    if cookie_token:
        return cookie_token

    if credentials is None:
        return None

    token = credentials.credentials.strip()
    return token or None


def require_auth(
    request: Request,
    auth_config: AuthConfigDep,
    credentials: CredentialsDep,
) -> None:
    token = _resolve_auth_token(request, auth_config, credentials)
    if token is None:
        raise HTTPException(
            status_code=401,
            detail={
                "code": EC.AUTH_TOKEN_MISSING,
                "message": "Authentication cookie or Authorization: Bearer <token> header required",
            },
        )
    try:
        auth_config.decode_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=401,
            detail={"code": EC.AUTH_TOKEN_EXPIRED, "message": "Token has expired"},
        ) from None
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail={"code": EC.AUTH_TOKEN_INVALID, "message": "Invalid or malformed token"},
        ) from None


# Shorthand Depends wrappers used in router files
AuthRequired = [Depends(require_auth)]
BotDep = Depends(_bot)
BootDep = Depends(_boot_controller)
RuntimeControlDep = Depends(_runtime_control)
SystemUpdateDep = Depends(_system_update_service)
DashboardDistUpdateDep = Depends(_dashboard_dist_update_service)
