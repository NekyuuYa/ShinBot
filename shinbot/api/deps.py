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


# ── Auth dependency ──────────────────────────────────────────────────


AuthConfigDep = Annotated[object, Depends(_auth_config)]
CredentialsDep = Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)]


def require_auth(
    auth_config: AuthConfigDep,
    credentials: CredentialsDep,
) -> None:
    if credentials is None:
        raise HTTPException(
            status_code=401,
            detail={
                "code": EC.AUTH_TOKEN_MISSING,
                "message": "Authorization: Bearer <token> header required",
            },
        )
    try:
        auth_config.decode_token(credentials.credentials)
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
