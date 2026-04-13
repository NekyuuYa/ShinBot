"""FastAPI dependency injection helpers."""

from __future__ import annotations

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


# ── Auth dependency ──────────────────────────────────────────────────


def require_auth(
    auth_config=Depends(_auth_config),
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
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
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=401,
            detail={"code": EC.AUTH_TOKEN_INVALID, "message": "Invalid or malformed token"},
        )


# Shorthand Depends wrappers used in router files
AuthRequired = [Depends(require_auth)]
BotDep = Depends(_bot)
BootDep = Depends(_boot_controller)
