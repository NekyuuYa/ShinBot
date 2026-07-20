"""FastAPI dependency injection helpers."""

from __future__ import annotations

from typing import Annotated, Any

import jwt
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from shinbot.api.auth import AuthConfig
from shinbot.api.models import EC

_bearer = HTTPBearer(auto_error=False)


# ── State accessors ──────────────────────────────────────────────────


async def _auth_config(request: Request) -> AuthConfig:
    return request.app.state.auth_config


async def _bot(request: Request) -> Any:
    return request.app.state.bot


async def _boot_controller(request: Request) -> Any:
    return request.app.state.boot_controller


async def _runtime_control(request: Request) -> Any:
    return request.app.state.runtime_control


async def _dashboard_build_service(request: Request) -> Any:
    return request.app.state.dashboard_build_service


async def _framework_update_service(request: Request) -> Any:
    return request.app.state.framework_update_service


# ── Auth dependency ──────────────────────────────────────────────────


AuthConfigDep = Annotated[AuthConfig, Depends(_auth_config)]
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
    """Verify the caller carries a valid JWT session.

    Resolves the token from the session cookie first, then falls back to
    the ``Authorization: Bearer`` header.  Raises ``HTTPException`` with
    the appropriate error code if the token is missing, expired, or
    malformed.
    """
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
        claims = auth_config.decode_token(token)
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
    normalized_subject = ""
    subject = claims.get("sub")
    if isinstance(subject, str):
        normalized_subject = subject.strip()
    if not normalized_subject:
        username = claims.get("username")
        normalized_subject = username.strip() if isinstance(username, str) else ""
    request.state.auth_subject = normalized_subject or auth_config.username


# Shorthand Depends wrappers used in router files
AuthRequired = [Depends(require_auth)]
BotDep = Depends(_bot)
BootDep = Depends(_boot_controller)
RuntimeControlDep = Depends(_runtime_control)
DashboardBuildDep = Depends(_dashboard_build_service)
FrameworkUpdateDep = Depends(_framework_update_service)
