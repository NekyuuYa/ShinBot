"""Auth router: POST /api/v1/auth/login"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel

from shinbot.api.auth import AuthConfig
from shinbot.api.deps import AuthConfigDep, AuthRequired, BootDep
from shinbot.api.models import EC, Envelope, ok
from shinbot.api.schemas import LoginPayload, LogoutPayload, ProfilePayload

router = APIRouter(prefix="/auth", tags=["auth"])

# --- Login rate limiting -------------------------------------------------

_MAX_FAILED_ATTEMPTS = 5
_ATTEMPT_WINDOW_SECONDS = 60.0
_BLOCK_DURATION_SECONDS = 300.0


@dataclass
class _RateLimitEntry:
    """Per-IP rate-limit state for the login endpoint."""

    fail_count: int = 0
    first_fail_time: float = 0.0
    blocked_until: float = 0.0


_rate_limits: dict[str, _RateLimitEntry] = {}


def _get_client_ip(request: Request) -> str:
    """Extract the client IP from the direct connection."""
    if request.client is not None:
        return request.client.host
    return "unknown"


def _evict_stale_entries(now: float) -> None:
    """Remove entries whose block window has fully expired to cap memory."""
    stale_ips = [
        ip
        for ip, entry in _rate_limits.items()
        if entry.blocked_until <= now
        and (now - entry.first_fail_time) > _BLOCK_DURATION_SECONDS
    ]
    for ip in stale_ips:
        del _rate_limits[ip]


def _check_rate_limit(request: Request) -> None:
    """Raise 429 if the caller's IP is currently blocked."""
    now = time.monotonic()
    _evict_stale_entries(now)

    ip = _get_client_ip(request)
    entry = _rate_limits.get(ip)
    if entry is None:
        return

    # Still inside the block window?
    if entry.blocked_until > now:
        retry_after = math.ceil(entry.blocked_until - now)
        raise HTTPException(
            status_code=429,
            detail={
                "code": "RATE_LIMITED",
                "message": "Too many failed login attempts. Try again later.",
            },
            headers={"Retry-After": str(retry_after)},
        )


def _record_failure(request: Request) -> None:
    """Record a failed login attempt; block IP after exceeding the threshold."""
    now = time.monotonic()
    ip = _get_client_ip(request)
    entry = _rate_limits.get(ip)

    if entry is None:
        _rate_limits[ip] = _RateLimitEntry(fail_count=1, first_fail_time=now)
        return

    # Reset the window if it has expired
    if (now - entry.first_fail_time) > _ATTEMPT_WINDOW_SECONDS:
        entry.fail_count = 1
        entry.first_fail_time = now
        entry.blocked_until = 0.0
        return

    entry.fail_count += 1
    if entry.fail_count >= _MAX_FAILED_ATTEMPTS:
        entry.blocked_until = now + _BLOCK_DURATION_SECONDS


def _clear_rate_limit(request: Request) -> None:
    """Clear rate-limit state on successful login."""
    ip = _get_client_ip(request)
    _rate_limits.pop(ip, None)


class LoginRequest(BaseModel):
    username: str
    password: str


class UpdateProfileRequest(BaseModel):
    username: str
    current_password: str
    new_password: str


def _login_payload(auth_config: AuthConfig, subject: str | None = None) -> dict[str, Any]:
    return {
        "expires_in_hours": auth_config.jwt_expire_hours,
        "username": subject or auth_config.username,
        "must_change_credentials": auth_config.is_using_default_credentials(),
    }


def _set_session_cookie(
    response: Response,
    auth_config: AuthConfig,
    token: str,
    request: Request,
) -> None:
    response.set_cookie(
        key=auth_config.session_cookie_name,
        value=token,
        max_age=auth_config.session_cookie_max_age,
        httponly=True,
        secure=auth_config.is_secure_cookie(request.url.scheme),
        samesite=auth_config.session_cookie_samesite,
        path=auth_config.session_cookie_path,
        domain=auth_config.session_cookie_domain,
    )


def _clear_session_cookie(response: Response, auth_config: AuthConfig) -> None:
    response.delete_cookie(
        key=auth_config.session_cookie_name,
        path=auth_config.session_cookie_path,
        domain=auth_config.session_cookie_domain,
    )


@router.get("/login", include_in_schema=False)
async def login_method_not_allowed():
    """Explicitly surface wrong-method access as 405 for /auth/login."""
    raise HTTPException(
        status_code=405,
        detail={
            "code": "METHOD_NOT_ALLOWED",
            "message": "Use POST /api/v1/auth/login",
        },
    )


@router.post("/login", response_model=Envelope[LoginPayload])
async def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    auth_config: AuthConfigDep,
):
    """Exchange credentials for an authenticated session cookie."""
    _check_rate_limit(request)

    if not auth_config.verify_password(body.username, body.password):
        _record_failure(request)
        raise HTTPException(
            status_code=401,
            detail={
                "code": EC.AUTH_CREDENTIALS_INVALID,
                "message": "Invalid username or password",
            },
        )

    _clear_rate_limit(request)
    token = auth_config.create_token(subject=body.username)
    _set_session_cookie(response, auth_config, token, request)
    return ok(_login_payload(auth_config, subject=body.username))


@router.post("/logout", response_model=Envelope[LogoutPayload])
async def logout(response: Response, auth_config: AuthConfigDep):
    """Clear the session cookie to log the user out."""
    _clear_session_cookie(response, auth_config)
    return ok({"logged_out": True})


@router.get("/profile", dependencies=AuthRequired, response_model=Envelope[ProfilePayload])
async def get_profile(auth_config: AuthConfigDep):
    """Return the current authenticated user's profile."""
    return ok(
        {
            "username": auth_config.username,
            "must_change_credentials": auth_config.is_using_default_credentials(),
        }
    )


@router.patch("/profile", dependencies=AuthRequired, response_model=Envelope[LoginPayload])
async def update_profile(
    body: UpdateProfileRequest,
    request: Request,
    response: Response,
    auth_config: AuthConfigDep,
    boot=BootDep,
):
    """Update username and/or password, re-issue session cookie."""
    username = body.username.strip()
    if not username:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Username cannot be empty",
            },
        )

    new_password = body.new_password.strip()
    if not new_password:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "New password cannot be empty",
            },
        )

    if not auth_config.verify_password(auth_config.username, body.current_password):
        raise HTTPException(
            status_code=401,
            detail={
                "code": EC.AUTH_CREDENTIALS_INVALID,
                "message": "Current password is incorrect",
            },
        )

    if username == auth_config.username and new_password == body.current_password:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Please change both username and password",
            },
        )

    if auth_config.is_using_default_credentials() and (
        username == AuthConfig.DEFAULT_USERNAME or new_password == AuthConfig.DEFAULT_PASSWORD
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Default credentials are not allowed. Please change both username and password",
            },
        )

    admin_cfg = boot.config.setdefault("admin", {})
    previous_username = admin_cfg.get("username", auth_config.username)
    previous_password = admin_cfg.get("password", "")
    admin_cfg["username"] = username
    admin_cfg["password"] = new_password

    saved = False
    try:
        saved = bool(boot.save_config())
    except Exception:
        saved = False

    if not saved:
        admin_cfg["username"] = previous_username
        admin_cfg["password"] = previous_password
        raise HTTPException(
            status_code=500,
            detail={
                "code": EC.CONFIG_WRITE_FAILED,
                "message": "Failed to persist admin credentials",
            },
        )

    auth_config.set_credentials(username=username, password=new_password)
    token = auth_config.create_token(subject=username)
    _set_session_cookie(response, auth_config, token, request)
    return ok(_login_payload(auth_config, subject=username))
