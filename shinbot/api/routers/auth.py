"""Auth router: POST /api/v1/auth/login"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from shinbot.api.auth import AuthConfig
from shinbot.api.deps import AuthRequired, BootDep, _auth_config
from shinbot.api.models import EC, ok

router = APIRouter(prefix="/auth", tags=["auth"])
AuthConfigDep = Annotated[object, Depends(_auth_config)]


class LoginRequest(BaseModel):
    username: str
    password: str


class UpdateProfileRequest(BaseModel):
    username: str
    current_password: str
    new_password: str


def _login_payload(auth_config: AuthConfig, subject: str | None = None) -> dict:
    token = auth_config.create_token(subject=subject)
    return {
        "token": token,
        "token_type": "Bearer",
        "expires_in_hours": auth_config.jwt_expire_hours,
        "username": auth_config.username,
        "must_change_credentials": auth_config.is_using_default_credentials(),
    }


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


@router.post("/login")
async def login(body: LoginRequest, auth_config: AuthConfigDep):
    """Exchange credentials for a JWT bearer token."""
    if not auth_config.verify_password(body.username, body.password):
        raise HTTPException(
            status_code=401,
            detail={
                "code": EC.AUTH_CREDENTIALS_INVALID,
                "message": "Invalid username or password",
            },
        )

    return ok(_login_payload(auth_config, subject=body.username))


@router.get("/profile", dependencies=AuthRequired)
async def get_profile(auth_config: AuthConfigDep):
    return ok(
        {
            "username": auth_config.username,
            "must_change_credentials": auth_config.is_using_default_credentials(),
        }
    )


@router.patch("/profile", dependencies=AuthRequired)
async def update_profile(
    body: UpdateProfileRequest,
    auth_config: AuthConfigDep,
    boot=BootDep,
):
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
    return ok(_login_payload(auth_config, subject=username))
