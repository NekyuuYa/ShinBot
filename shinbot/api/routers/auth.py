"""Auth router: POST /api/v1/auth/login"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from shinbot.api.deps import _auth_config
from shinbot.api.models import EC, ok

router = APIRouter(prefix="/auth", tags=["auth"])
AuthConfigDep = Annotated[object, Depends(_auth_config)]


class LoginRequest(BaseModel):
    username: str
    password: str


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
    token = auth_config.create_token()
    return ok({"token": token, "token_type": "Bearer", "expires_in_hours": auth_config.jwt_expire_hours})
