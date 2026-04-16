"""Unified API response envelope models per 16_api_communication_spec.md."""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel, Field


class ErrorBody(BaseModel):
    code: str
    message: str


class Envelope(BaseModel):
    success: bool
    data: Any = None
    error: ErrorBody | None = None
    timestamp: int = Field(default_factory=lambda: int(time.time()))


class EC:
    """Standardized error codes."""

    AUTH_TOKEN_MISSING = "AUTH_TOKEN_MISSING"
    AUTH_TOKEN_INVALID = "AUTH_TOKEN_INVALID"
    AUTH_TOKEN_EXPIRED = "AUTH_TOKEN_EXPIRED"
    AUTH_CREDENTIALS_INVALID = "AUTH_CREDENTIALS_INVALID"
    INSTANCE_NOT_FOUND = "INSTANCE_NOT_FOUND"
    INSTANCE_ALREADY_EXISTS = "INSTANCE_ALREADY_EXISTS"
    INSTANCE_ALREADY_RUNNING = "INSTANCE_ALREADY_RUNNING"
    INSTANCE_NOT_RUNNING = "INSTANCE_NOT_RUNNING"
    PLUGIN_NOT_FOUND = "PLUGIN_NOT_FOUND"
    PLUGIN_RELOAD_FAILED = "PLUGIN_RELOAD_FAILED"
    PLUGIN_RESCAN_FAILED = "PLUGIN_RESCAN_FAILED"
    CONFIG_WRITE_FAILED = "CONFIG_WRITE_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_ACTION = "INVALID_ACTION"
    UNSUPPORTED_PLATFORM = "UNSUPPORTED_PLATFORM"
    PROVIDER_NOT_FOUND = "PROVIDER_NOT_FOUND"
    PROVIDER_ALREADY_EXISTS = "PROVIDER_ALREADY_EXISTS"
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"
    MODEL_ALREADY_EXISTS = "MODEL_ALREADY_EXISTS"
    ROUTE_NOT_FOUND = "ROUTE_NOT_FOUND"
    ROUTE_ALREADY_EXISTS = "ROUTE_ALREADY_EXISTS"
    PERSONA_NOT_FOUND = "PERSONA_NOT_FOUND"
    PERSONA_ALREADY_EXISTS = "PERSONA_ALREADY_EXISTS"


def ok(data: Any = None) -> dict:
    """Wrap successful response data in standard Envelope."""
    return Envelope(success=True, data=data).model_dump()


def fail(code: str, message: str) -> dict:
    """Wrap error in standard Envelope (use for direct JSONResponse construction)."""
    return Envelope(success=False, error=ErrorBody(code=code, message=message)).model_dump()
