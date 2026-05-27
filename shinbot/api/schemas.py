"""Shared Pydantic response models for the ShinBot management API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from shinbot.api.models import Envelope, ErrorBody  # noqa: F401 – re-export

# ── Auth ─────────────────────────────────────────────────────────────


class LoginPayload(BaseModel):
    """Successful login / credential-update response."""

    model_config = ConfigDict(extra="allow")

    expires_in_hours: int
    username: str
    must_change_credentials: bool


class LogoutPayload(BaseModel):
    """Logout confirmation."""

    model_config = ConfigDict(extra="allow")

    logged_out: bool


class ProfilePayload(BaseModel):
    """Current admin profile."""

    model_config = ConfigDict(extra="allow")

    username: str
    must_change_credentials: bool


# ── Instances ────────────────────────────────────────────────────────


class DeletedResponse(BaseModel):
    """Generic deletion confirmation."""

    model_config = ConfigDict(extra="allow")

    deleted: bool
    id: str | None = None
    uuid: str | None = None
    fileName: str | None = None


class InstanceControlResponse(BaseModel):
    """Instance start/stop result."""

    model_config = ConfigDict(extra="allow")

    id: str
    state: str


# ── Plugins ──────────────────────────────────────────────────────────


class PluginRescanResponse(BaseModel):
    """Plugin rescan / reload result."""

    model_config = ConfigDict(extra="allow")

    loaded_count: int
    plugins: list[Any]


# ── Config providers ─────────────────────────────────────────────────


class ValidationIssuesResponse(BaseModel):
    """Validation result containing issue list."""

    model_config = ConfigDict(extra="allow")

    issues: list[dict[str, Any]]


# ── System ───────────────────────────────────────────────────────────


class RestartResponse(BaseModel):
    """Restart request accepted."""

    model_config = ConfigDict(extra="allow")

    accepted: bool
    restart_requested: bool
    restart_request: dict[str, Any] | None = None


class RuntimeStateResponse(BaseModel):
    """Current runtime state (restart request status)."""

    model_config = ConfigDict(extra="allow")

    restart_requested: bool
    restart_request: dict[str, Any] | None = None


# ── Model runtime – execution audit page ─────────────────────────────


class ExecutionAuditPage(BaseModel):
    """Paginated model execution audit records."""

    model_config = ConfigDict(extra="allow")

    items: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


# ── Model runtime – execution payload ────────────────────────────────


class ExecutionPayloadResponse(BaseModel):
    """Audit payload for a single model execution."""

    model_config = ConfigDict(extra="allow")

    available: bool
    execution_id: str
    expired: bool
    request: dict[str, Any] | None = None
    response: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None
    return_value: dict[str, Any] | None = Field(None, alias="return")
