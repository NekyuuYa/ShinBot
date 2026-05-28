"""Config provider registry router: /api/v1/config-providers."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import Envelope, ok
from shinbot.core.config_provider import ConfigProviderKind

router = APIRouter(
    prefix="/config-providers",
    tags=["config-providers"],
    dependencies=AuthRequired,
)


class ValidateConfigRequest(BaseModel):
    config: dict[str, Any] = Field(default_factory=dict)
    pathPrefix: str = ""
    strict: bool = False


class ConfigProviderSummary(BaseModel):
    """Compact representation of a registered config provider."""

    model_config = {"extra": "allow"}

    kind: str = ""
    id: str = ""
    display_name: str = ""
    description: str = ""
    config_version: str = ""
    fields: list[dict[str, Any]] = Field(default_factory=list)
    example_toml: str = ""
    owner_module: str = ""
    source_path: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)


class ConfigProviderDefaults(BaseModel):
    """Default configuration for a config provider."""

    defaults: dict[str, Any] = Field(default_factory=dict)


class ConfigProviderValidationIssue(BaseModel):
    """A single validation issue from a config provider."""

    path: str = ""
    message: str = ""
    code: str = ""


class ConfigProviderValidationResult(BaseModel):
    """Validation result for a config provider."""

    issues: list[ConfigProviderValidationIssue] = Field(default_factory=list)


def _coerce_kind_or_404(kind: str) -> ConfigProviderKind:
    try:
        return ConfigProviderKind(kind)
    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "CONFIG_PROVIDER_NOT_FOUND",
                "message": f"Config provider kind {kind!r} is not registered",
            },
        ) from exc


def _provider_or_404(bot: Any, kind: str, provider_id: str) -> Any:
    provider_kind = _coerce_kind_or_404(kind)
    provider = bot.config_provider_registry.get(provider_kind, provider_id)
    if provider is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "CONFIG_PROVIDER_NOT_FOUND",
                "message": f"Config provider {provider_kind.value}:{provider_id} is not registered",
            },
        )
    return provider


@router.get("", response_model=Envelope[list[ConfigProviderSummary]])
async def list_config_providers(kind: str | None = Query(default=None), bot=BotDep):
    """List registered config providers, optionally filtered by kind."""
    provider_kind = _coerce_kind_or_404(kind) if kind else None
    return ok(bot.config_provider_registry.catalog(provider_kind))


@router.get("/{kind}/{provider_id}", response_model=Envelope[ConfigProviderSummary])
async def get_config_provider(kind: str, provider_id: str, bot=BotDep):
    """Retrieve a single config provider definition by kind and id."""
    return ok(_provider_or_404(bot, kind, provider_id).to_dict())


@router.get("/{kind}/{provider_id}/defaults", response_model=Envelope[dict[str, Any]])
async def get_config_provider_defaults(kind: str, provider_id: str, bot=BotDep):
    """Return the default configuration for a config provider."""
    _provider_or_404(bot, kind, provider_id)
    return ok(bot.config_provider_registry.default_config(kind, provider_id))


@router.post("/{kind}/{provider_id}/validate", response_model=Envelope[ConfigProviderValidationResult])
async def validate_config_provider(
    kind: str,
    provider_id: str,
    body: ValidateConfigRequest,
    bot=BotDep,
):
    """Validate a configuration payload against a config provider's schema."""
    _provider_or_404(bot, kind, provider_id)
    issues = bot.config_provider_registry.validate(
        kind,
        provider_id,
        body.config,
        path_prefix=body.pathPrefix,
        strict=body.strict,
    )
    return ok({"issues": [issue.to_dict() for issue in issues]})
