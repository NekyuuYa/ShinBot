"""Config provider registration for Agent runtime configs."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from shinbot.core.config_provider import (
    ConfigFieldDefinition,
    ConfigProviderDefinition,
    ConfigProviderRegistry,
    load_provider_schema,
)

AGENT_RUNTIME_CONFIG_PROVIDER_ID = "shinbot.agent.runtime"


def load_agent_runtime_config_provider() -> ConfigProviderDefinition:
    """Load the built-in Agent runtime config provider schema."""

    module_dir = Path(__file__).resolve().parent
    repo_root = module_dir.parents[2]
    provider = load_provider_schema(
        module_dir / "config.schema.toml",
        example_path=repo_root / "agent.example.toml",
        owner_module="shinbot.agent.runtime",
    )
    return replace(
        provider,
        metadata={
            **dict(provider.metadata),
            "capabilities": _build_capability_map(provider.fields),
        },
    )


def register_builtin_agent_config_provider(registry: ConfigProviderRegistry) -> None:
    """Register the built-in Agent runtime config schema."""

    registry.upsert(load_agent_runtime_config_provider())


def _build_capability_map(
    fields: tuple[ConfigFieldDefinition, ...],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "effective": [],
        "reserved": [],
        "deprecated": [],
        "status_by_path": {},
    }
    for field in fields:
        status = _field_capability_status(field)
        result[status].append(field.path)
        result["status_by_path"][field.path] = status
    return result


def _field_capability_status(field: ConfigFieldDefinition) -> str:
    if field.deprecated:
        return "deprecated"
    if field.metadata.get("runtime_status") == "reserved":
        return "reserved"
    return "effective"


__all__ = [
    "AGENT_RUNTIME_CONFIG_PROVIDER_ID",
    "load_agent_runtime_config_provider",
    "register_builtin_agent_config_provider",
]
