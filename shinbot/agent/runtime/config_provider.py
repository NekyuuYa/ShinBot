"""Config provider registration for Agent runtime configs."""

from __future__ import annotations

from pathlib import Path

from shinbot.core.config_provider import (
    ConfigProviderDefinition,
    ConfigProviderRegistry,
    load_provider_schema,
)

AGENT_RUNTIME_CONFIG_PROVIDER_ID = "shinbot.agent.runtime"


def load_agent_runtime_config_provider() -> ConfigProviderDefinition:
    """Load the built-in Agent runtime config provider schema."""

    module_dir = Path(__file__).resolve().parent
    repo_root = module_dir.parents[2]
    return load_provider_schema(
        module_dir / "config.schema.toml",
        example_path=repo_root / "agent.example.toml",
        owner_module="shinbot.agent.runtime",
    )


def register_builtin_agent_config_provider(registry: ConfigProviderRegistry) -> None:
    """Register the built-in Agent runtime config schema."""

    registry.upsert(load_agent_runtime_config_provider())


__all__ = [
    "AGENT_RUNTIME_CONFIG_PROVIDER_ID",
    "load_agent_runtime_config_provider",
    "register_builtin_agent_config_provider",
]
