"""Provider-owned configuration schema registry."""

from shinbot.core.config_provider.loader import (
    ConfigProviderLoadError,
    load_provider_schema,
    load_provider_schema_from_module,
)
from shinbot.core.config_provider.registry import ConfigProviderRegistry
from shinbot.core.config_provider.schema import (
    ConfigFieldDefinition,
    ConfigFieldType,
    ConfigProviderDefinition,
    ConfigProviderKind,
    ConfigValidationIssue,
)

__all__ = [
    "ConfigFieldDefinition",
    "ConfigFieldType",
    "ConfigProviderDefinition",
    "ConfigProviderKind",
    "ConfigProviderLoadError",
    "ConfigProviderRegistry",
    "ConfigValidationIssue",
    "load_provider_schema",
    "load_provider_schema_from_module",
]
