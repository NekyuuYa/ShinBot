"""Provider descriptor registry for model runtime integrations."""

from shinbot.agent.services.model_runtime.providers.builtin import (
    register_builtin_provider_descriptors,
)
from shinbot.agent.services.model_runtime.providers.registry import (
    CatalogFormat,
    ContextWindowInferer,
    ModelProviderDescriptor,
    ModelProviderRegistry,
    ProviderFieldDescriptor,
    ProviderPresetDescriptor,
    get_provider_descriptor,
    register_provider_descriptor,
    registered_provider_descriptors,
    require_provider_descriptor,
    supported_provider_types,
)

register_builtin_provider_descriptors()

__all__ = [
    "CatalogFormat",
    "ContextWindowInferer",
    "ModelProviderDescriptor",
    "ModelProviderRegistry",
    "ProviderFieldDescriptor",
    "ProviderPresetDescriptor",
    "get_provider_descriptor",
    "registered_provider_descriptors",
    "register_provider_descriptor",
    "register_builtin_provider_descriptors",
    "require_provider_descriptor",
    "supported_provider_types",
]
