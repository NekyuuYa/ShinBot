"""Model runtime backend implementations."""

from shinbot.agent.services.model_runtime.backends.litellm import LiteLLMBackend
from shinbot.agent.services.model_runtime.backends.openai_compatible import (
    OpenAICompatibleBackend,
)
from shinbot.agent.services.model_runtime.backends.protocol import (
    BackendOperation,
    BackendRequestPlan,
    ModelBackend,
)
from shinbot.agent.services.model_runtime.backends.registry import (
    ModelBackendDescriptor,
    ModelBackendRegistry,
    create_registered_backend,
    get_backend_descriptor,
    register_backend,
    registered_backend_descriptors,
    supported_backend_names,
)

register_backend(
    "litellm",
    LiteLLMBackend,
    descriptor=ModelBackendDescriptor(
        name="litellm",
        display_name="LiteLLM Backend",
        description="Compatibility backend built on LiteLLM provider adapters.",
        kind="compatibility",
    ),
)
register_backend(
    "openai_compatible",
    OpenAICompatibleBackend,
    descriptor=ModelBackendDescriptor(
        name="openai_compatible",
        display_name="OpenAI-Compatible Backend",
        description="Native backend for OpenAI-style HTTP APIs with provider descriptors.",
        kind="native",
    ),
)

__all__ = [
    "BackendOperation",
    "BackendRequestPlan",
    "ModelBackendDescriptor",
    "LiteLLMBackend",
    "ModelBackendRegistry",
    "ModelBackend",
    "OpenAICompatibleBackend",
    "create_registered_backend",
    "get_backend_descriptor",
    "registered_backend_descriptors",
    "register_backend",
    "supported_backend_names",
]
