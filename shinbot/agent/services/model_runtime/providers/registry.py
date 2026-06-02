"""Provider descriptor registry for model runtime integrations."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any, Literal

AuthStrategy = Literal["bearer", "azure_api_key", "anthropic_api_key", "gemini_api_key", "none"]
CatalogFormat = Literal["openai_data", "ollama_models", "gemini_models"]
ContextWindowInferer = Callable[[dict[str, Any], str], int | None]
ProviderFieldLocation = Literal["auth", "default_params"]
ProviderFieldControl = Literal["string", "secret", "json", "key_value"]


@dataclass(slots=True, frozen=True)
class ProviderFieldDescriptor:
    """Schema fragment for a provider-managed configuration field."""

    key: str
    location: ProviderFieldLocation
    control: ProviderFieldControl
    label: str = ""
    description: str = ""
    placeholder: str = ""
    default_value: Any = None
    secret: bool = False


@dataclass(slots=True, frozen=True)
class ProviderPresetDescriptor:
    """Selectable preset shown in management UI for a provider type."""

    key: str
    label: str
    default_base_url: str = ""
    description: str = ""
    icon: str = ""
    recommended: bool = False


@dataclass(slots=True, frozen=True)
class ModelProviderDescriptor:
    """Descriptor for one provider type's runtime and admin behavior."""

    provider_type: str
    supported_backends: frozenset[str]
    display_name: str = ""
    description: str = ""
    icon: str = ""
    default_base_url: str = ""
    presets: tuple[ProviderPresetDescriptor, ...] = ()
    config_fields: tuple[ProviderFieldDescriptor, ...] = ()
    auth_strategy: AuthStrategy = "bearer"
    auth_param_key: str = "api_key"
    static_headers: dict[str, str] = field(default_factory=dict)
    request_headers_param_keys: tuple[str, ...] = ("requestHeaders", "extra_headers")
    litellm_custom_llm_provider: str | None = None
    model_info_custom_llm_provider: str | None = None
    request_model_prefixes: dict[str, str] = field(default_factory=dict)
    catalog_path: str | None = "/models"
    catalog_format: CatalogFormat = "openai_data"
    catalog_backend_prefix: str = ""

    def supports_backend(self, backend_name: str) -> bool:
        """Return whether this provider can be used with the named backend."""

        return backend_name in self.supported_backends

    def request_headers(self, provider: dict[str, Any]) -> dict[str, str]:
        """Build outbound HTTP headers for provider-side admin requests."""

        headers = dict(self.static_headers)
        auth = provider.get("auth") or {}
        api_key = auth.get(self.auth_param_key)
        if api_key:
            if self.auth_strategy == "azure_api_key":
                headers["api-key"] = str(api_key)
            elif self.auth_strategy == "anthropic_api_key":
                headers["x-api-key"] = str(api_key)
            elif self.auth_strategy == "gemini_api_key":
                headers["x-goog-api-key"] = str(api_key)
            elif self.auth_strategy == "bearer":
                headers["Authorization"] = f"Bearer {api_key}"

        default_params = provider.get("default_params") or {}
        for key in self.request_headers_param_keys:
            value = default_params.get(key)
            if not isinstance(value, dict):
                continue
            for header_name, header_value in value.items():
                if header_value is None:
                    continue
                headers[str(header_name)] = str(header_value)
        return headers

    def backend_client_auth_value(self, provider: dict[str, Any], *, backend_name: str) -> str:
        """Return the auth credential value used by a backend client."""

        auth = provider.get("auth") or {}
        value = auth.get(self.auth_param_key)
        return str(value or "")

    def merge_request_header_params(self, *sources: dict[str, Any]) -> dict[str, Any]:
        """Merge provider-defined request-header parameter maps from multiple sources."""

        merged: dict[str, Any] = {}
        for source in sources:
            for key in self.request_headers_param_keys:
                value = source.get(key)
                if isinstance(value, dict):
                    merged.update(value)
        return merged

    @property
    def supports_catalog(self) -> bool:
        """Return whether this provider exposes a remote catalog endpoint."""

        return self.catalog_path is not None

    def field(self, key: str, *, location: ProviderFieldLocation | None = None) -> ProviderFieldDescriptor | None:
        """Return one configured management field, if present."""

        for field_descriptor in self.config_fields:
            if field_descriptor.key != key:
                continue
            if location is not None and field_descriptor.location != location:
                continue
            return field_descriptor
        return None

    def request_model_name(self, backend_model: str, *, backend_name: str) -> str:
        """Translate stored backend model id into the request model name."""

        prefix = self.request_model_prefixes.get(backend_name, "")
        if not prefix:
            return backend_model
        needle = f"{prefix}/"
        if backend_model.startswith(needle):
            return backend_model[len(needle) :]
        return backend_model

    def normalize_runtime_messages(
        self,
        messages: list[dict[str, Any]],
        *,
        backend_name: str,
    ) -> list[dict[str, Any]]:
        """Normalize request messages for a provider/backend pair."""

        normalized: list[dict[str, Any]] = []
        seen_non_system = False
        for message in messages:
            copied = dict(message)
            if copied.get("role") == "system" and seen_non_system:
                copied["role"] = "user"
            elif copied.get("role") == "system":
                copied["content"] = self.normalize_system_message_content(
                    copied.get("content"),
                    backend_name=backend_name,
                )
            else:
                seen_non_system = True
            normalized.append(copied)
        return normalized

    def normalize_system_message_content(
        self,
        content: Any,
        *,
        backend_name: str,
    ) -> Any:
        """Normalize one system-message content block for provider quirks."""

        if self.provider_type != "dashscope":
            return content
        if not isinstance(content, list):
            return content
        if any(isinstance(item, dict) and item.get("cache_control") for item in content):
            return content

        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text = str(item.get("text", "") or "").strip()
                if text:
                    text_parts.append(text)
            elif isinstance(item, str) and item.strip():
                text_parts.append(item.strip())
        return "\n\n".join(text_parts)

    def catalog_url(self, provider: dict[str, Any]) -> str | None:
        """Return the catalog endpoint URL for a provider, if supported."""

        if self.catalog_path is None:
            return None
        base_url = str(provider.get("base_url") or "").rstrip("/")
        if not base_url:
            return None
        return f"{base_url}{self.catalog_path}"

    def normalize_catalog(
        self,
        provider: dict[str, Any],
        body: Any,
        *,
        infer_context_window: ContextWindowInferer,
    ) -> list[dict[str, Any]]:
        """Normalize a provider-specific catalog response into model records."""

        if self.catalog_format == "ollama_models":
            models: list[dict[str, Any]] = []
            for item in body.get("models", []):
                model_id = item.get("name")
                if not model_id:
                    continue
                backend_model = self._catalog_backend_model(str(model_id))
                models.append(
                    {
                        "id": str(model_id),
                        "displayName": str(model_id),
                        "backendModel": backend_model,
                        "contextWindow": infer_context_window(provider, backend_model),
                    }
                )
            return models

        if self.catalog_format == "gemini_models":
            models = []
            for item in body.get("models", []):
                raw_name = str(item.get("name") or "")
                model_id = raw_name.replace("models/", "")
                if not model_id:
                    continue
                backend_model = self._catalog_backend_model(model_id)
                models.append(
                    {
                        "id": model_id,
                        "displayName": str(item.get("displayName") or model_id),
                        "backendModel": backend_model,
                        "contextWindow": infer_context_window(provider, backend_model),
                    }
                )
            return models

        items = body.get("data", [])
        models = []
        for item in items:
            model_id = item.get("id")
            if not model_id:
                continue
            backend_model = self._catalog_backend_model(str(model_id))
            models.append(
                {
                    "id": str(model_id),
                    "displayName": str(item.get("name") or model_id),
                    "backendModel": backend_model,
                    "contextWindow": infer_context_window(provider, backend_model),
                }
            )
        return models

    def _catalog_backend_model(self, model_id: str) -> str:
        if not self.catalog_backend_prefix:
            return model_id
        return f"{self.catalog_backend_prefix}/{model_id}"


class ModelProviderRegistry:
    """In-memory registry of provider descriptors."""

    def __init__(self) -> None:
        self._descriptors: dict[str, ModelProviderDescriptor] = {}

    def register(self, descriptor: ModelProviderDescriptor) -> None:
        """Register or replace a descriptor by provider type."""

        self._descriptors[descriptor.provider_type] = descriptor

    def get(self, provider_type: str) -> ModelProviderDescriptor | None:
        """Return the descriptor for a provider type, if present."""

        normalized = str(provider_type or "").strip()
        if not normalized:
            return None
        return self._descriptors.get(normalized)

    def require(self, provider_type: str) -> ModelProviderDescriptor:
        """Return the descriptor for a provider type or raise."""

        descriptor = self.get(provider_type)
        if descriptor is None:
            raise KeyError(f"Unknown model provider type: {provider_type!r}")
        return descriptor

    def provider_types(self) -> frozenset[str]:
        """Return the currently registered provider type ids."""

        return frozenset(self._descriptors)

    def descriptor_items(self) -> Iterable[tuple[str, ModelProviderDescriptor]]:
        """Iterate over registered provider descriptors keyed by type."""

        return tuple(self._descriptors.items())

    def descriptors(self) -> Iterable[ModelProviderDescriptor]:
        """Iterate over registered descriptors."""

        return tuple(self._descriptors.values())


_DEFAULT_PROVIDER_REGISTRY = ModelProviderRegistry()


def register_provider_descriptor(descriptor: ModelProviderDescriptor) -> None:
    """Register a provider descriptor in the default registry."""

    _DEFAULT_PROVIDER_REGISTRY.register(descriptor)


def get_provider_descriptor(provider_type: str) -> ModelProviderDescriptor | None:
    """Look up a provider descriptor in the default registry."""

    return _DEFAULT_PROVIDER_REGISTRY.get(provider_type)


def require_provider_descriptor(provider_type: str) -> ModelProviderDescriptor:
    """Look up a provider descriptor or raise when it is missing."""

    return _DEFAULT_PROVIDER_REGISTRY.require(provider_type)


def supported_provider_types() -> frozenset[str]:
    """Return all provider types from the default registry."""

    return _DEFAULT_PROVIDER_REGISTRY.provider_types()


def registered_provider_descriptors() -> tuple[ModelProviderDescriptor, ...]:
    """Return all registered provider descriptors from the default registry."""

    return tuple(_DEFAULT_PROVIDER_REGISTRY.descriptors())
