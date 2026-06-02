"""Built-in provider descriptors for bundled model integrations."""

from __future__ import annotations

from shinbot.agent.services.model_runtime.providers.registry import (
    ModelProviderDescriptor,
    ProviderFieldDescriptor,
    ProviderPresetDescriptor,
    register_provider_descriptor,
)


def _token_field(*, label: str = "API Key") -> ProviderFieldDescriptor:
    return ProviderFieldDescriptor(
        key="api_key",
        location="auth",
        control="secret",
        label=label,
        description="Credential used for authenticated requests to the provider.",
        secret=True,
    )


def _request_headers_field() -> ProviderFieldDescriptor:
    return ProviderFieldDescriptor(
        key="requestHeaders",
        location="default_params",
        control="key_value",
        label="Request Headers",
        description="Additional HTTP headers forwarded to provider requests.",
        default_value={},
    )


def _proxy_field() -> ProviderFieldDescriptor:
    return ProviderFieldDescriptor(
        key="proxy",
        location="default_params",
        control="string",
        label="Proxy Address",
        description="Optional HTTP proxy used when contacting the provider.",
        placeholder="http://127.0.0.1:7890",
    )


def _thinking_field() -> ProviderFieldDescriptor:
    return ProviderFieldDescriptor(
        key="thinking",
        location="default_params",
        control="json",
        label="Thinking Config",
        description="Provider-specific reasoning or thinking options.",
        default_value=None,
    )


def _filters_field() -> ProviderFieldDescriptor:
    return ProviderFieldDescriptor(
        key="filters",
        location="default_params",
        control="json",
        label="Filters Config",
        description="Provider-specific filtering or policy options.",
        default_value=None,
    )


def _api_version_field() -> ProviderFieldDescriptor:
    return ProviderFieldDescriptor(
        key="apiVersion",
        location="default_params",
        control="string",
        label="API Version",
        description="Provider API version appended to management and runtime requests.",
        placeholder="2024-10-21",
    )


def _make_preset(
    key: str,
    label: str,
    default_base_url: str,
    *,
    description: str = "",
    icon: str = "",
    recommended: bool = False,
) -> ProviderPresetDescriptor:
    return ProviderPresetDescriptor(
        key=key,
        label=label,
        default_base_url=default_base_url,
        description=description,
        icon=icon,
        recommended=recommended,
    )


def register_builtin_provider_descriptors() -> None:
    """Populate the default provider registry with bundled descriptors."""

    descriptors = (
        ModelProviderDescriptor(
            provider_type="openai",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="OpenAI",
            icon="mdi-cloud-outline",
            default_base_url="https://api.openai.com/v1",
            presets=(
                _make_preset(
                    "openai",
                    "OpenAI",
                    "https://api.openai.com/v1",
                    recommended=True,
                    icon="mdi-cloud-outline",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
            ),
            request_model_prefixes={"openai_compatible": "openai"},
        ),
        ModelProviderDescriptor(
            provider_type="custom_openai",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="Custom OpenAI Compatible",
            icon="mdi-api",
            default_base_url="https://api.example.com/v1",
            presets=(
                _make_preset(
                    "custom_openai",
                    "Custom OpenAI Compatible",
                    "https://api.example.com/v1",
                    icon="mdi-api",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
            ),
            litellm_custom_llm_provider="openai",
            request_model_prefixes={"openai_compatible": "openai"},
        ),
        ModelProviderDescriptor(
            provider_type="azure_openai",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="Azure OpenAI",
            icon="mdi-microsoft-azure",
            default_base_url="https://your-resource.openai.azure.com/openai",
            presets=(
                _make_preset(
                    "azure_openai",
                    "Azure OpenAI",
                    "https://your-resource.openai.azure.com/openai",
                    icon="mdi-microsoft-azure",
                ),
            ),
            config_fields=(
                _token_field(label="API Key"),
                _api_version_field(),
                _proxy_field(),
                _request_headers_field(),
            ),
            auth_strategy="azure_api_key",
            litellm_custom_llm_provider="azure",
            request_model_prefixes={"openai_compatible": "openai"},
        ),
        ModelProviderDescriptor(
            provider_type="dashscope",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="DashScope (Qwen)",
            icon="mdi-cloud-outline",
            default_base_url="https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
            presets=(
                _make_preset(
                    "dashscope",
                    "DashScope (Qwen)",
                    "https://dashscope-intl.aliyuncs.com/compatible-mode/v1",
                    icon="mdi-cloud-outline",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
                _thinking_field(),
            ),
            litellm_custom_llm_provider="dashscope",
            model_info_custom_llm_provider="dashscope",
        ),
        ModelProviderDescriptor(
            provider_type="siliconflow",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="SiliconFlow",
            icon="mdi-atom",
            default_base_url="https://api.siliconflow.cn/v1",
            presets=(
                _make_preset(
                    "siliconflow",
                    "SiliconFlow",
                    "https://api.siliconflow.cn/v1",
                    icon="mdi-atom",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
            ),
            litellm_custom_llm_provider="openai",
            model_info_custom_llm_provider="openai",
        ),
        ModelProviderDescriptor(
            provider_type="xiaomi_mimo",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="Xiaomi MiMo",
            icon="mdi-cellphone",
            default_base_url="https://api.xiaomimimo.com/v1",
            presets=(
                _make_preset(
                    "xiaomi_mimo",
                    "Xiaomi MiMo",
                    "https://api.xiaomimimo.com/v1",
                    icon="mdi-cellphone",
                ),
                _make_preset(
                    "xiaomi_mimo_token_plan",
                    "Xiaomi MiMo Token Plan",
                    "https://token-plan-cn.xiaomimimo.com/v1",
                    icon="mdi-cellphone",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
                _thinking_field(),
            ),
            litellm_custom_llm_provider="openai",
            request_model_prefixes={"litellm": "xiaomi_mimo", "openai_compatible": "xiaomi_mimo"},
            catalog_backend_prefix="xiaomi_mimo",
        ),
        ModelProviderDescriptor(
            provider_type="openrouter",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="OpenRouter",
            icon="mdi-cloud-outline",
            default_base_url="https://openrouter.ai/api/v1",
            presets=(
                _make_preset(
                    "openrouter",
                    "OpenRouter",
                    "https://openrouter.ai/api/v1",
                    icon="mdi-cloud-outline",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
                _thinking_field(),
            ),
            request_model_prefixes={"openai_compatible": "openrouter"},
            catalog_backend_prefix="openrouter",
        ),
        ModelProviderDescriptor(
            provider_type="deepseek",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="DeepSeek",
            icon="mdi-fish",
            default_base_url="https://api.deepseek.com",
            presets=(
                _make_preset(
                    "deepseek",
                    "DeepSeek",
                    "https://api.deepseek.com",
                    icon="mdi-fish",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
                _thinking_field(),
            ),
            request_model_prefixes={"openai_compatible": "deepseek"},
            catalog_backend_prefix="deepseek",
        ),
        ModelProviderDescriptor(
            provider_type="ollama",
            supported_backends=frozenset({"litellm", "openai_compatible"}),
            display_name="Ollama",
            icon="mdi-lan",
            default_base_url="http://127.0.0.1:11434",
            presets=(
                _make_preset(
                    "ollama",
                    "Ollama",
                    "http://127.0.0.1:11434",
                    icon="mdi-lan",
                ),
            ),
            config_fields=(
                _proxy_field(),
                _request_headers_field(),
            ),
            request_model_prefixes={"openai_compatible": "ollama"},
            catalog_path="/api/tags",
            catalog_format="ollama_models",
            catalog_backend_prefix="ollama",
        ),
        ModelProviderDescriptor(
            provider_type="anthropic",
            supported_backends=frozenset({"litellm"}),
            display_name="Anthropic",
            icon="mdi-alpha-a-circle-outline",
            default_base_url="https://api.anthropic.com",
            presets=(
                _make_preset(
                    "anthropic",
                    "Anthropic",
                    "https://api.anthropic.com",
                    icon="mdi-alpha-a-circle-outline",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
                _thinking_field(),
            ),
            auth_strategy="anthropic_api_key",
            static_headers={"anthropic-version": "2023-06-01"},
            catalog_path="/v1/models",
            catalog_backend_prefix="anthropic",
        ),
        ModelProviderDescriptor(
            provider_type="gemini",
            supported_backends=frozenset({"litellm"}),
            display_name="Gemini",
            icon="mdi-google",
            default_base_url="https://generativelanguage.googleapis.com",
            presets=(
                _make_preset(
                    "gemini",
                    "Gemini",
                    "https://generativelanguage.googleapis.com",
                    icon="mdi-google",
                ),
            ),
            config_fields=(
                _token_field(),
                _proxy_field(),
                _request_headers_field(),
                _thinking_field(),
                _filters_field(),
            ),
            auth_strategy="gemini_api_key",
            catalog_path="/v1beta/models",
            catalog_format="gemini_models",
            catalog_backend_prefix="gemini",
        ),
    )
    for descriptor in descriptors:
        register_provider_descriptor(descriptor)
