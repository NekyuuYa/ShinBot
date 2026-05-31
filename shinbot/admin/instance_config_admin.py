"""Administrative helpers for instance-config management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from shinbot.agent.runtime.instance_config import parse_tagged_llm_ref
from shinbot.core.application.config_sections import iter_adapter_instance_records
from shinbot.persistence.records import InstanceConfigRecord, utc_now_iso


@dataclass(slots=True)
class InstanceConfigAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class NormalizedInstanceConfigInput:
    instance_id: str
    main_llm: str
    config: dict[str, Any]
    tags: list[str]


def normalize_optional_string(value: Any) -> str | None:
    """Normalise an optional string value, returning ``None`` for blank/empty.

    Args:
        value: The raw value to normalise.

    Returns:
        The stripped string, or ``None`` if empty.
    """
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_optional_int(value: Any, *, field_name: str) -> int | None:
    """Normalise an optional value to an integer, or ``None``.

    Args:
        value: The raw value to parse.
        field_name: Name of the field (used in error messages).

    Returns:
        Parsed integer, or ``None`` if empty.

    Raises:
        InstanceConfigAdminError: If the value cannot be parsed as int.
    """
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        value = normalized
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise InstanceConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message=f"InstanceConfig {field_name} must be an integer",
        ) from exc
    return parsed


def normalize_optional_float(value: Any, *, field_name: str) -> float | None:
    """Normalise an optional value to a float, or ``None``.

    Args:
        value: The raw value to parse.
        field_name: Name of the field (used in error messages).

    Returns:
        Parsed float, or ``None`` if empty.

    Raises:
        InstanceConfigAdminError: If the value cannot be parsed as float.
    """
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        value = normalized
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise InstanceConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message=f"InstanceConfig {field_name} must be a number",
        ) from exc
    return parsed


def normalize_optional_bool(value: Any, *, field_name: str) -> bool | None:
    """Normalise an optional value to a boolean, or ``None``.

    Args:
        value: The raw value to parse.
        field_name: Name of the field (used in error messages).

    Returns:
        Parsed boolean, or ``None`` if empty.

    Raises:
        InstanceConfigAdminError: If the value cannot be interpreted as bool.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    raise InstanceConfigAdminError(
        status_code=400,
        code="INVALID_ACTION",
        message=f"InstanceConfig {field_name} must be a boolean",
    )


def assign_optional_profile(config: dict[str, Any], key: str, value: Any) -> None:
    """Set a config key to a normalised optional string value.

    Args:
        config: The config dict to mutate.
        key: The config key to set.
        value: The raw value; skipped if ``None`` after normalisation.
    """
    normalized = normalize_optional_string(value)
    if normalized is None:
        return
    config[key] = normalized


def assign_optional_numeric(config: dict[str, Any], key: str, value: int | float | None) -> None:
    """Set a config key to a numeric value if present.

    Args:
        config: The config dict to mutate.
        key: The config key to set.
        value: The numeric value; skipped if ``None``.
    """
    if value is None:
        return
    config[key] = value


def assign_optional_bool(config: dict[str, Any], key: str, value: bool | None) -> None:
    """Set a config key to a boolean value, or remove it when false.

    Args:
        config: The config dict to mutate.
        key: The config key to set or remove.
        value: The boolean value; skipped if ``None``.
    """
    if value is None:
        return
    if value:
        config[key] = True
        return
    config.pop(key, None)


def extract_response_profiles(config: dict[str, Any]) -> dict[str, str | None]:
    """Extract response-profile fields from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        A dict with camelCase response-profile keys and their values.
    """
    return {
        "responseProfile": normalize_optional_string(config.get("response_profile")),
        "responseProfilePrivate": normalize_optional_string(config.get("response_profile_private")),
        "responseProfilePriority": normalize_optional_string(
            config.get("response_profile_priority")
        ),
        "responseProfileGroup": normalize_optional_string(config.get("response_profile_group")),
    }


def extract_explicit_prompt_cache_enabled(config: dict[str, Any]) -> bool:
    """Extract the explicit-prompt-cache-enabled flag from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        ``True`` if the flag is set and truthy, ``False`` otherwise.
    """
    return bool(normalize_optional_bool(
        config.get("explicit_prompt_cache_enabled"),
        field_name="explicit_prompt_cache_enabled",
    ))


def strip_response_profiles(config: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of config with response-profile keys removed.

    Args:
        config: The instance config dict.

    Returns:
        A new dict without response-profile keys.
    """
    cleaned = dict(config)
    cleaned.pop("response_profile", None)
    cleaned.pop("response_profile_private", None)
    cleaned.pop("response_profile_priority", None)
    cleaned.pop("response_profile_group", None)
    return cleaned


def extract_media_inspection_llm(config: dict[str, Any]) -> str:
    """Extract the media-inspection LLM target from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The LLM target string, or empty string if not set.
    """
    return str(config.get("media_inspection_llm") or "").strip()


def extract_media_inspection_prompt(config: dict[str, Any]) -> str:
    """Extract the media-inspection prompt from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The prompt string, or empty string if not set.
    """
    return str(config.get("media_inspection_prompt") or "").strip()


def extract_sticker_summary_llm(config: dict[str, Any]) -> str:
    """Extract the sticker-summary LLM target from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The LLM target string, or empty string if not set.
    """
    return str(config.get("sticker_summary_llm") or "").strip()


def extract_sticker_summary_prompt(config: dict[str, Any]) -> str:
    """Extract the sticker-summary prompt from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The prompt string, or empty string if not set.
    """
    return str(config.get("sticker_summary_prompt") or "").strip()


def extract_context_compression_llm(config: dict[str, Any]) -> str:
    """Extract the context-compression LLM target from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The LLM target string, or empty string if not set.
    """
    return str(config.get("context_compression_llm") or "").strip()


def extract_max_context_tokens(config: dict[str, Any]) -> int | None:
    """Extract the max-context-tokens value from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The integer value, or ``None`` if not set.
    """
    return normalize_optional_int(config.get("max_context_tokens"), field_name="max_context_tokens")


def extract_context_evict_ratio(config: dict[str, Any]) -> float | None:
    """Extract the context-evict-ratio from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The float ratio, or ``None`` if not set.
    """
    return normalize_optional_float(
        config.get("context_evict_ratio"),
        field_name="context_evict_ratio",
    )


def extract_context_compression_max_chars(config: dict[str, Any]) -> int | None:
    """Extract the context-compression-max-chars from a config dict.

    Args:
        config: The instance config dict.

    Returns:
        The integer value, or ``None`` if not set.
    """
    return normalize_optional_int(
        config.get("context_compression_max_chars"),
        field_name="context_compression_max_chars",
    )


def strip_media_inspection_llm(config: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of config with the media-inspection LLM key removed.

    Args:
        config: The instance config dict.

    Returns:
        A new dict without the media_inspection_llm key.
    """
    cleaned = dict(config)
    cleaned.pop("media_inspection_llm", None)
    return cleaned


def strip_explicit_context_fields(config: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of config with all explicit context fields removed.

    Removes media inspection, sticker summary, context compression,
    cache, and evict ratio keys.

    Args:
        config: The instance config dict.

    Returns:
        A new dict with explicit context keys stripped.
    """
    cleaned = strip_media_inspection_llm(config)
    cleaned.pop("explicit_prompt_cache_enabled", None)
    cleaned.pop("media_inspection_prompt", None)
    cleaned.pop("sticker_summary_llm", None)
    cleaned.pop("sticker_summary_prompt", None)
    cleaned.pop("context_compression_llm", None)
    cleaned.pop("max_context_tokens", None)
    cleaned.pop("context_evict_ratio", None)
    cleaned.pop("context_compression_max_chars", None)
    return cleaned


def serialize_instance_config(payload: dict[str, Any]) -> dict[str, Any]:
    """Serialise an instance-config record to the camelCase API shape.

    Args:
        payload: Internal instance-config dict with snake_case keys.

    Returns:
        A dict with camelCase keys for the front-end.
    """
    config = dict(payload["config"])
    return {
        "uuid": payload["uuid"],
        "instanceId": payload["instance_id"],
        "mainLlm": payload["main_llm"],
        "explicitPromptCacheEnabled": extract_explicit_prompt_cache_enabled(config),
        "mediaInspectionLlm": extract_media_inspection_llm(config),
        "mediaInspectionPrompt": extract_media_inspection_prompt(config),
        "stickerSummaryLlm": extract_sticker_summary_llm(config),
        "stickerSummaryPrompt": extract_sticker_summary_prompt(config),
        "contextCompressionLlm": extract_context_compression_llm(config),
        "maxContextTokens": extract_max_context_tokens(config),
        "contextEvictRatio": extract_context_evict_ratio(config),
        "contextCompressionMaxChars": extract_context_compression_max_chars(config),
        **extract_response_profiles(config),
        "config": strip_response_profiles(strip_explicit_context_fields(config)),
        "tags": payload["tags"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def known_instance_ids(bot: Any, boot: Any) -> set[str]:
    """Collect all known instance IDs from persisted config and live adapters.

    Args:
        bot: The running application.
        boot: The application boot controller.

    Returns:
        A set of known instance ID strings.
    """
    ids = {str(item.get("id")) for item in iter_adapter_instance_records(boot.config) if item.get("id")}
    ids.update(adapter.instance_id for adapter in bot.adapter_manager.all_instances)
    return ids


def normalize_instance_config_input(
    *,
    instance_id: str,
    main_llm: str,
    explicit_prompt_cache_enabled: Any = None,
    response_profile: str | None,
    response_profile_private: str | None,
    response_profile_priority: str | None,
    response_profile_group: str | None,
    media_inspection_llm: str | None = None,
    media_inspection_prompt: str | None = None,
    sticker_summary_llm: str | None = None,
    sticker_summary_prompt: str | None = None,
    context_compression_llm: str | None = None,
    max_context_tokens: Any = None,
    context_evict_ratio: Any = None,
    context_compression_max_chars: Any = None,
    config: dict[str, Any],
    tags: list[str],
) -> NormalizedInstanceConfigInput:
    """Normalise and validate raw instance-config input fields.

    Args:
        instance_id: The instance identifier.
        main_llm: The main LLM target string.
        explicit_prompt_cache_enabled: Optional explicit cache flag.
        response_profile: Optional response profile name.
        response_profile_private: Optional private response profile.
        response_profile_priority: Optional priority response profile.
        response_profile_group: Optional group response profile.
        media_inspection_llm: Optional media inspection LLM target.
        media_inspection_prompt: Optional media inspection prompt.
        sticker_summary_llm: Optional sticker summary LLM target.
        sticker_summary_prompt: Optional sticker summary prompt.
        context_compression_llm: Optional context compression LLM target.
        max_context_tokens: Optional max context token count.
        context_evict_ratio: Optional context eviction ratio.
        context_compression_max_chars: Optional compression max chars.
        config: The raw config dict.
        tags: Raw list of tag strings.

    Returns:
        A normalised instance config input dataclass.

    Raises:
        InstanceConfigAdminError: On validation failures.
    """
    normalized_instance_id = instance_id.strip()
    normalized_main_llm = main_llm.strip()
    normalized_tags = [tag.strip() for tag in tags if tag.strip()]
    normalized_config = dict(config)

    if not normalized_instance_id:
        raise InstanceConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="InstanceConfig instanceId must not be empty",
        )

    normalized_config.pop("response_profile", None)
    normalized_config.pop("response_profile_private", None)
    normalized_config.pop("response_profile_priority", None)
    normalized_config.pop("response_profile_group", None)
    normalized_config.pop("explicit_prompt_cache_enabled", None)
    normalized_config.pop("media_inspection_llm", None)
    normalized_config.pop("media_inspection_prompt", None)
    normalized_config.pop("sticker_summary_llm", None)
    normalized_config.pop("sticker_summary_prompt", None)
    normalized_config.pop("context_compression_llm", None)
    normalized_config.pop("max_context_tokens", None)
    normalized_config.pop("context_evict_ratio", None)
    normalized_config.pop("context_compression_max_chars", None)

    normalized_max_context_tokens = normalize_optional_int(
        max_context_tokens,
        field_name="max_context_tokens",
    )
    if normalized_max_context_tokens is not None and normalized_max_context_tokens <= 0:
        raise InstanceConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="InstanceConfig max_context_tokens must be greater than 0",
        )

    normalized_context_evict_ratio = normalize_optional_float(
        context_evict_ratio,
        field_name="context_evict_ratio",
    )
    if normalized_context_evict_ratio is not None and not 0 < normalized_context_evict_ratio <= 1:
        raise InstanceConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="InstanceConfig context_evict_ratio must be between 0 and 1",
        )

    normalized_context_compression_max_chars = normalize_optional_int(
        context_compression_max_chars,
        field_name="context_compression_max_chars",
    )
    if (
        normalized_context_compression_max_chars is not None
        and normalized_context_compression_max_chars <= 0
    ):
        raise InstanceConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="InstanceConfig context_compression_max_chars must be greater than 0",
        )

    normalized_explicit_prompt_cache_enabled = normalize_optional_bool(
        explicit_prompt_cache_enabled,
        field_name="explicit_prompt_cache_enabled",
    )

    assign_optional_profile(normalized_config, "response_profile", response_profile)
    assign_optional_profile(
        normalized_config,
        "response_profile_private",
        response_profile_private,
    )
    assign_optional_profile(
        normalized_config,
        "response_profile_priority",
        response_profile_priority,
    )
    assign_optional_profile(
        normalized_config,
        "response_profile_group",
        response_profile_group,
    )
    assign_optional_profile(normalized_config, "media_inspection_llm", media_inspection_llm)
    assign_optional_profile(normalized_config, "media_inspection_prompt", media_inspection_prompt)
    assign_optional_profile(normalized_config, "sticker_summary_llm", sticker_summary_llm)
    assign_optional_profile(normalized_config, "sticker_summary_prompt", sticker_summary_prompt)
    assign_optional_profile(
        normalized_config,
        "context_compression_llm",
        context_compression_llm,
    )
    assign_optional_numeric(
        normalized_config,
        "max_context_tokens",
        normalized_max_context_tokens,
    )
    assign_optional_numeric(
        normalized_config,
        "context_evict_ratio",
        normalized_context_evict_ratio,
    )
    assign_optional_numeric(
        normalized_config,
        "context_compression_max_chars",
        normalized_context_compression_max_chars,
    )
    assign_optional_bool(
        normalized_config,
        "explicit_prompt_cache_enabled",
        normalized_explicit_prompt_cache_enabled,
    )

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    return NormalizedInstanceConfigInput(
        instance_id=normalized_instance_id,
        main_llm=normalized_main_llm,
        config=normalized_config,
        tags=deduped_tags,
    )


def validate_instance_config_references(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    main_llm: str = "",
    config: dict[str, Any] | None = None,
) -> None:
    """Validate that all LLM references in an instance config exist.

    Args:
        bot: The running application.
        boot: The application boot controller.
        instance_id: The instance identifier.
        main_llm: The main LLM target to validate.
        config: Optional config dict with additional LLM targets.

    Raises:
        InstanceConfigAdminError: If the instance or any LLM target is not found.
    """
    if instance_id not in known_instance_ids(bot, boot):
        raise InstanceConfigAdminError(
            status_code=404,
            code="INSTANCE_NOT_FOUND",
            message=f"Instance {instance_id!r} was not found",
        )

    runtime_config = config or {}
    for field_name, target in (
        ("mainLlm", main_llm),
        ("mediaInspectionLlm", str(runtime_config.get("media_inspection_llm") or "")),
        ("stickerSummaryLlm", str(runtime_config.get("sticker_summary_llm") or "")),
        ("contextCompressionLlm", str(runtime_config.get("context_compression_llm") or "")),
    ):
        validate_model_runtime_target(bot.database, field_name, target)


def validate_model_runtime_target(database: Any, field_name: str, target: str) -> None:
    """Validate that a single LLM target reference exists in the model registry.

    Args:
        database: The application database handle.
        field_name: Config field name (used in error messages).
        target: The LLM target string to validate.

    Raises:
        InstanceConfigAdminError: If the target is not found.
    """
    normalized = str(target or "").strip()
    if not normalized:
        return

    registry = database.model_registry
    tagged = parse_tagged_llm_ref(normalized)
    if tagged is not None:
        if tagged.route_id:
            if registry.get_route(tagged.route_id) is not None:
                return
            raise InstanceConfigAdminError(
                status_code=404,
                code="MODEL_TARGET_NOT_FOUND",
                message=(
                    f"InstanceConfig {field_name} route target "
                    f"{tagged.route_id!r} was not found"
                ),
            )
        if tagged.model_id:
            if registry.get_model(tagged.model_id) is not None:
                return
            raise InstanceConfigAdminError(
                status_code=404,
                code="MODEL_TARGET_NOT_FOUND",
                message=(
                    f"InstanceConfig {field_name} model target "
                    f"{tagged.model_id!r} was not found"
                ),
            )
        raise InstanceConfigAdminError(
            status_code=400,
            code="MODEL_TARGET_NOT_FOUND",
            message=f"InstanceConfig {field_name} LLM target {normalized!r} is empty",
        )

    if registry.get_route(normalized) is not None:
        return
    if registry.get_model(normalized) is not None:
        return

    matching_backend_models = [
        item for item in registry.list_models() if item.get("backend_model") == normalized
    ]
    if matching_backend_models:
        model_ids = ", ".join(str(item["id"]) for item in matching_backend_models[:3])
        raise InstanceConfigAdminError(
            status_code=400,
            code="MODEL_TARGET_NOT_FOUND",
            message=(
                f"InstanceConfig {field_name} must reference a Route ID or configured Model ID, "
                f"not LiteLLM model {normalized!r}. Use one of: {model_ids}"
            ),
        )

    raise InstanceConfigAdminError(
        status_code=404,
        code="MODEL_TARGET_NOT_FOUND",
        message=f"InstanceConfig {field_name} model target {normalized!r} was not found",
    )


def get_instance_config_or_raise(database: Any, config_uuid: str) -> dict[str, Any]:
    """Retrieve an instance config by UUID, or raise a 404 error.

    Args:
        database: The application database handle.
        config_uuid: The config UUID to look up.

    Returns:
        The instance config record dict.

    Raises:
        InstanceConfigAdminError: If the config is not found.
    """
    payload = database.instance_configs.get(config_uuid)
    if payload is None:
        raise InstanceConfigAdminError(
            status_code=404,
            code="INSTANCE_CONFIG_NOT_FOUND",
            message=f"InstanceConfig {config_uuid!r} was not found",
        )
    return payload


def assert_instance_config_available(
    database: Any, instance_id: str, *, current_uuid: str | None
) -> None:
    """Assert that no other config already exists for the given instance.

    Args:
        database: The application database handle.
        instance_id: The instance identifier.
        current_uuid: UUID of the config being updated, or ``None`` for creation.

    Raises:
        InstanceConfigAdminError: If a different config already references the instance.
    """
    existing = database.instance_configs.get_by_instance_id(instance_id)
    if existing is not None and existing["uuid"] != current_uuid:
        raise InstanceConfigAdminError(
            status_code=409,
            code="INSTANCE_CONFIG_ALREADY_EXISTS",
            message=f"InstanceConfig for instance {instance_id!r} already exists",
        )


def build_instance_config_record(
    *,
    config_uuid: str | None,
    input_data: NormalizedInstanceConfigInput,
    created_at: str | None = None,
) -> InstanceConfigRecord:
    """Build an ``InstanceConfigRecord`` from normalised input.

    Args:
        config_uuid: Optional UUID; a new one is generated if omitted.
        input_data: Normalised instance config input.
        created_at: Optional override for creation timestamp.

    Returns:
        A new ``InstanceConfigRecord`` instance.
    """
    now = utc_now_iso()
    return InstanceConfigRecord(
        uuid=config_uuid or str(uuid4()),
        instance_id=input_data.instance_id,
        main_llm=input_data.main_llm,
        config=input_data.config,
        tags=input_data.tags,
        created_at=created_at or now,
        updated_at=now,
    )
