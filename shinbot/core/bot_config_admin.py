"""Administrative helpers for bot-config management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from shinbot.persistence.records import BotConfigRecord, utc_now_iso


@dataclass(slots=True)
class BotConfigAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class NormalizedBotConfigInput:
    instance_id: str
    default_agent_uuid: str
    main_llm: str
    config: dict[str, Any]
    tags: list[str]


def normalize_optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def assign_optional_profile(config: dict[str, Any], key: str, value: Any) -> None:
    normalized = normalize_optional_string(value)
    if normalized is None:
        return
    config[key] = normalized


def extract_response_profiles(config: dict[str, Any]) -> dict[str, str | None]:
    return {
        "responseProfile": normalize_optional_string(config.get("response_profile")),
        "responseProfilePrivate": normalize_optional_string(config.get("response_profile_private")),
        "responseProfilePriority": normalize_optional_string(
            config.get("response_profile_priority")
        ),
        "responseProfileGroup": normalize_optional_string(config.get("response_profile_group")),
    }


def strip_response_profiles(config: dict[str, Any]) -> dict[str, Any]:
    cleaned = dict(config)
    cleaned.pop("response_profile", None)
    cleaned.pop("response_profile_private", None)
    cleaned.pop("response_profile_priority", None)
    cleaned.pop("response_profile_group", None)
    return cleaned


def serialize_bot_config(payload: dict[str, Any]) -> dict[str, Any]:
    config = dict(payload["config"])
    return {
        "uuid": payload["uuid"],
        "instanceId": payload["instance_id"],
        "defaultAgentUuid": payload["default_agent_uuid"],
        "mainLlm": payload["main_llm"],
        **extract_response_profiles(config),
        "config": strip_response_profiles(config),
        "tags": payload["tags"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def known_instance_ids(bot: Any, boot: Any) -> set[str]:
    ids = {str(item.get("id")) for item in boot.config.get("instances", []) if item.get("id")}
    ids.update(adapter.instance_id for adapter in bot.adapter_manager.all_instances)
    return ids


def normalize_bot_config_input(
    *,
    instance_id: str,
    default_agent_uuid: str,
    main_llm: str,
    response_profile: str | None,
    response_profile_private: str | None,
    response_profile_priority: str | None,
    response_profile_group: str | None,
    config: dict[str, Any],
    tags: list[str],
) -> NormalizedBotConfigInput:
    normalized_instance_id = instance_id.strip()
    normalized_default_agent_uuid = default_agent_uuid.strip()
    normalized_main_llm = main_llm.strip()
    normalized_tags = [tag.strip() for tag in tags if tag.strip()]
    normalized_config = dict(config)

    if not normalized_instance_id:
        raise BotConfigAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="BotConfig instanceId must not be empty",
        )

    normalized_config.pop("response_profile", None)
    normalized_config.pop("response_profile_private", None)
    normalized_config.pop("response_profile_priority", None)
    normalized_config.pop("response_profile_group", None)

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

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    return NormalizedBotConfigInput(
        instance_id=normalized_instance_id,
        default_agent_uuid=normalized_default_agent_uuid,
        main_llm=normalized_main_llm,
        config=normalized_config,
        tags=deduped_tags,
    )


def validate_bot_config_references(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    default_agent_uuid: str,
) -> None:
    if instance_id not in known_instance_ids(bot, boot):
        raise BotConfigAdminError(
            status_code=404,
            code="INSTANCE_NOT_FOUND",
            message=f"Instance {instance_id!r} was not found",
        )
    if default_agent_uuid and bot.database.agents.get(default_agent_uuid) is None:
        raise BotConfigAdminError(
            status_code=404,
            code="AGENT_NOT_FOUND",
            message=f"Agent {default_agent_uuid!r} was not found",
        )


def get_bot_config_or_raise(database: Any, config_uuid: str) -> dict[str, Any]:
    payload = database.bot_configs.get(config_uuid)
    if payload is None:
        raise BotConfigAdminError(
            status_code=404,
            code="BOT_CONFIG_NOT_FOUND",
            message=f"BotConfig {config_uuid!r} was not found",
        )
    return payload


def assert_bot_config_instance_available(database: Any, instance_id: str, *, current_uuid: str | None) -> None:
    existing = database.bot_configs.get_by_instance_id(instance_id)
    if existing is not None and existing["uuid"] != current_uuid:
        raise BotConfigAdminError(
            status_code=409,
            code="BOT_CONFIG_ALREADY_EXISTS",
            message=f"BotConfig for instance {instance_id!r} already exists",
        )


def build_bot_config_record(
    *,
    config_uuid: str | None,
    input_data: NormalizedBotConfigInput,
    created_at: str | None = None,
) -> BotConfigRecord:
    now = utc_now_iso()
    return BotConfigRecord(
        uuid=config_uuid or str(uuid4()),
        instance_id=input_data.instance_id,
        default_agent_uuid=input_data.default_agent_uuid,
        main_llm=input_data.main_llm,
        config=input_data.config,
        tags=input_data.tags,
        created_at=created_at or now,
        updated_at=now,
    )
