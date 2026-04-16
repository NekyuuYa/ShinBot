"""Bot config management router: /api/v1/bot-configs"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, ok
from shinbot.persistence.records import BotConfigRecord, utc_now_iso

router = APIRouter(
    prefix="/bot-configs",
    tags=["bot-configs"],
    dependencies=AuthRequired,
)


class BotConfigRequest(BaseModel):
    instanceId: str
    defaultAgentUuid: str = ""
    mainLlm: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class BotConfigPatchRequest(BaseModel):
    instanceId: str | None = None
    defaultAgentUuid: str | None = None
    mainLlm: str | None = None
    config: dict[str, Any] | None = None
    tags: list[str] | None = None


def _serialize_bot_config(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "uuid": payload["uuid"],
        "instanceId": payload["instance_id"],
        "defaultAgentUuid": payload["default_agent_uuid"],
        "mainLlm": payload["main_llm"],
        "config": payload["config"],
        "tags": payload["tags"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _known_instance_ids(bot: Any, boot: Any) -> set[str]:
    ids = {str(item.get("id")) for item in boot.config.get("instances", []) if item.get("id")}
    ids.update(adapter.instance_id for adapter in bot.adapter_manager.all_instances)
    return ids


def _normalize_bot_config_input(
    *,
    instance_id: str,
    default_agent_uuid: str,
    main_llm: str,
    config: dict[str, Any],
    tags: list[str],
) -> tuple[str, str, str, dict[str, Any], list[str]]:
    normalized_instance_id = instance_id.strip()
    normalized_default_agent_uuid = default_agent_uuid.strip()
    normalized_main_llm = main_llm.strip()
    normalized_tags = [tag.strip() for tag in tags if tag.strip()]

    if not normalized_instance_id:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "BotConfig instanceId must not be empty"},
        )

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    return (
        normalized_instance_id,
        normalized_default_agent_uuid,
        normalized_main_llm,
        dict(config),
        deduped_tags,
    )


def _validate_bot_config_references(
    *,
    bot: Any,
    boot: Any,
    instance_id: str,
    default_agent_uuid: str,
) -> None:
    if instance_id not in _known_instance_ids(bot, boot):
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.INSTANCE_NOT_FOUND,
                "message": f"Instance {instance_id!r} was not found",
            },
        )
    if default_agent_uuid and bot.database.agents.get(default_agent_uuid) is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.AGENT_NOT_FOUND,
                "message": f"Agent {default_agent_uuid!r} was not found",
            },
        )


@router.get("")
def list_bot_configs(bot=BotDep):
    return ok([_serialize_bot_config(item) for item in bot.database.bot_configs.list()])


@router.post("", status_code=201)
def create_bot_config(body: BotConfigRequest, bot=BotDep, boot=BootDep):
    instance_id, default_agent_uuid, main_llm, config, tags = _normalize_bot_config_input(
        instance_id=body.instanceId,
        default_agent_uuid=body.defaultAgentUuid,
        main_llm=body.mainLlm,
        config=body.config,
        tags=body.tags,
    )
    if bot.database.bot_configs.get_by_instance_id(instance_id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.BOT_CONFIG_ALREADY_EXISTS,
                "message": f"BotConfig for instance {instance_id!r} already exists",
            },
        )
    _validate_bot_config_references(
        bot=bot,
        boot=boot,
        instance_id=instance_id,
        default_agent_uuid=default_agent_uuid,
    )

    now = utc_now_iso()
    record = BotConfigRecord(
        uuid=str(uuid4()),
        instance_id=instance_id,
        default_agent_uuid=default_agent_uuid,
        main_llm=main_llm,
        config=config,
        tags=tags,
        created_at=now,
        updated_at=now,
    )
    bot.database.bot_configs.upsert(record)
    payload = bot.database.bot_configs.get(record.uuid)
    assert payload is not None
    return ok(_serialize_bot_config(payload))


@router.get("/{config_uuid}")
def get_bot_config(config_uuid: str, bot=BotDep):
    payload = bot.database.bot_configs.get(config_uuid)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.BOT_CONFIG_NOT_FOUND,
                "message": f"BotConfig {config_uuid!r} was not found",
            },
        )
    return ok(_serialize_bot_config(payload))


@router.patch("/{config_uuid}")
def patch_bot_config(config_uuid: str, body: BotConfigPatchRequest, bot=BotDep, boot=BootDep):
    current = bot.database.bot_configs.get(config_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.BOT_CONFIG_NOT_FOUND,
                "message": f"BotConfig {config_uuid!r} was not found",
            },
        )

    instance_id, default_agent_uuid, main_llm, config, tags = _normalize_bot_config_input(
        instance_id=body.instanceId if body.instanceId is not None else str(current["instance_id"]),
        default_agent_uuid=(
            body.defaultAgentUuid
            if body.defaultAgentUuid is not None
            else str(current["default_agent_uuid"])
        ),
        main_llm=body.mainLlm if body.mainLlm is not None else str(current["main_llm"]),
        config=body.config if body.config is not None else dict(current["config"]),
        tags=body.tags if body.tags is not None else list(current["tags"]),
    )

    existing = bot.database.bot_configs.get_by_instance_id(instance_id)
    if existing is not None and existing["uuid"] != config_uuid:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.BOT_CONFIG_ALREADY_EXISTS,
                "message": f"BotConfig for instance {instance_id!r} already exists",
            },
        )
    _validate_bot_config_references(
        bot=bot,
        boot=boot,
        instance_id=instance_id,
        default_agent_uuid=default_agent_uuid,
    )

    bot.database.bot_configs.upsert(
        BotConfigRecord(
            uuid=config_uuid,
            instance_id=instance_id,
            default_agent_uuid=default_agent_uuid,
            main_llm=main_llm,
            config=config,
            tags=tags,
            created_at=str(current["created_at"]),
            updated_at=utc_now_iso(),
        )
    )
    payload = bot.database.bot_configs.get(config_uuid)
    assert payload is not None
    return ok(_serialize_bot_config(payload))


@router.delete("/{config_uuid}")
def delete_bot_config(config_uuid: str, bot=BotDep):
    current = bot.database.bot_configs.get(config_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.BOT_CONFIG_NOT_FOUND,
                "message": f"BotConfig {config_uuid!r} was not found",
            },
        )
    bot.database.bot_configs.delete(config_uuid)
    return ok({"deleted": True, "uuid": config_uuid})
