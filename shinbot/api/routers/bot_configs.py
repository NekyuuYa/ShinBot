"""Bot config management router: /api/v1/bot-configs"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import ok
from shinbot.core.bot_config_admin import (
    BotConfigAdminError,
    assert_bot_config_instance_available,
    build_bot_config_record,
    get_bot_config_or_raise,
    normalize_bot_config_input,
    serialize_bot_config,
    validate_bot_config_references,
)

router = APIRouter(
    prefix="/bot-configs",
    tags=["bot-configs"],
    dependencies=AuthRequired,
)


class BotConfigRequest(BaseModel):
    instanceId: str
    defaultAgentUuid: str = ""
    mainLlm: str = ""
    responseProfile: str | None = None
    responseProfilePrivate: str | None = None
    responseProfilePriority: str | None = None
    responseProfileGroup: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class BotConfigPatchRequest(BaseModel):
    instanceId: str | None = None
    defaultAgentUuid: str | None = None
    mainLlm: str | None = None
    responseProfile: str | None = None
    responseProfilePrivate: str | None = None
    responseProfilePriority: str | None = None
    responseProfileGroup: str | None = None
    config: dict[str, Any] | None = None
    tags: list[str] | None = None


def _raise_admin_http_error(exc: BotConfigAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("")
def list_bot_configs(bot=BotDep):
    return ok([serialize_bot_config(item) for item in bot.database.bot_configs.list()])


@router.post("", status_code=201)
def create_bot_config(body: BotConfigRequest, bot=BotDep, boot=BootDep):
    try:
        normalized = normalize_bot_config_input(
            instance_id=body.instanceId,
            default_agent_uuid=body.defaultAgentUuid,
            main_llm=body.mainLlm,
            response_profile=body.responseProfile,
            response_profile_private=body.responseProfilePrivate,
            response_profile_priority=body.responseProfilePriority,
            response_profile_group=body.responseProfileGroup,
            config=body.config,
            tags=body.tags,
        )
        assert_bot_config_instance_available(
            bot.database,
            normalized.instance_id,
            current_uuid=None,
        )
        validate_bot_config_references(
            bot=bot,
            boot=boot,
            instance_id=normalized.instance_id,
            default_agent_uuid=normalized.default_agent_uuid,
        )
    except BotConfigAdminError as exc:
        _raise_admin_http_error(exc)

    record = build_bot_config_record(config_uuid=None, input_data=normalized)
    bot.database.bot_configs.upsert(record)
    payload = bot.database.bot_configs.get(record.uuid)
    assert payload is not None
    return ok(serialize_bot_config(payload))


@router.get("/{config_uuid}")
def get_bot_config(config_uuid: str, bot=BotDep):
    try:
        payload = get_bot_config_or_raise(bot.database, config_uuid)
    except BotConfigAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_bot_config(payload))


@router.patch("/{config_uuid}")
def patch_bot_config(config_uuid: str, body: BotConfigPatchRequest, bot=BotDep, boot=BootDep):
    try:
        current = get_bot_config_or_raise(bot.database, config_uuid)
        normalized = normalize_bot_config_input(
            instance_id=(
                body.instanceId if body.instanceId is not None else str(current["instance_id"])
            ),
            default_agent_uuid=(
                body.defaultAgentUuid
                if body.defaultAgentUuid is not None
                else str(current["default_agent_uuid"])
            ),
            main_llm=body.mainLlm if body.mainLlm is not None else str(current["main_llm"]),
            response_profile=(
                body.responseProfile
                if body.responseProfile is not None
                else dict(current["config"]).get("response_profile")
            ),
            response_profile_private=(
                body.responseProfilePrivate
                if body.responseProfilePrivate is not None
                else dict(current["config"]).get("response_profile_private")
            ),
            response_profile_priority=(
                body.responseProfilePriority
                if body.responseProfilePriority is not None
                else dict(current["config"]).get("response_profile_priority")
            ),
            response_profile_group=(
                body.responseProfileGroup
                if body.responseProfileGroup is not None
                else dict(current["config"]).get("response_profile_group")
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            tags=body.tags if body.tags is not None else list(current["tags"]),
        )
        assert_bot_config_instance_available(
            bot.database,
            normalized.instance_id,
            current_uuid=config_uuid,
        )
        validate_bot_config_references(
            bot=bot,
            boot=boot,
            instance_id=normalized.instance_id,
            default_agent_uuid=normalized.default_agent_uuid,
        )
    except BotConfigAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.bot_configs.upsert(
        build_bot_config_record(
            config_uuid=config_uuid,
            input_data=normalized,
            created_at=str(current["created_at"]),
        )
    )
    payload = bot.database.bot_configs.get(config_uuid)
    assert payload is not None
    return ok(serialize_bot_config(payload))


@router.delete("/{config_uuid}")
def delete_bot_config(config_uuid: str, bot=BotDep):
    try:
        get_bot_config_or_raise(bot.database, config_uuid)
    except BotConfigAdminError as exc:
        _raise_admin_http_error(exc)
    bot.database.bot_configs.delete(config_uuid)
    return ok({"deleted": True, "uuid": config_uuid})
