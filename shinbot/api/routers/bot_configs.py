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
    mediaInspectionLlm: str | None = None
    mediaInspectionPrompt: str | None = None
    stickerSummaryLlm: str | None = None
    stickerSummaryPrompt: str | None = None
    contextCompressionLlm: str | None = None
    maxContextTokens: int | None = None
    contextEvictRatio: float | None = None
    contextCompressionMaxChars: int | None = None
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
    mediaInspectionLlm: str | None = None
    mediaInspectionPrompt: str | None = None
    stickerSummaryLlm: str | None = None
    stickerSummaryPrompt: str | None = None
    contextCompressionLlm: str | None = None
    maxContextTokens: int | None = None
    contextEvictRatio: float | None = None
    contextCompressionMaxChars: int | None = None
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


def _patch_value(body: BotConfigPatchRequest, field_name: str, current_value: Any) -> Any:
    if field_name in body.model_fields_set:
        return getattr(body, field_name)
    return current_value


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
            media_inspection_llm=body.mediaInspectionLlm,
            media_inspection_prompt=body.mediaInspectionPrompt,
            sticker_summary_llm=body.stickerSummaryLlm,
            sticker_summary_prompt=body.stickerSummaryPrompt,
            context_compression_llm=body.contextCompressionLlm,
            max_context_tokens=body.maxContextTokens,
            context_evict_ratio=body.contextEvictRatio,
            context_compression_max_chars=body.contextCompressionMaxChars,
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
        current_config = dict(current["config"])
        normalized = normalize_bot_config_input(
            instance_id=str(_patch_value(body, "instanceId", current["instance_id"]) or ""),
            default_agent_uuid=str(
                _patch_value(body, "defaultAgentUuid", current["default_agent_uuid"]) or ""
            ),
            main_llm=str(_patch_value(body, "mainLlm", current["main_llm"]) or ""),
            media_inspection_llm=_patch_value(
                body,
                "mediaInspectionLlm",
                current_config.get("media_inspection_llm"),
            ),
            media_inspection_prompt=_patch_value(
                body,
                "mediaInspectionPrompt",
                current_config.get("media_inspection_prompt"),
            ),
            sticker_summary_llm=_patch_value(
                body,
                "stickerSummaryLlm",
                current_config.get("sticker_summary_llm"),
            ),
            sticker_summary_prompt=_patch_value(
                body,
                "stickerSummaryPrompt",
                current_config.get("sticker_summary_prompt"),
            ),
            context_compression_llm=_patch_value(
                body,
                "contextCompressionLlm",
                current_config.get("context_compression_llm"),
            ),
            max_context_tokens=_patch_value(
                body,
                "maxContextTokens",
                current_config.get("max_context_tokens"),
            ),
            context_evict_ratio=_patch_value(
                body,
                "contextEvictRatio",
                current_config.get("context_evict_ratio"),
            ),
            context_compression_max_chars=_patch_value(
                body,
                "contextCompressionMaxChars",
                current_config.get("context_compression_max_chars"),
            ),
            response_profile=_patch_value(
                body,
                "responseProfile",
                current_config.get("response_profile"),
            ),
            response_profile_private=_patch_value(
                body,
                "responseProfilePrivate",
                current_config.get("response_profile_private"),
            ),
            response_profile_priority=_patch_value(
                body,
                "responseProfilePriority",
                current_config.get("response_profile_priority"),
            ),
            response_profile_group=_patch_value(
                body,
                "responseProfileGroup",
                current_config.get("response_profile_group"),
            ),
            config=_patch_value(body, "config", current_config) or {},
            tags=_patch_value(body, "tags", list(current["tags"])) or [],
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
