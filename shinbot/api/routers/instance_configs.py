"""Instance config management router: /api/v1/instance-configs"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.instance_config_admin import (
    InstanceConfigAdminError,
    assert_instance_config_available,
    build_instance_config_record,
    get_instance_config_or_raise,
    normalize_instance_config_input,
    serialize_instance_config,
    validate_instance_config_references,
)
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import Envelope, ok

router = APIRouter(
    prefix="/instance-configs",
    tags=["instance-configs"],
    dependencies=AuthRequired,
)


class InstanceConfigRequest(BaseModel):
    instanceId: str
    mainLlm: str = ""
    explicitPromptCacheEnabled: bool | None = None
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


class InstanceConfigPatchRequest(BaseModel):
    instanceId: str | None = None
    mainLlm: str | None = None
    explicitPromptCacheEnabled: bool | None = None
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


class InstanceConfigData(BaseModel):
    """Response data model for a single instance configuration."""

    uuid: str
    instanceId: str
    mainLlm: str
    explicitPromptCacheEnabled: bool
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
    config: dict[str, Any]
    tags: list[str]
    createdAt: str
    lastModified: str


class InstanceConfigDeletedData(BaseModel):
    """Response data model for instance config deletion confirmation."""

    deleted: bool
    uuid: str


def _raise_admin_http_error(exc: InstanceConfigAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


def _patch_value(body: InstanceConfigPatchRequest, field_name: str, current_value: Any) -> Any:
    if field_name in body.model_fields_set:
        return getattr(body, field_name)
    return current_value


@router.get("", response_model=Envelope[list[InstanceConfigData]])
def list_instance_configs(bot=BotDep):
    """List all instance configuration records."""
    return ok(
        [serialize_instance_config(item) for item in bot.database.instance_configs.list()]
    )


@router.post("", status_code=201, response_model=Envelope[InstanceConfigData])
def create_instance_config(body: InstanceConfigRequest, bot=BotDep, boot=BootDep):
    """Create a new instance configuration, validating references beforehand."""
    try:
        normalized = normalize_instance_config_input(
            instance_id=body.instanceId,
            main_llm=body.mainLlm,
            explicit_prompt_cache_enabled=body.explicitPromptCacheEnabled,
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
        assert_instance_config_available(
            bot.database,
            normalized.instance_id,
            current_uuid=None,
        )
        validate_instance_config_references(
            bot=bot,
            boot=boot,
            instance_id=normalized.instance_id,
            main_llm=normalized.main_llm,
            config=normalized.config,
        )
    except InstanceConfigAdminError as exc:
        _raise_admin_http_error(exc)

    record = build_instance_config_record(config_uuid=None, input_data=normalized)
    bot.database.instance_configs.upsert(record)
    payload = bot.database.instance_configs.get(record.uuid)
    assert payload is not None
    return ok(serialize_instance_config(payload))


@router.get("/{config_uuid}", response_model=Envelope[InstanceConfigData])
def get_instance_config(config_uuid: str, bot=BotDep):
    """Retrieve a single instance configuration by its UUID."""
    try:
        payload = get_instance_config_or_raise(bot.database, config_uuid)
    except InstanceConfigAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_instance_config(payload))


@router.patch("/{config_uuid}", response_model=Envelope[InstanceConfigData])
def patch_instance_config(
    config_uuid: str,
    body: InstanceConfigPatchRequest,
    bot=BotDep,
    boot=BootDep,
):
    """Partially update an existing instance configuration."""
    try:
        current = get_instance_config_or_raise(bot.database, config_uuid)
        current_config = dict(current["config"])
        normalized = normalize_instance_config_input(
            instance_id=str(_patch_value(body, "instanceId", current["instance_id"]) or ""),
            main_llm=str(_patch_value(body, "mainLlm", current["main_llm"]) or ""),
            explicit_prompt_cache_enabled=_patch_value(
                body,
                "explicitPromptCacheEnabled",
                current_config.get("explicit_prompt_cache_enabled"),
            ),
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
        assert_instance_config_available(
            bot.database,
            normalized.instance_id,
            current_uuid=config_uuid,
        )
        validate_instance_config_references(
            bot=bot,
            boot=boot,
            instance_id=normalized.instance_id,
            main_llm=normalized.main_llm,
            config=normalized.config,
        )
    except InstanceConfigAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.instance_configs.upsert(
        build_instance_config_record(
            config_uuid=config_uuid,
            input_data=normalized,
            created_at=str(current["created_at"]),
        )
    )
    payload = bot.database.instance_configs.get(config_uuid)
    assert payload is not None
    return ok(serialize_instance_config(payload))


@router.delete("/{config_uuid}", response_model=Envelope[InstanceConfigDeletedData])
def delete_instance_config(config_uuid: str, bot=BotDep):
    """Delete an instance configuration by its UUID."""
    try:
        get_instance_config_or_raise(bot.database, config_uuid)
    except InstanceConfigAdminError as exc:
        _raise_admin_http_error(exc)
    bot.database.instance_configs.delete(config_uuid)
    return ok({"deleted": True, "uuid": config_uuid})
