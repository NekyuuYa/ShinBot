"""Persona management router: /api/v1/personas"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import ok
from shinbot.core.persona_admin import (
    PersonaAdminError,
    assert_persona_name_available,
    build_persona_prompt_definition,
    build_persona_record,
    build_updated_persona_prompt_definition,
    get_persona_or_raise,
    get_persona_prompt_definition_or_raise,
    normalize_persona_input,
    normalize_persona_tags,
    serialize_persona,
)

router = APIRouter(
    prefix="/personas",
    tags=["personas"],
    dependencies=AuthRequired,
)


class PersonaRequest(BaseModel):
    name: str
    promptText: str
    tags: list[str] = Field(default_factory=list)
    enabled: bool = True


class PersonaPatchRequest(BaseModel):
    name: str | None = None
    promptText: str | None = None
    tags: list[str] | None = None
    enabled: bool | None = None


def _raise_admin_http_error(exc: PersonaAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("")
def list_personas(bot=BotDep):
    return ok([serialize_persona(item) for item in bot.database.personas.list()])


@router.post("", status_code=201)
def create_persona(body: PersonaRequest, bot=BotDep):
    try:
        name, prompt_text = normalize_persona_input(body.name, body.promptText)
        tags = normalize_persona_tags(body.tags)
        assert_persona_name_available(bot.database, name, current_uuid=None)
        record = build_persona_record(
            persona_uuid=None,
            name=name,
            prompt_definition_uuid="",
            tags=tags,
            enabled=body.enabled,
        )
        prompt_definition = build_persona_prompt_definition(record.uuid, name, prompt_text)
        prompt_definition.created_at = record.created_at
        prompt_definition.updated_at = record.updated_at
        record.prompt_definition_uuid = prompt_definition.uuid
    except PersonaAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.prompt_definitions.upsert(prompt_definition)
    bot.database.personas.upsert(record)
    payload = bot.database.personas.get(record.uuid)
    assert payload is not None
    return ok(serialize_persona(payload))


@router.get("/{persona_uuid}")
def get_persona(persona_uuid: str, bot=BotDep):
    try:
        payload = get_persona_or_raise(bot.database, persona_uuid)
    except PersonaAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_persona(payload))


@router.patch("/{persona_uuid}")
def patch_persona(persona_uuid: str, body: PersonaPatchRequest, bot=BotDep):
    try:
        current = get_persona_or_raise(bot.database, persona_uuid)
        next_name = body.name if body.name is not None else str(current["name"])
        next_prompt_text = (
            body.promptText if body.promptText is not None else str(current["prompt_text"] or "")
        )
        next_tags = body.tags if body.tags is not None else list(current["tags"])
        normalized_name, normalized_prompt = normalize_persona_input(next_name, next_prompt_text)
        normalized_tags = normalize_persona_tags(next_tags)
        assert_persona_name_available(bot.database, normalized_name, current_uuid=persona_uuid)
        prompt_definition_uuid = str(current["prompt_definition_uuid"])
        prompt_definition_payload = get_persona_prompt_definition_or_raise(
            bot.database,
            prompt_definition_uuid,
        )
        prompt_definition = build_updated_persona_prompt_definition(
            persona_uuid=persona_uuid,
            name=normalized_name,
            prompt_text=normalized_prompt,
            current_payload=prompt_definition_payload,
        )
        record = build_persona_record(
            persona_uuid=persona_uuid,
            name=normalized_name,
            prompt_definition_uuid=prompt_definition_uuid,
            tags=normalized_tags,
            enabled=body.enabled if body.enabled is not None else bool(current["enabled"]),
            created_at=str(current["created_at"]),
        )
    except PersonaAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.prompt_definitions.upsert(prompt_definition)
    bot.database.personas.upsert(record)
    payload = bot.database.personas.get(persona_uuid)
    assert payload is not None
    return ok(serialize_persona(payload))


@router.delete("/{persona_uuid}")
def delete_persona(persona_uuid: str, bot=BotDep):
    try:
        current = get_persona_or_raise(bot.database, persona_uuid)
    except PersonaAdminError as exc:
        _raise_admin_http_error(exc)
    prompt_definition_uuid = str(current["prompt_definition_uuid"])
    bot.database.personas.delete(persona_uuid)
    bot.database.prompt_definitions.delete(prompt_definition_uuid)
    return ok({"deleted": True, "uuid": persona_uuid})
