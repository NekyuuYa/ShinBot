"""Persona management router: /api/v1/personas"""

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import EC, ok
from shinbot.persistence.records import PersonaRecord, PromptDefinitionRecord, utc_now_iso

router = APIRouter(
    prefix="/personas",
    tags=["personas"],
    dependencies=AuthRequired,
)


class PersonaRequest(BaseModel):
    name: str
    promptText: str
    enabled: bool = True


class PersonaPatchRequest(BaseModel):
    name: str | None = None
    promptText: str | None = None
    enabled: bool | None = None


def _serialize_persona(payload: dict[str, object]) -> dict[str, object]:
    return {
        "uuid": payload["uuid"],
        "name": payload["name"],
        "promptDefinitionUuid": payload["prompt_definition_uuid"],
        "promptText": payload["prompt_text"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _normalize_persona_input(name: str, prompt_text: str) -> tuple[str, str]:
    normalized_name = name.strip()
    normalized_prompt = prompt_text.strip()
    if not normalized_name:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Persona name must not be empty"},
        )
    if not normalized_prompt:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Persona promptText must not be empty"},
        )
    return normalized_name, normalized_prompt


def _persona_prompt_definition(persona_uuid: str, name: str, prompt_text: str) -> PromptDefinitionRecord:
    return PromptDefinitionRecord(
        uuid=str(uuid4()),
        prompt_id=f"persona.{persona_uuid}",
        name=f"{name} Persona Prompt",
        source_type="persona",
        source_id=persona_uuid,
        stage="identity",
        type="static_text",
        priority=100,
        description=f"Backing prompt for persona {name}",
        content=prompt_text,
        metadata={},
    )


@router.get("")
def list_personas(bot=BotDep):
    return ok([_serialize_persona(item) for item in bot.database.personas.list()])


@router.post("", status_code=201)
def create_persona(body: PersonaRequest, bot=BotDep):
    name, prompt_text = _normalize_persona_input(body.name, body.promptText)
    if bot.database.personas.get_by_name(name) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.PERSONA_ALREADY_EXISTS,
                "message": f"Persona {name!r} already exists",
            },
        )

    now = utc_now_iso()
    persona_uuid = str(uuid4())
    prompt_definition = _persona_prompt_definition(persona_uuid, name, prompt_text)
    prompt_definition.created_at = now
    prompt_definition.updated_at = now
    bot.database.prompt_definitions.upsert(prompt_definition)

    record = PersonaRecord(
        uuid=persona_uuid,
        name=name,
        prompt_definition_uuid=prompt_definition.uuid,
        enabled=body.enabled,
        created_at=now,
        updated_at=now,
    )
    bot.database.personas.upsert(record)
    payload = bot.database.personas.get(record.uuid)
    assert payload is not None
    return ok(_serialize_persona(payload))


@router.get("/{persona_uuid}")
def get_persona(persona_uuid: str, bot=BotDep):
    payload = bot.database.personas.get(persona_uuid)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PERSONA_NOT_FOUND,
                "message": f"Persona {persona_uuid!r} was not found",
            },
        )
    return ok(_serialize_persona(payload))


@router.patch("/{persona_uuid}")
def patch_persona(persona_uuid: str, body: PersonaPatchRequest, bot=BotDep):
    current = bot.database.personas.get(persona_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PERSONA_NOT_FOUND,
                "message": f"Persona {persona_uuid!r} was not found",
            },
        )

    next_name = body.name if body.name is not None else str(current["name"])
    next_prompt_text = (
        body.promptText if body.promptText is not None else str(current["prompt_text"] or "")
    )
    normalized_name, normalized_prompt = _normalize_persona_input(next_name, next_prompt_text)

    existing = bot.database.personas.get_by_name(normalized_name)
    if existing is not None and existing["uuid"] != persona_uuid:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.PERSONA_ALREADY_EXISTS,
                "message": f"Persona {normalized_name!r} already exists",
            },
        )

    prompt_definition_uuid = str(current["prompt_definition_uuid"])
    prompt_definition_payload = bot.database.prompt_definitions.get(prompt_definition_uuid)
    if prompt_definition_payload is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PROMPT_NOT_FOUND,
                "message": f"PromptDefinition {prompt_definition_uuid!r} was not found",
            },
        )

    bot.database.prompt_definitions.upsert(
        PromptDefinitionRecord(
            uuid=prompt_definition_uuid,
            prompt_id=f"persona.{persona_uuid}",
            name=f"{normalized_name} Persona Prompt",
            source_type="persona",
            source_id=persona_uuid,
            stage="identity",
            type="static_text",
            priority=int(prompt_definition_payload["priority"]),
            version=str(prompt_definition_payload["version"]),
            description=f"Backing prompt for persona {normalized_name}",
            enabled=bool(prompt_definition_payload["enabled"]),
            content=normalized_prompt,
            template_vars=list(prompt_definition_payload["template_vars"]),
            resolver_ref=str(prompt_definition_payload["resolver_ref"]),
            bundle_refs=list(prompt_definition_payload["bundle_refs"]),
            config=dict(prompt_definition_payload["config"]),
            tags=list(prompt_definition_payload["tags"]),
            metadata=dict(prompt_definition_payload["metadata"]),
            created_at=str(prompt_definition_payload["created_at"]),
            updated_at=utc_now_iso(),
        )
    )

    bot.database.personas.upsert(
        PersonaRecord(
            uuid=persona_uuid,
            name=normalized_name,
            prompt_definition_uuid=prompt_definition_uuid,
            enabled=body.enabled if body.enabled is not None else bool(current["enabled"]),
            created_at=str(current["created_at"]),
            updated_at=utc_now_iso(),
        )
    )
    payload = bot.database.personas.get(persona_uuid)
    assert payload is not None
    return ok(_serialize_persona(payload))


@router.delete("/{persona_uuid}")
def delete_persona(persona_uuid: str, bot=BotDep):
    current = bot.database.personas.get(persona_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PERSONA_NOT_FOUND,
                "message": f"Persona {persona_uuid!r} was not found",
            },
        )
    prompt_definition_uuid = str(current["prompt_definition_uuid"])
    bot.database.personas.delete(persona_uuid)
    bot.database.prompt_definitions.delete(prompt_definition_uuid)
    return ok({"deleted": True, "uuid": persona_uuid})
