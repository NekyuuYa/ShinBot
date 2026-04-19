"""Administrative helpers for persona management flows."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

from shinbot.persistence.records import PersonaRecord, PromptDefinitionRecord, utc_now_iso


@dataclass(slots=True)
class PersonaAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


def serialize_persona(payload: dict[str, object]) -> dict[str, object]:
    return {
        "uuid": payload["uuid"],
        "name": payload["name"],
        "promptDefinitionUuid": payload["prompt_definition_uuid"],
        "promptText": payload["prompt_text"],
        "tags": payload["tags"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def normalize_persona_input(name: str, prompt_text: str) -> tuple[str, str]:
    normalized_name = name.strip()
    normalized_prompt = prompt_text.strip()
    if not normalized_name:
        raise PersonaAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Persona name must not be empty",
        )
    if not normalized_prompt:
        raise PersonaAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Persona promptText must not be empty",
        )
    return normalized_name, normalized_prompt


def normalize_persona_tags(tags: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for tag in tags:
        value = tag.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def build_persona_prompt_definition(
    persona_uuid: str, name: str, prompt_text: str
) -> PromptDefinitionRecord:
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


def get_persona_or_raise(database, persona_uuid: str) -> dict[str, object]:
    payload = database.personas.get(persona_uuid)
    if payload is None:
        raise PersonaAdminError(
            status_code=404,
            code="PERSONA_NOT_FOUND",
            message=f"Persona {persona_uuid!r} was not found",
        )
    return payload


def assert_persona_name_available(database, name: str, *, current_uuid: str | None) -> None:
    existing = database.personas.get_by_name(name)
    if existing is not None and existing["uuid"] != current_uuid:
        raise PersonaAdminError(
            status_code=409,
            code="PERSONA_ALREADY_EXISTS",
            message=f"Persona {name!r} already exists",
        )


def get_persona_prompt_definition_or_raise(database, prompt_definition_uuid: str):
    payload = database.prompt_definitions.get(prompt_definition_uuid)
    if payload is None:
        raise PersonaAdminError(
            status_code=404,
            code="PROMPT_NOT_FOUND",
            message=f"PromptDefinition {prompt_definition_uuid!r} was not found",
        )
    return payload


def build_persona_record(
    *,
    persona_uuid: str | None,
    name: str,
    prompt_definition_uuid: str,
    tags: list[str],
    enabled: bool,
    created_at: str | None = None,
) -> PersonaRecord:
    now = utc_now_iso()
    return PersonaRecord(
        uuid=persona_uuid or str(uuid4()),
        name=name,
        prompt_definition_uuid=prompt_definition_uuid,
        tags=tags,
        enabled=enabled,
        created_at=created_at or now,
        updated_at=now,
    )


def build_updated_persona_prompt_definition(
    *,
    persona_uuid: str,
    name: str,
    prompt_text: str,
    current_payload: dict[str, object],
) -> PromptDefinitionRecord:
    return PromptDefinitionRecord(
        uuid=str(current_payload["uuid"]),
        prompt_id=f"persona.{persona_uuid}",
        name=f"{name} Persona Prompt",
        source_type="persona",
        source_id=persona_uuid,
        stage="identity",
        type="static_text",
        priority=int(current_payload["priority"]),
        version=str(current_payload["version"]),
        description=f"Backing prompt for persona {name}",
        enabled=bool(current_payload["enabled"]),
        content=prompt_text,
        template_vars=list(current_payload["template_vars"]),
        resolver_ref=str(current_payload["resolver_ref"]),
        bundle_refs=list(current_payload["bundle_refs"]),
        config=dict(current_payload["config"]),
        tags=list(current_payload["tags"]),
        metadata=dict(current_payload["metadata"]),
        created_at=str(current_payload["created_at"]),
        updated_at=utc_now_iso(),
    )
