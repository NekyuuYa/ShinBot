"""Persona management router: /api/v1/personas"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.persona_files import (
    FewShotExample,
    PersonaFileError,
    PersonaFileRepository,
    serialize_persona,
)
from shinbot.api.deps import AuthRequired, BootDep
from shinbot.api.models import Envelope, ok

router = APIRouter(
    prefix="/personas",
    tags=["personas"],
    dependencies=AuthRequired,
)


class FewShotExampleRequest(BaseModel):
    user: str
    assistant: str


class PersonaRequest(BaseModel):
    id: str | None = None
    name: str
    promptText: str
    tags: list[str] = Field(default_factory=list)
    enabled: bool = True
    fewShotExamples: list[FewShotExampleRequest] = Field(default_factory=list)


class PersonaPatchRequest(BaseModel):
    name: str | None = None
    promptText: str | None = None
    tags: list[str] | None = None
    enabled: bool | None = None
    fewShotExamples: list[FewShotExampleRequest] | None = None


class PersonaData(BaseModel):
    """Response data model for a single persona."""

    uuid: str
    name: str
    promptText: str
    tags: list[str]
    enabled: bool
    createdAt: str
    lastModified: str
    fewShotExamples: list[FewShotExampleRequest] = Field(default_factory=list)


class PersonaDeletedData(BaseModel):
    """Response data model for persona deletion confirmation."""

    deleted: bool
    uuid: str


def _persona_repository(boot: Any) -> PersonaFileRepository:
    return PersonaFileRepository.from_data_dir(boot.data_dir)


def _raise_admin_http_error(exc: PersonaFileError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("", response_model=Envelope[list[PersonaData]])
def list_personas(boot: Any = BootDep) -> dict[str, Any]:
    """List all personas stored on disk."""
    try:
        return ok([serialize_persona(item) for item in _persona_repository(boot).list()])
    except PersonaFileError as exc:
        _raise_admin_http_error(exc)


@router.post("", status_code=201, response_model=Envelope[PersonaData])
def create_persona(body: PersonaRequest, boot: Any = BootDep) -> dict[str, Any]:
    """Create a new persona with the given name, prompt text, and tags."""
    try:
        few_shot = [
            FewShotExample(user=ex.user, assistant=ex.assistant)
            for ex in body.fewShotExamples
        ]
        payload = _persona_repository(boot).create(
            persona_id=body.id,
            name=body.name,
            prompt_text=body.promptText,
            tags=body.tags,
            enabled=body.enabled,
            few_shot=few_shot or None,
        )
    except PersonaFileError as exc:
        _raise_admin_http_error(exc)

    return ok(serialize_persona(payload))


@router.get("/{persona_uuid}", response_model=Envelope[PersonaData])
def get_persona(persona_uuid: str, boot: Any = BootDep) -> dict[str, Any]:
    """Retrieve a single persona by its UUID."""
    try:
        payload = _persona_repository(boot).get(persona_uuid)
        if payload is None:
            raise PersonaFileError(
                status_code=404,
                code="PERSONA_NOT_FOUND",
                message=f"Persona {persona_uuid!r} was not found",
            )
    except PersonaFileError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_persona(payload))


@router.patch("/{persona_uuid}", response_model=Envelope[PersonaData])
def patch_persona(persona_uuid: str, body: PersonaPatchRequest, boot: Any = BootDep) -> dict[str, Any]:
    """Partially update an existing persona's fields."""
    try:
        repository = _persona_repository(boot)
        current = repository.get(persona_uuid)
        if current is None:
            raise PersonaFileError(
                status_code=404,
                code="PERSONA_NOT_FOUND",
                message=f"Persona {persona_uuid!r} was not found",
            )
        next_name = body.name if body.name is not None else str(current["name"])
        next_prompt_text = (
            body.promptText if body.promptText is not None else str(current["prompt_text"] or "")
        )
        next_tags = body.tags if body.tags is not None else list(current["tags"])
        next_few_shot = None
        if body.fewShotExamples is not None:
            next_few_shot = [
                FewShotExample(user=ex.user, assistant=ex.assistant)
                for ex in body.fewShotExamples
            ]
        else:
            raw_few_shot = current.get("few_shot", [])
            if isinstance(raw_few_shot, list):
                next_few_shot = [
                    FewShotExample(user=str(ex["user"]), assistant=str(ex["assistant"]))
                    for ex in raw_few_shot
                    if isinstance(ex, dict)
                ]
        payload = repository.update(
            persona_uuid,
            name=next_name,
            prompt_text=next_prompt_text,
            tags=next_tags,
            enabled=body.enabled if body.enabled is not None else bool(current["enabled"]),
            few_shot=next_few_shot,
        )
    except PersonaFileError as exc:
        _raise_admin_http_error(exc)

    return ok(serialize_persona(payload))


@router.delete("/{persona_uuid}", response_model=Envelope[PersonaDeletedData])
def delete_persona(persona_uuid: str, boot: Any = BootDep) -> dict[str, Any]:
    """Delete a persona by its UUID."""
    try:
        _persona_repository(boot).delete(persona_uuid)
    except PersonaFileError as exc:
        _raise_admin_http_error(exc)
    return ok({"deleted": True, "uuid": persona_uuid})
