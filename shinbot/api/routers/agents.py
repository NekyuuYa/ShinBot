"""Agent management router: /api/v1/agents"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.agent_admin import (
    AgentAdminError,
    assert_agent_id_available,
    build_agent_record,
    get_agent_or_raise,
    normalize_agent_input,
    serialize_agent,
    validate_agent_references,
)
from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import ok

router = APIRouter(
    prefix="/agents",
    tags=["agents"],
    dependencies=AuthRequired,
)


class AgentContextStrategyRequest(BaseModel):
    ref: str = ""
    type: str = ""
    params: dict[str, object] = Field(default_factory=dict)


class AgentRequest(BaseModel):
    agentId: str
    name: str
    personaUuid: str
    prompts: list[str] = Field(default_factory=list)
    tools: list[str] = Field(default_factory=list)
    contextStrategy: AgentContextStrategyRequest = Field(
        default_factory=AgentContextStrategyRequest
    )
    config: dict[str, object] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)


class AgentPatchRequest(BaseModel):
    agentId: str | None = None
    name: str | None = None
    personaUuid: str | None = None
    prompts: list[str] | None = None
    tools: list[str] | None = None
    contextStrategy: AgentContextStrategyRequest | None = None
    config: dict[str, object] | None = None
    tags: list[str] | None = None


def _raise_admin_http_error(exc: AgentAdminError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail={"code": exc.code, "message": exc.message},
    ) from exc


@router.get("")
def list_agents(bot=BotDep):
    return ok([serialize_agent(item) for item in bot.database.agents.list()])


@router.post("", status_code=201)
def create_agent(body: AgentRequest, bot=BotDep):
    try:
        normalized = normalize_agent_input(
            agent_id=body.agentId,
            name=body.name,
            persona_uuid=body.personaUuid,
            prompts=body.prompts,
            tools=body.tools,
            context_strategy=body.contextStrategy,
            config=body.config,
            tags=body.tags,
        )
        assert_agent_id_available(bot.database, normalized.agent_id, current_uuid=None)
        validate_agent_references(
            bot=bot,
            persona_uuid=normalized.persona_uuid,
            prompt_uuids=normalized.prompts,
            context_strategy=normalized.context_strategy,
        )
    except AgentAdminError as exc:
        _raise_admin_http_error(exc)

    record = build_agent_record(agent_uuid=None, input_data=normalized)
    bot.database.agents.upsert(record)
    payload = bot.database.agents.get(record.uuid)
    assert payload is not None
    return ok(serialize_agent(payload))


@router.get("/{agent_uuid}")
def get_agent(agent_uuid: str, bot=BotDep):
    try:
        payload = get_agent_or_raise(bot.database, agent_uuid)
    except AgentAdminError as exc:
        _raise_admin_http_error(exc)
    return ok(serialize_agent(payload))


@router.patch("/{agent_uuid}")
def patch_agent(agent_uuid: str, body: AgentPatchRequest, bot=BotDep):
    try:
        current = get_agent_or_raise(bot.database, agent_uuid)
        normalized = normalize_agent_input(
            agent_id=body.agentId if body.agentId is not None else str(current["agent_id"]),
            name=body.name if body.name is not None else str(current["name"]),
            persona_uuid=(
                body.personaUuid if body.personaUuid is not None else str(current["persona_uuid"])
            ),
            prompts=body.prompts if body.prompts is not None else list(current["prompts"]),
            tools=body.tools if body.tools is not None else list(current["tools"]),
            context_strategy=(
                body.contextStrategy
                if body.contextStrategy is not None
                else AgentContextStrategyRequest.model_validate(current["context_strategy"])
            ),
            config=body.config if body.config is not None else dict(current["config"]),
            tags=body.tags if body.tags is not None else list(current["tags"]),
        )
        assert_agent_id_available(bot.database, normalized.agent_id, current_uuid=agent_uuid)
        validate_agent_references(
            bot=bot,
            persona_uuid=normalized.persona_uuid,
            prompt_uuids=normalized.prompts,
            context_strategy=normalized.context_strategy,
        )
    except AgentAdminError as exc:
        _raise_admin_http_error(exc)

    bot.database.agents.upsert(
        build_agent_record(
            agent_uuid=agent_uuid,
            input_data=normalized,
            created_at=str(current["created_at"]),
        )
    )
    payload = bot.database.agents.get(agent_uuid)
    assert payload is not None
    return ok(serialize_agent(payload))


@router.delete("/{agent_uuid}")
def delete_agent(agent_uuid: str, bot=BotDep):
    try:
        get_agent_or_raise(bot.database, agent_uuid)
    except AgentAdminError as exc:
        _raise_admin_http_error(exc)
    bot.database.agents.delete(agent_uuid)
    return ok({"deleted": True, "uuid": agent_uuid})
