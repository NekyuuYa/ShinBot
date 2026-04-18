"""Agent management router: /api/v1/agents"""

from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.api.deps import AuthRequired, BotDep
from shinbot.api.models import EC, ok
from shinbot.persistence.records import AgentRecord, utc_now_iso

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


def _serialize_agent(payload: dict[str, object]) -> dict[str, object]:
    return {
        "uuid": payload["uuid"],
        "agentId": payload["agent_id"],
        "name": payload["name"],
        "personaUuid": payload["persona_uuid"],
        "prompts": payload["prompts"],
        "tools": payload["tools"],
        "contextStrategy": payload["context_strategy"],
        "config": payload["config"],
        "tags": payload["tags"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def _normalize_agent_input(
    *,
    agent_id: str,
    name: str,
    persona_uuid: str,
    prompts: list[str],
    tools: list[str],
    context_strategy: AgentContextStrategyRequest,
    config: dict[str, object],
    tags: list[str],
) -> tuple[str, str, str, list[str], list[str], dict[str, object], dict[str, object], list[str]]:
    normalized_agent_id = agent_id.strip()
    normalized_name = name.strip()
    normalized_persona_uuid = persona_uuid.strip()
    normalized_prompts = [prompt_id.strip() for prompt_id in prompts if prompt_id.strip()]
    normalized_tools = [tool.strip() for tool in tools if tool.strip()]
    normalized_tags = [tag.strip() for tag in tags if tag.strip()]

    if not normalized_agent_id:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Agent agentId must not be empty"},
        )
    if not normalized_name:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Agent name must not be empty"},
        )
    if not normalized_persona_uuid:
        raise HTTPException(
            status_code=400,
            detail={"code": EC.INVALID_ACTION, "message": "Agent personaUuid must not be empty"},
        )

    deduped_prompts: list[str] = []
    seen_prompts: set[str] = set()
    for prompt_id in normalized_prompts:
        if prompt_id in seen_prompts:
            continue
        seen_prompts.add(prompt_id)
        deduped_prompts.append(prompt_id)

    deduped_tools: list[str] = []
    seen_tools: set[str] = set()
    for tool_id in normalized_tools:
        if tool_id in seen_tools:
            continue
        seen_tools.add(tool_id)
        deduped_tools.append(tool_id)

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    normalized_context_strategy = _normalize_context_strategy(context_strategy)

    return (
        normalized_agent_id,
        normalized_name,
        normalized_persona_uuid,
        deduped_prompts,
        deduped_tools,
        normalized_context_strategy,
        dict(config),
        deduped_tags,
    )


def _normalize_context_strategy(
    context_strategy: AgentContextStrategyRequest,
) -> dict[str, object]:
    ref = context_strategy.ref.strip()
    strategy_type = context_strategy.type.strip()
    params = dict(context_strategy.params)
    if not ref and not strategy_type and not params:
        return {}
    if not ref:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.ref must not be empty",
            },
        )
    if not strategy_type:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.type must not be empty",
            },
        )
    if strategy_type == "sliding_window":
        _validate_sliding_window_params(params)
    return {"ref": ref, "type": strategy_type, "params": params}


def _validate_sliding_window_params(params: dict[str, object]) -> None:
    trigger_ratio = params.get("triggerRatio")
    if trigger_ratio is not None and (
        not isinstance(trigger_ratio, int | float) or not 0 < float(trigger_ratio) <= 1
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.params.triggerRatio must be within (0, 1]",
            },
        )
    trim_turns = params.get("trimTurns")
    if trim_turns is not None and (not isinstance(trim_turns, int) or trim_turns < 1):
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.params.trimTurns must be greater than or equal to 1",
            },
        )
    trim_ratio = params.get("trimRatio")
    if trim_ratio is not None and (
        not isinstance(trim_ratio, int | float) or not 0 < float(trim_ratio) < 1
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.params.trimRatio must be within (0, 1)",
            },
        )
    max_history_turns = params.get("maxHistoryTurns")
    if max_history_turns is not None and (
        not isinstance(max_history_turns, int) or max_history_turns < 1
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.params.maxHistoryTurns must be greater than or equal to 1",
            },
        )
    max_context_tokens = params.get("maxContextTokens")
    if max_context_tokens is not None and (
        not isinstance(max_context_tokens, int) or max_context_tokens < 1
    ):
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.params.maxContextTokens must be greater than or equal to 1",
            },
        )


def _validate_agent_references(
    *,
    bot,
    persona_uuid: str,
    prompt_uuids: list[str],
    context_strategy: dict[str, object],
) -> None:
    if bot.database.personas.get(persona_uuid) is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.PERSONA_NOT_FOUND,
                "message": f"Persona {persona_uuid!r} was not found",
            },
        )

    for prompt_uuid in prompt_uuids:
        if (
            bot.database.prompt_definitions.get(prompt_uuid) is None
            and bot.database.prompt_definitions.get_by_prompt_id(prompt_uuid) is None
            and bot.prompt_registry.get_component(prompt_uuid) is None
        ):
            raise HTTPException(
                status_code=404,
                detail={
                    "code": EC.PROMPT_NOT_FOUND,
                    "message": f"Prompt {prompt_uuid!r} was not found",
                },
            )

    if not context_strategy:
        return
    context_strategy_ref = str(context_strategy["ref"])
    payload = bot.database.context_strategies.get(context_strategy_ref)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.CONTEXT_STRATEGY_NOT_FOUND,
                "message": f"Context strategy {context_strategy_ref!r} was not found",
            },
        )
    strategy_type = str(context_strategy["type"])
    if payload["type"] != strategy_type:
        raise HTTPException(
            status_code=400,
            detail={
                "code": EC.INVALID_ACTION,
                "message": "Agent contextStrategy.type must match the referenced context strategy",
            },
        )


@router.get("")
def list_agents(bot=BotDep):
    return ok([_serialize_agent(item) for item in bot.database.agents.list()])


@router.post("", status_code=201)
def create_agent(body: AgentRequest, bot=BotDep):
    agent_id, name, persona_uuid, prompts, tools, context_strategy, config, tags = (
        _normalize_agent_input(
            agent_id=body.agentId,
            name=body.name,
            persona_uuid=body.personaUuid,
            prompts=body.prompts,
            tools=body.tools,
            context_strategy=body.contextStrategy,
            config=body.config,
            tags=body.tags,
        )
    )
    if bot.database.agents.get_by_agent_id(agent_id) is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.AGENT_ALREADY_EXISTS,
                "message": f"Agent {agent_id!r} already exists",
            },
        )
    _validate_agent_references(
        bot=bot,
        persona_uuid=persona_uuid,
        prompt_uuids=prompts,
        context_strategy=context_strategy,
    )

    now = utc_now_iso()
    record = AgentRecord(
        uuid=str(uuid4()),
        agent_id=agent_id,
        name=name,
        persona_uuid=persona_uuid,
        prompts=prompts,
        tools=tools,
        context_strategy=context_strategy,
        config=config,
        tags=tags,
        created_at=now,
        updated_at=now,
    )
    bot.database.agents.upsert(record)
    payload = bot.database.agents.get(record.uuid)
    assert payload is not None
    return ok(_serialize_agent(payload))


@router.get("/{agent_uuid}")
def get_agent(agent_uuid: str, bot=BotDep):
    payload = bot.database.agents.get(agent_uuid)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.AGENT_NOT_FOUND, "message": f"Agent {agent_uuid!r} was not found"},
        )
    return ok(_serialize_agent(payload))


@router.patch("/{agent_uuid}")
def patch_agent(agent_uuid: str, body: AgentPatchRequest, bot=BotDep):
    current = bot.database.agents.get(agent_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.AGENT_NOT_FOUND, "message": f"Agent {agent_uuid!r} was not found"},
        )

    next_agent_id = body.agentId if body.agentId is not None else str(current["agent_id"])
    next_name = body.name if body.name is not None else str(current["name"])
    next_persona_uuid = (
        body.personaUuid if body.personaUuid is not None else str(current["persona_uuid"])
    )
    next_prompts = body.prompts if body.prompts is not None else list(current["prompts"])
    next_tools = body.tools if body.tools is not None else list(current["tools"])
    next_context_strategy = (
        body.contextStrategy
        if body.contextStrategy is not None
        else AgentContextStrategyRequest.model_validate(current["context_strategy"])
    )
    next_config = body.config if body.config is not None else dict(current["config"])
    next_tags = body.tags if body.tags is not None else list(current["tags"])
    agent_id, name, persona_uuid, prompts, tools, context_strategy, config, tags = (
        _normalize_agent_input(
            agent_id=next_agent_id,
            name=next_name,
            persona_uuid=next_persona_uuid,
            prompts=next_prompts,
            tools=next_tools,
            context_strategy=next_context_strategy,
            config=next_config,
            tags=next_tags,
        )
    )

    existing = bot.database.agents.get_by_agent_id(agent_id)
    if existing is not None and existing["uuid"] != agent_uuid:
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.AGENT_ALREADY_EXISTS,
                "message": f"Agent {agent_id!r} already exists",
            },
        )
    _validate_agent_references(
        bot=bot,
        persona_uuid=persona_uuid,
        prompt_uuids=prompts,
        context_strategy=context_strategy,
    )

    bot.database.agents.upsert(
        AgentRecord(
            uuid=agent_uuid,
            agent_id=agent_id,
            name=name,
            persona_uuid=persona_uuid,
            prompts=prompts,
            tools=tools,
            context_strategy=context_strategy,
            config=config,
            tags=tags,
            created_at=str(current["created_at"]),
            updated_at=utc_now_iso(),
        )
    )
    payload = bot.database.agents.get(agent_uuid)
    assert payload is not None
    return ok(_serialize_agent(payload))


@router.delete("/{agent_uuid}")
def delete_agent(agent_uuid: str, bot=BotDep):
    current = bot.database.agents.get(agent_uuid)
    if current is None:
        raise HTTPException(
            status_code=404,
            detail={"code": EC.AGENT_NOT_FOUND, "message": f"Agent {agent_uuid!r} was not found"},
        )
    bot.database.agents.delete(agent_uuid)
    return ok({"deleted": True, "uuid": agent_uuid})
