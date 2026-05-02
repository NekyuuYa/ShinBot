"""Administrative helpers for agent management flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from shinbot.persistence.records import AgentRecord, utc_now_iso


@dataclass(slots=True)
class AgentAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class NormalizedAgentInput:
    agent_id: str
    name: str
    persona_uuid: str
    prompts: list[str]
    tools: list[str]
    context_strategy: dict[str, object]
    config: dict[str, object]
    tags: list[str]


def serialize_agent(payload: dict[str, object]) -> dict[str, object]:
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


def _dedupe_strings(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def validate_sliding_window_params(params: dict[str, object]) -> None:
    trigger_ratio = params.get("triggerRatio")
    if trigger_ratio is not None and (
        not isinstance(trigger_ratio, int | float) or not 0 < float(trigger_ratio) <= 1
    ):
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.params.triggerRatio must be within (0, 1]",
        )
    trim_turns = params.get("trimTurns")
    if trim_turns is not None and (not isinstance(trim_turns, int) or trim_turns < 1):
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.params.trimTurns must be greater than or equal to 1",
        )
    trim_ratio = params.get("trimRatio")
    if trim_ratio is not None and (
        not isinstance(trim_ratio, int | float) or not 0 < float(trim_ratio) < 1
    ):
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.params.trimRatio must be within (0, 1)",
        )
    max_history_turns = params.get("maxHistoryTurns")
    if max_history_turns is not None and (
        not isinstance(max_history_turns, int) or max_history_turns < 1
    ):
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.params.maxHistoryTurns must be greater than or equal to 1",
        )
    max_context_tokens = params.get("maxContextTokens")
    if max_context_tokens is not None and (
        not isinstance(max_context_tokens, int) or max_context_tokens < 1
    ):
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.params.maxContextTokens must be greater than or equal to 1",
        )


def normalize_context_strategy(context_strategy: Any) -> dict[str, object]:
    ref = str(getattr(context_strategy, "ref", "") or "").strip()
    strategy_type = str(getattr(context_strategy, "type", "") or "").strip()
    params = dict(getattr(context_strategy, "params", {}) or {})
    if not ref and not strategy_type and not params:
        return {}
    if not ref:
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.ref must not be empty",
        )
    if not strategy_type:
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.type must not be empty",
        )
    if strategy_type == "sliding_window":
        validate_sliding_window_params(params)
    return {"ref": ref, "type": strategy_type, "params": params}


def normalize_agent_input(
    *,
    agent_id: str,
    name: str,
    persona_uuid: str,
    prompts: list[str],
    tools: list[str],
    context_strategy: Any,
    config: dict[str, object],
    tags: list[str],
) -> NormalizedAgentInput:
    normalized_agent_id = agent_id.strip()
    normalized_name = name.strip()
    normalized_persona_uuid = persona_uuid.strip()
    normalized_prompts = [prompt_id.strip() for prompt_id in prompts if prompt_id.strip()]
    normalized_tools = [tool.strip() for tool in tools if tool.strip()]
    normalized_tags = [tag.strip() for tag in tags if tag.strip()]

    if not normalized_agent_id:
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent agentId must not be empty",
        )
    if not normalized_name:
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent name must not be empty",
        )
    if not normalized_persona_uuid:
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent personaUuid must not be empty",
        )

    return NormalizedAgentInput(
        agent_id=normalized_agent_id,
        name=normalized_name,
        persona_uuid=normalized_persona_uuid,
        prompts=_dedupe_strings(normalized_prompts),
        tools=_dedupe_strings(normalized_tools),
        context_strategy=normalize_context_strategy(context_strategy),
        config=dict(config),
        tags=_dedupe_strings(normalized_tags),
    )


def validate_agent_references(
    *,
    bot: Any,
    persona_uuid: str,
    prompt_uuids: list[str],
    context_strategy: dict[str, object],
) -> None:
    if bot.database.personas.get(persona_uuid) is None:
        raise AgentAdminError(
            status_code=404,
            code="PERSONA_NOT_FOUND",
            message=f"Persona {persona_uuid!r} was not found",
        )

    agent_runtime = getattr(bot, "agent_runtime", None)
    prompt_registry = getattr(agent_runtime, "prompt_registry", None)
    for prompt_uuid in prompt_uuids:
        runtime_prompt = (
            prompt_registry.get_component(prompt_uuid) if prompt_registry is not None else None
        )
        if (
            bot.database.prompt_definitions.get(prompt_uuid) is None
            and bot.database.prompt_definitions.get_by_prompt_id(prompt_uuid) is None
            and runtime_prompt is None
        ):
            raise AgentAdminError(
                status_code=404,
                code="PROMPT_NOT_FOUND",
                message=f"Prompt {prompt_uuid!r} was not found",
            )

    if not context_strategy:
        return
    context_strategy_ref = str(context_strategy["ref"])
    payload = bot.database.context_strategies.get(context_strategy_ref)
    if payload is None:
        raise AgentAdminError(
            status_code=404,
            code="CONTEXT_STRATEGY_NOT_FOUND",
            message=f"Context strategy {context_strategy_ref!r} was not found",
        )
    strategy_type = str(context_strategy["type"])
    if payload["type"] != strategy_type:
        raise AgentAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Agent contextStrategy.type must match the referenced context strategy",
        )


def get_agent_or_raise(database: Any, agent_uuid: str) -> dict[str, object]:
    payload = database.agents.get(agent_uuid)
    if payload is None:
        raise AgentAdminError(
            status_code=404,
            code="AGENT_NOT_FOUND",
            message=f"Agent {agent_uuid!r} was not found",
        )
    return payload


def assert_agent_id_available(database: Any, agent_id: str, *, current_uuid: str | None) -> None:
    existing = database.agents.get_by_agent_id(agent_id)
    if existing is not None and existing["uuid"] != current_uuid:
        raise AgentAdminError(
            status_code=409,
            code="AGENT_ALREADY_EXISTS",
            message=f"Agent {agent_id!r} already exists",
        )


def build_agent_record(
    *,
    agent_uuid: str | None,
    input_data: NormalizedAgentInput,
    created_at: str | None = None,
) -> AgentRecord:
    now = utc_now_iso()
    return AgentRecord(
        uuid=agent_uuid or str(uuid4()),
        agent_id=input_data.agent_id,
        name=input_data.name,
        persona_uuid=input_data.persona_uuid,
        prompts=input_data.prompts,
        tools=input_data.tools,
        context_strategy=input_data.context_strategy,
        config=input_data.config,
        tags=input_data.tags,
        created_at=created_at or now,
        updated_at=now,
    )
