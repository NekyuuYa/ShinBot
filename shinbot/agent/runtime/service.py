"""Minimal agent runtime that assembles prompts and calls the model runtime."""

from __future__ import annotations

import logging
from typing import Any

from shinbot.agent.model_runtime import ModelRuntime, ModelRuntimeCall
from shinbot.agent.prompting import (
    ContextStrategy,
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptProfile,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.tools import ToolRegistry
from shinbot.persistence import DatabaseManager, PromptSnapshotRecord

logger = logging.getLogger(__name__)

DEFAULT_AGENT_SYSTEM_PROMPT_ID = "__agent_runtime.system_base__"


class AgentRuntime:
    """Resolve configured agents and execute completion calls."""

    def __init__(
        self,
        *,
        database: DatabaseManager | None,
        prompt_registry: PromptRegistry,
        model_runtime: ModelRuntime,
        tool_registry: ToolRegistry,
    ) -> None:
        self._database = database
        self._prompt_registry = prompt_registry
        self._model_runtime = model_runtime
        self._tool_registry = tool_registry

    async def handle_message(self, ctx: Any) -> bool:
        if self._database is None:
            return False

        agent_payload = self._resolve_default_agent(ctx.adapter.instance_id)
        if agent_payload is None:
            return False

        target = self._resolve_model_target(ctx.adapter.instance_id, agent_payload)
        if target is None:
            logger.debug("No model target configured for instance %s", ctx.adapter.instance_id)
            return False

        prompt_registry = self._build_prompt_registry(agent_payload)
        request = self._build_prompt_request(ctx, agent_payload, target)
        result = prompt_registry.assemble(request)
        snapshot = prompt_registry.create_snapshot(result, request)
        self._persist_snapshot(snapshot)

        tools = result.tools + self._resolve_agent_tools(agent_payload)
        ctx.mark_trigger_read()
        response = await self._model_runtime.generate(
            ModelRuntimeCall(
                caller="agent.runtime",
                route_id=target["route_id"],
                model_id=target["model_id"],
                session_id=ctx.session_id,
                instance_id=ctx.adapter.instance_id,
                prompt_snapshot_id=snapshot.id,
                purpose="message_reply",
                messages=result.messages,
                tools=tools,
                metadata={
                    "agent_uuid": agent_payload["uuid"],
                    "agent_id": agent_payload["agent_id"],
                },
            )
        )
        text = response.text.strip()
        if not text:
            return False
        await ctx.send(text)
        return True

    def _resolve_default_agent(self, instance_id: str) -> dict[str, Any] | None:
        assert self._database is not None
        bot_config = self._database.bot_configs.get_by_instance_id(instance_id)
        if bot_config is None or not bot_config["default_agent_uuid"]:
            return None
        return self._database.agents.get(str(bot_config["default_agent_uuid"]))

    def _resolve_model_target(
        self,
        instance_id: str,
        agent_payload: dict[str, Any],
    ) -> dict[str, str] | None:
        assert self._database is not None
        agent_config = dict(agent_payload.get("config") or {})
        bot_config = self._database.bot_configs.get_by_instance_id(instance_id) or {}
        candidates = [
            str(agent_config.get("routeId") or ""),
            str(agent_config.get("route_id") or ""),
            str(agent_config.get("modelId") or ""),
            str(agent_config.get("model_id") or ""),
            str(bot_config.get("main_llm") or ""),
        ]
        for candidate in candidates:
            if not candidate:
                continue
            route = self._database.model_registry.get_route(candidate)
            if route is not None and route["enabled"]:
                return {"route_id": candidate, "model_id": ""}
            model = self._database.model_registry.get_model(candidate)
            if model is not None and model["enabled"]:
                return {"route_id": "", "model_id": candidate}
        return None

    def _build_prompt_registry(self, agent_payload: dict[str, Any]) -> PromptRegistry:
        registry = PromptRegistry(context_manager=self._prompt_registry._context_manager)
        registry._components = dict(self._prompt_registry._components)
        registry._profiles = dict(self._prompt_registry._profiles)
        registry._resolvers = dict(self._prompt_registry._resolvers)
        registry._context_strategies = dict(self._prompt_registry._context_strategies)
        registry._context_strategy_resolvers = dict(
            self._prompt_registry._context_strategy_resolvers
        )

        registry.register_component(
            PromptComponent(
                id=DEFAULT_AGENT_SYSTEM_PROMPT_ID,
                stage=PromptStage.SYSTEM_BASE,
                kind=PromptComponentKind.STATIC_TEXT,
                content="You are ShinBot, a helpful AI assistant.",
                metadata={"builtin": True, "display_name": "Agent Runtime System Base"},
            )
        )

        component_ids = [DEFAULT_AGENT_SYSTEM_PROMPT_ID]
        for prompt_payload in self._iter_agent_prompt_payloads(agent_payload):
            component = self._payload_to_component(prompt_payload)
            if registry.get_component(component.id) is None:
                registry.register_component(component)
            component_ids.append(component.id)

        profile_id = self._agent_profile_id(agent_payload)
        registry.register_profile(
            PromptProfile(
                id=profile_id,
                display_name=str(agent_payload["name"]),
                base_components=component_ids,
            )
        )

        strategy_ref = str((agent_payload.get("context_strategy") or {}).get("ref") or "")
        if strategy_ref and strategy_ref not in registry._context_strategies:
            strategy = self._payload_to_context_strategy(strategy_ref)
            if strategy is not None:
                registry.register_context_strategy(strategy)

        return registry

    def _build_prompt_request(
        self,
        ctx: Any,
        agent_payload: dict[str, Any],
        target: dict[str, str],
    ) -> PromptAssemblyRequest:
        assert self._database is not None
        strategy_ref = str((agent_payload.get("context_strategy") or {}).get("ref") or "")
        model_context_window = None
        if target["model_id"]:
            model = self._database.model_registry.get_model(target["model_id"])
            if model is not None:
                model_context_window = model.get("context_window")
        return PromptAssemblyRequest(
            profile_id=self._agent_profile_id(agent_payload),
            context_strategy_id=strategy_ref,
            caller="agent.runtime",
            session_id=ctx.session_id,
            instance_id=ctx.adapter.instance_id,
            route_id=target["route_id"],
            model_id=target["model_id"],
            model_context_window=model_context_window,
            metadata={
                "agent_uuid": agent_payload["uuid"],
                "agent_id": agent_payload["agent_id"],
                "platform": ctx.platform,
            },
        )

    def _resolve_agent_tools(self, agent_payload: dict[str, Any]) -> list[dict[str, Any]]:
        selected: list[dict[str, Any]] = []
        for tool_ref in agent_payload.get("tools", []):
            definition = self._tool_registry.get_tool(str(tool_ref))
            if definition is None:
                definition = self._tool_registry.get_tool_by_name(str(tool_ref))
            if definition is None or not definition.enabled:
                continue
            selected.append(
                {
                    "type": "function",
                    "function": {
                        "name": definition.name,
                        "description": definition.description,
                        "parameters": definition.input_schema,
                    },
                }
            )
        return selected

    def _persist_snapshot(self, snapshot: Any) -> None:
        if self._database is None:
            return
        self._database.prompt_snapshots.insert(
            PromptSnapshotRecord(
                id=snapshot.id,
                profile_id=snapshot.profile_id,
                caller=snapshot.caller,
                session_id=snapshot.session_id,
                instance_id=snapshot.instance_id,
                route_id=snapshot.route_id,
                model_id=snapshot.model_id,
                prompt_signature=snapshot.prompt_signature,
                cache_key=snapshot.cache_key,
                messages=snapshot.full_messages,
                tools=snapshot.full_tools,
                compatibility_used=snapshot.compatibility_used,
            )
        )

    def _iter_agent_prompt_payloads(self, agent_payload: dict[str, Any]) -> list[dict[str, Any]]:
        assert self._database is not None
        payloads: list[dict[str, Any]] = []
        persona = self._database.personas.get(str(agent_payload["persona_uuid"]))
        if persona is not None:
            prompt_payload = self._database.prompt_definitions.get(str(persona["prompt_definition_uuid"]))
            if prompt_payload is not None:
                payloads.append(prompt_payload)
        for prompt_uuid in agent_payload.get("prompts", []):
            prompt_payload = self._database.prompt_definitions.get(str(prompt_uuid))
            if prompt_payload is not None:
                payloads.append(prompt_payload)
        return payloads

    def _payload_to_component(self, payload: dict[str, Any]) -> PromptComponent:
        return PromptComponent(
            id=str(payload["prompt_id"]),
            stage=PromptStage(str(payload["stage"])),
            kind=PromptComponentKind(str(payload["type"])),
            version=str(payload["version"]),
            priority=int(payload["priority"]),
            enabled=bool(payload["enabled"]),
            content=str(payload["content"]),
            template_vars=list(payload["template_vars"]),
            resolver_ref=str(payload["resolver_ref"]),
            bundle_refs=list(payload["bundle_refs"]),
            tags=list(payload["tags"]),
            metadata=dict(payload["metadata"]),
        )

    def _payload_to_context_strategy(self, strategy_ref: str) -> ContextStrategy | None:
        assert self._database is not None
        payload = self._database.context_strategies.get(strategy_ref)
        if payload is None:
            return None
        config = dict(payload["config"])
        budget = dict(config.get("budget") or {})
        return ContextStrategy(
            id=str(payload["uuid"]),
            display_name=str(payload["name"]),
            description=str(payload["description"]),
            resolver_ref=str(payload["resolver_ref"]),
            enabled=bool(payload["enabled"]),
            budget=budget or {},
            metadata={k: v for k, v in config.items() if k != "budget"},
        )

    def _agent_profile_id(self, agent_payload: dict[str, Any]) -> str:
        return f"agent.runtime.{agent_payload['uuid']}"
