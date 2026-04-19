"""Message workflow pipeline — the core processing engine.

Implements the message workflow specification (01_message_workflow.md).

Pipeline stages:
  1. Ingress & Normalization: adapter → UnifiedEvent + MessageElement AST
  2. Context Enrichment: session resolution, permission merge, context build
  3. Workflow Dispatching: interceptors → command resolution → agent/event bus
  4. Post-processing: state persistence, audit logging
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from shinbot.agent.model_runtime import ModelCallError, ModelRuntime, ModelRuntimeCall
from shinbot.agent.prompting import (
    ContextStrategy,
    ContextStrategyBudget,
    PromptAssemblyRequest,
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
)
from shinbot.core.dispatch.command import CommandMatch, CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine, check_permission
from shinbot.core.state.session import Session, SessionManager, build_session_id
from shinbot.persistence.records import MessageLogRecord, PromptSnapshotRecord
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.logger import get_logger
from shinbot.utils.resource_ingress import summarize_message_modalities

if TYPE_CHECKING:
    from shinbot.agent.context import ContextManager
    from shinbot.agent.attention.scheduler import AttentionScheduler
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)


# ── Interactive input registry ────────────────────────────────────────────────


class WaitingInputRegistry:
    """Tracks sessions that are waiting for the next user message.

    When a plugin calls `ctx.wait_for_input()`, it registers a Future here.
    The pipeline checks this registry before command resolution: if the
    session is waiting, the incoming message text is used to resolve the
    Future and normal processing is skipped.
    """

    def __init__(self) -> None:
        self._waiting: dict[str, asyncio.Future[str]] = {}

    def is_waiting(self, session_id: str) -> bool:
        return session_id in self._waiting

    def register(self, session_id: str) -> asyncio.Future[str]:
        """Create and store a Future for the given session."""
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[str] = loop.create_future()
        self._waiting[session_id] = fut
        return fut

    def resolve(self, session_id: str, text: str) -> bool:
        """Resolve the pending Future with received text. Returns True if resolved."""
        fut = self._waiting.pop(session_id, None)
        if fut is not None and not fut.done():
            fut.set_result(text)
            return True
        return False

    def cancel(self, session_id: str) -> None:
        """Cancel and remove the pending Future."""
        fut = self._waiting.pop(session_id, None)
        if fut is not None and not fut.done():
            fut.cancel()


class MessageContext:
    """Rich context object threaded through the entire processing pipeline.

    Created during context enrichment (stage 2) and passed to all
    handlers, interceptors, and plugins. Provides the send() API
    that plugins use to emit responses, and wait_for_input() for
    multi-turn interactive commands.
    """

    def __init__(
        self,
        event: UnifiedEvent,
        message: Message,
        session: Session,
        adapter: BaseAdapter,
        permissions: set[str],
        waiting_registry: WaitingInputRegistry | None = None,
        database: DatabaseManager | None = None,
        context_manager: ContextManager | None = None,
    ):
        self.event = event
        self.message = message
        self.session = session
        self.adapter = adapter
        self.permissions = permissions
        self._waiting_registry = waiting_registry
        self._database = database
        self._context_manager = context_manager

        # Command resolution result (set during dispatch)
        self.command_match: CommandMatch | None = None

        # Tracking
        self.start_time: float = time.monotonic()
        self._sent_messages: list[MessageHandle] = []
        self._assistant_log_ids: list[int] = []
        self._stopped: bool = False

        # Message log: id of the triggering user message in message_logs
        self._msg_log_id: int | None = None

    # ── Convenience accessors ────────────────────────────────────────

    @property
    def text(self) -> str:
        """Plain text content of the message."""
        return self.message.get_text()

    @property
    def elements(self) -> list[MessageElement]:
        return self.message.elements

    @property
    def user_id(self) -> str:
        return self.event.sender_id or ""

    @property
    def session_id(self) -> str:
        return self.session.id

    @property
    def platform(self) -> str:
        return self.event.platform

    @property
    def is_private(self) -> bool:
        return self.session.is_private

    @property
    def is_mentioned(self) -> bool:
        return any(
            el.type == "at" and el.attrs.get("id") == self.event.self_id
            for el in self.message.elements
        )

    def _quoted_message_ids(self) -> list[str]:
        ids: list[str] = []
        stack = list(self.message.elements)
        while stack:
            element = stack.pop()
            if element.type == "quote":
                quote_id = str(element.attrs.get("id") or "").strip()
                if quote_id:
                    ids.append(quote_id)
            if element.children:
                stack.extend(element.children)
        return ids

    def is_reply_to_bot(self) -> bool:
        """Return True when this message quotes a previously sent bot message."""
        quote_ids = self._quoted_message_ids()
        if not quote_ids or self._database is None:
            return False

        for quote_id in quote_ids:
            record = self._database.message_logs.get_by_platform_msg_id(self.session_id, quote_id)
            if record is None:
                continue
            if str(record.get("role") or "").strip() == "assistant":
                return True
            sender_id = str(record.get("sender_id") or "").strip()
            if sender_id and sender_id == self.event.self_id:
                return True
        return False

    # ── Permission checking ──────────────────────────────────────────

    def has_permission(self, permission: str) -> bool:
        return check_permission(permission, self.permissions)

    # ── Response API ─────────────────────────────────────────────────

    async def send(
        self,
        content: str | Message | list[MessageElement],
    ) -> MessageHandle:
        """Send a response to the current session.

        Accepts:
          - str: auto-wrapped as a single text element
          - Message: sent as-is
          - list[MessageElement]: wrapped in a Message

        Returns a MessageHandle for edit/recall.
        """
        if isinstance(content, str):
            elements = (
                Message.from_xml(content).elements
                if "<" in content
                else [MessageElement.text(content)]
            )
        elif isinstance(content, Message):
            elements = content.elements
        else:
            elements = content

        handle = await self.adapter.send(self.session.id, elements)
        self._sent_messages.append(handle)

        # Persist assistant message to message_logs
        try:
            self._log_assistant_message(elements, handle)
        except Exception:
            logger.exception(
                "Failed to log assistant message in session %s"
                " (message was already delivered to platform)",
                self.session_id,
            )

        return handle

    async def _call_api(self, method: str, params: dict[str, Any]) -> Any:
        """Call adapter API and re-raise with clear context on failure."""
        try:
            return await self.adapter.call_api(method, params)
        except Exception as e:
            raise RuntimeError(f"API call failed: {method} params={params}") from e

    async def kick(self, user_id: str, guild_id: str | None = None) -> Any:
        """Kick a guild member via `member.kick`."""
        resolved_guild_id = guild_id or self.event.guild_id
        if not resolved_guild_id:
            raise ValueError(
                "guild_id is required for kick; provide guild_id or ensure event.guild is set"
            )
        return await self._call_api(
            "member.kick",
            {"user_id": user_id, "guild_id": resolved_guild_id},
        )

    async def mute(self, user_id: str, duration: int, guild_id: str | None = None) -> Any:
        """Mute a guild member via `member.mute`."""
        resolved_guild_id = guild_id or self.event.guild_id
        if not resolved_guild_id:
            raise ValueError(
                "guild_id is required for mute; provide guild_id or ensure event.guild is set"
            )
        return await self._call_api(
            "member.mute",
            {"user_id": user_id, "duration": duration, "guild_id": resolved_guild_id},
        )

    async def poke(self, user_id: str) -> Any:
        """Send a poke action via platform internal API namespace."""
        return await self._call_api(
            f"internal.{self.platform}.poke",
            {"user_id": user_id},
        )

    async def approve_friend(self, message_id: str) -> Any:
        """Approve a friend request via `friend.approve`."""
        return await self._call_api(
            "friend.approve",
            {"message_id": message_id},
        )

    async def get_member_list(self, guild_id: str | None = None) -> Any:
        """Fetch member list for current guild/session context."""
        resolved_guild_id = guild_id or self.event.guild_id or self.event.channel_id
        if not resolved_guild_id:
            raise ValueError(
                "guild_id is required for get_member_list; provide guild_id or ensure event.guild/channel is set"
            )
        return await self._call_api(
            "guild.member.list",
            {"guild_id": resolved_guild_id},
        )

    async def set_group_name(self, name: str, guild_id: str | None = None) -> Any:
        """Update group/guild name with standard-first, internal-fallback strategy."""
        resolved_guild_id = guild_id or self.event.guild_id or self.event.channel_id
        if not resolved_guild_id:
            raise ValueError(
                "guild_id is required for set_group_name; provide guild_id or ensure event.guild/channel is set"
            )

        try:
            return await self._call_api(
                "guild.update",
                {"guild_id": resolved_guild_id, "name": name},
            )
        except Exception:
            return await self._call_api(
                f"internal.{self.platform}.set_group_name",
                {"group_id": resolved_guild_id, "group_name": name},
            )

    async def delete_msg(self, message_id: str) -> Any:
        """Delete/recall a message by id."""
        return await self._call_api(
            "message.delete",
            {"message_id": message_id},
        )

    async def reply(self, content: str | Message | list[MessageElement]) -> MessageHandle:
        """Send a reply that quotes the original message."""
        if self.event.message is None:
            return await self.send(content)

        # Build quote element
        quote = MessageElement.quote(self.event.message.id)

        # Build response elements
        if isinstance(content, str):
            response_els = (
                Message.from_xml(content).elements
                if "<" in content
                else [MessageElement.text(content)]
            )
        elif isinstance(content, Message):
            response_els = content.elements
        else:
            response_els = list(content)

        all_elements = [quote] + response_els
        handle = await self.adapter.send(self.session.id, all_elements)
        self._sent_messages.append(handle)

        # Persist assistant message to message_logs
        try:
            self._log_assistant_message(all_elements, handle)
        except Exception:
            logger.exception(
                "Failed to log assistant reply in session %s"
                " (message was already delivered to platform)",
                self.session_id,
            )

        return handle

    def _log_assistant_message(
        self,
        elements: list[MessageElement],
        handle: MessageHandle,
    ) -> int | None:
        """Insert an assistant message row into message_logs.

        Raises on DB failure — callers are responsible for catching and logging
        with appropriate context (e.g. "message was already delivered").
        """
        if self._database is None:
            return None
        plain_text = Message(elements=list(elements)).get_text()
        content_json = json.dumps(
            [el.model_dump(mode="json") for el in elements],
            ensure_ascii=False,
        )
        record = MessageLogRecord(
            session_id=self.session.id,
            platform_msg_id=handle.message_id if handle is not None else "",
            sender_id=self.event.self_id,
            sender_name="",
            content_json=content_json,
            raw_text=plain_text,
            role="assistant",
            is_read=True,
            is_mentioned=False,
            created_at=time.time() * 1000,
        )
        record.id = self._database.message_logs.insert(record)
        self._assistant_log_ids.append(record.id)
        if self._context_manager is not None:
            self._context_manager.track_message_record(record, platform=self.event.platform)
        return record.id

    def stop(self) -> None:
        """Signal that processing should stop (no further handlers)."""
        self._stopped = True

    @property
    def is_stopped(self) -> bool:
        return self._stopped

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000

    @property
    def last_response_log_id(self) -> int | None:
        if not self._assistant_log_ids:
            return None
        return self._assistant_log_ids[-1]

    def mark_trigger_read(self) -> None:
        """Mark the triggering user message as read in message_logs.

        Call this when the AI picks up the message for processing.
        """
        if self._database is not None and self._msg_log_id is not None:
            try:
                self._database.message_logs.mark_read(self._msg_log_id)
            except Exception:
                logger.exception("Failed to mark message %d as read", self._msg_log_id)

    # ── Interactive input ────────────────────────────────────────────────────

    async def wait_for_input(
        self,
        prompt: str = "",
        timeout: float | None = 60.0,
    ) -> str:
        """Suspend the current handler and wait for the next message in this session.

        When called, an optional `prompt` is sent, then the coroutine suspends.
        The next message the user sends in this session will resolve the Future
        and be returned as a string.

        Args:
            prompt: Optional message to send before waiting.
            timeout: Maximum seconds to wait. None = wait forever.
                     Raises asyncio.TimeoutError if exceeded.

        Returns:
            The plain text of the next message received in this session.

        Raises:
            RuntimeError: If wait_for_input is not supported in this context.
            asyncio.TimeoutError: If timeout expires with no response.
        """
        if self._waiting_registry is None:
            raise RuntimeError("wait_for_input is not available in this context")

        if prompt:
            await self.send(prompt)

        fut = self._waiting_registry.register(self.session_id)
        if timeout is not None:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        return await fut


# ── Interceptor protocol ─────────────────────────────────────────────

Interceptor = Callable[[MessageContext], Coroutine[Any, Any, bool]]
# Returns True to allow processing to continue, False to block


class MessagePipeline:
    """The main message processing pipeline.

    Orchestrates the full lifecycle:
      1. Build MessageContext from event
      2. Run interceptors (rate limit, mute check, blacklist)
      3. Check for pending wait_for_input — resolve and return early if waiting
      4. Resolve commands (P0/P1/P2)
      5. Dispatch to command handler or event bus
      6. Post-processing (state sync, audit)
    """

    def __init__(
        self,
        adapter_manager: AdapterManager,
        session_manager: SessionManager,
        permission_engine: PermissionEngine,
        command_registry: CommandRegistry,
        event_bus: EventBus,
        audit_logger: AuditLogger | None = None,
        database: DatabaseManager | None = None,
        context_manager: ContextManager | None = None,
        prompt_registry: PromptRegistry | None = None,
        model_runtime: ModelRuntime | None = None,
        attention_scheduler: AttentionScheduler | None = None,
    ):
        self._adapter_manager = adapter_manager
        self._session_manager = session_manager
        self._permission_engine = permission_engine
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._audit_logger = audit_logger
        self._database = database
        self._context_manager = context_manager
        self._prompt_registry = prompt_registry
        self._model_runtime = model_runtime
        self._attention_scheduler = attention_scheduler
        self._interceptors: list[tuple[int, Interceptor]] = []
        self._waiting_registry = WaitingInputRegistry()

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        if isinstance(value, int | float):
            return float(value)
        return None

    @staticmethod
    def _dedupe_text_items(items: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for item in items:
            value = str(item).strip()
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    def _resolve_prompt_definition(self, prompt_ref: str) -> dict[str, Any] | None:
        if self._database is None:
            return None
        payload = self._database.prompt_definitions.get(prompt_ref)
        if payload is not None:
            return payload
        return self._database.prompt_definitions.get_by_prompt_id(prompt_ref)

    def _sync_prompt_definition(self, payload: dict[str, Any]) -> str:
        assert self._prompt_registry is not None
        metadata = dict(payload.get("metadata") or {})
        metadata.setdefault("display_name", str(payload.get("name", "")).strip())
        metadata.setdefault("description", str(payload.get("description", "")).strip())
        for source_key in ("owner_plugin_id", "owner_module", "module_path"):
            source_value = str(payload.get(source_key, "") or "").strip()
            if source_value:
                metadata.setdefault(source_key, source_value)

        component = PromptComponent(
            id=str(payload["prompt_id"]),
            stage=PromptStage(str(payload["stage"])),
            kind=PromptComponentKind(str(payload["type"])),
            version=str(payload.get("version", "1.0.0")),
            priority=int(payload.get("priority", 100)),
            enabled=bool(payload.get("enabled", True)),
            content=str(payload.get("content", "")),
            template_vars=list(payload.get("template_vars", [])),
            resolver_ref=str(payload.get("resolver_ref", "")),
            bundle_refs=list(payload.get("bundle_refs", [])),
            tags=list(payload.get("tags", [])),
            metadata=metadata,
        )
        self._prompt_registry.upsert_component(component)
        return component.id

    def _sync_context_strategy(self, payload: dict[str, Any]) -> str:
        assert self._prompt_registry is not None
        raw_config = dict(payload.get("config") or {})
        budget_payload = raw_config.get("budget", raw_config)
        if not isinstance(budget_payload, dict):
            budget_payload = {}
        budget = ContextStrategyBudget(
            max_context_tokens=self._coerce_int(
                budget_payload.get("max_context_tokens", budget_payload.get("maxContextTokens"))
            ),
            max_history_turns=self._coerce_int(
                budget_payload.get("max_history_turns", budget_payload.get("maxHistoryTurns"))
            ),
            memory_summary_required=bool(
                budget_payload.get(
                    "memory_summary_required",
                    budget_payload.get("memorySummaryRequired", False),
                )
            ),
            truncate_policy=str(
                budget_payload.get("truncate_policy", budget_payload.get("truncatePolicy", "tail"))
            ),
            trigger_ratio=float(
                budget_payload.get("trigger_ratio", budget_payload.get("triggerRatio", 0.5))
            ),
            trim_ratio=self._coerce_float(
                budget_payload.get("trim_ratio", budget_payload.get("trimRatio"))
            ),
            trim_turns=int(budget_payload.get("trim_turns", budget_payload.get("trimTurns", 2))),
        )
        strategy = ContextStrategy(
            id=str(payload["uuid"]),
            display_name=str(payload.get("name", "")),
            description=str(payload.get("description", "")),
            resolver_ref=str(payload["resolver_ref"]),
            enabled=bool(payload.get("enabled", True)),
            budget=budget,
            metadata=raw_config,
        )
        self._prompt_registry.upsert_context_strategy(strategy)
        return strategy.id

    def _resolve_model_target(self, target: str) -> tuple[str, str, int | None]:
        if self._database is None:
            return "", "", None

        route = self._database.model_registry.get_route(target)
        if route is not None and route["enabled"]:
            members = self._database.model_registry.list_route_members(target)
            enabled_members = [member for member in members if member["enabled"]]
            enabled_members.sort(
                key=lambda item: (item["priority"], -item["weight"], item["model_id"])
            )
            for member in enabled_members:
                model = self._database.model_registry.get_model(member["model_id"])
                if model is not None and model["enabled"]:
                    return target, "", model.get("context_window")
            return target, "", None

        model = self._database.model_registry.get_model(target)
        if model is not None and model["enabled"]:
            return "", target, model.get("context_window")

        return "", "", None

    def _should_run_fallback(
        self,
        bot: MessageContext,
        bot_config: dict[str, Any],
    ) -> bool:
        if (
            self._database is None
            or self._prompt_registry is None
            or self._model_runtime is None
            or bot._sent_messages
            or bot.is_stopped
        ):
            return False

        if not str(bot_config.get("default_agent_uuid", "")).strip():
            return False
        if not str(bot_config.get("main_llm", "")).strip():
            return False

        config = dict(bot_config.get("config") or {})
        reply_mode = str(config.get("reply_mode", "")).strip().lower()
        if reply_mode in {"disabled", "off", "none"}:
            return False
        if bot.is_private:
            return True
        if reply_mode in {"group", "all", "always"}:
            return True
        return bot.is_mentioned

    async def _run_fallback_responder(self, bot: MessageContext) -> bool:
        if self._database is None or self._prompt_registry is None or self._model_runtime is None:
            return False

        bot_config = self._database.bot_configs.get_by_instance_id(bot.adapter.instance_id)
        if bot_config is None or not self._should_run_fallback(bot, bot_config):
            return False

        agent_uuid = str(bot_config.get("default_agent_uuid", "")).strip()
        model_target = str(bot_config.get("main_llm", "")).strip()
        agent_payload = self._database.agents.get(agent_uuid)
        if agent_payload is None:
            logger.warning(
                "Fallback responder skipped: default agent %s not found for instance %s",
                agent_uuid,
                bot.adapter.instance_id,
            )
            return False

        persona_uuid = str(agent_payload.get("persona_uuid", "")).strip()
        persona_payload = self._database.personas.get(persona_uuid) if persona_uuid else None
        if persona_payload is None:
            logger.warning(
                "Fallback responder skipped: persona %s not found for agent %s",
                persona_uuid,
                agent_uuid,
            )
            return False

        route_id, model_id, model_context_window = self._resolve_model_target(model_target)
        if not route_id and not model_id:
            logger.warning(
                "Fallback responder skipped: main_llm target %s not found for instance %s",
                model_target,
                bot.adapter.instance_id,
            )
            return False

        component_ids: list[str] = []
        persona_prompt_uuid = str(persona_payload.get("prompt_definition_uuid", "")).strip()
        if persona_prompt_uuid:
            persona_prompt_payload = self._database.prompt_definitions.get(persona_prompt_uuid)
            if persona_prompt_payload is not None:
                component_ids.append(self._sync_prompt_definition(persona_prompt_payload))

        for prompt_ref in agent_payload.get("prompts", []):
            normalized_prompt_ref = str(prompt_ref).strip()
            payload = self._resolve_prompt_definition(normalized_prompt_ref)
            if payload is None and self._prompt_registry is not None:
                component = self._prompt_registry.get_component(normalized_prompt_ref)
                if component is not None:
                    component_ids.append(component.id)
                    continue
            if payload is None:
                logger.warning(
                    "Fallback responder skipped missing prompt %s for agent %s",
                    prompt_ref,
                    agent_uuid,
                )
                continue
            component_ids.append(self._sync_prompt_definition(payload))

        component_ids = self._dedupe_text_items(component_ids)
        if not component_ids:
            logger.warning(
                "Fallback responder skipped: agent %s has no resolvable prompt components",
                agent_uuid,
            )
            return False

        context_strategy_id = ""
        strategy_ref = str((agent_payload.get("context_strategy") or {}).get("ref", "")).strip()
        if strategy_ref:
            strategy_payload = self._database.context_strategies.get(strategy_ref)
            if strategy_payload is not None:
                context_strategy_id = self._sync_context_strategy(strategy_payload)

        request = PromptAssemblyRequest(
            caller="pipeline.fallback_responder",
            session_id=bot.session_id,
            instance_id=bot.adapter.instance_id,
            route_id=route_id,
            model_id=model_id,
            model_context_window=model_context_window,
            context_strategy_id=context_strategy_id,
            component_overrides=component_ids,
            template_inputs={
                "user_id": bot.user_id,
                "session_id": bot.session_id,
                "instance_id": bot.adapter.instance_id,
                "platform": bot.platform,
                "message_text": bot.text,
            },
            metadata={
                "trigger": "pipeline_fallback",
                "agent_uuid": agent_uuid,
                "agent_id": str(agent_payload.get("agent_id", "")),
                "persona_uuid": persona_uuid,
                "bot_config_uuid": str(bot_config.get("uuid", "")),
            },
        )

        try:
            assembly = self._prompt_registry.assemble(request)
        except Exception:
            logger.exception(
                "Fallback responder prompt assembly failed for session %s",
                bot.session_id,
            )
            return False

        snapshot = self._prompt_registry.create_snapshot(assembly, request)
        try:
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
                    created_at=snapshot.timestamp,
                )
            )
        except Exception:
            logger.exception(
                "Failed to persist prompt snapshot %s for session %s",
                snapshot.id,
                bot.session_id,
            )

        bot.mark_trigger_read()
        try:
            result = await self._model_runtime.generate(
                ModelRuntimeCall(
                    route_id=route_id or None,
                    model_id=model_id or None,
                    caller="pipeline.fallback_responder",
                    session_id=bot.session_id,
                    instance_id=bot.adapter.instance_id,
                    purpose="default_reply",
                    messages=assembly.messages,
                    tools=assembly.tools,
                    prompt_snapshot_id=snapshot.id,
                    metadata={
                        "agent_uuid": agent_uuid,
                        "agent_id": str(agent_payload.get("agent_id", "")),
                        "persona_uuid": persona_uuid,
                        "bot_config_uuid": str(bot_config.get("uuid", "")),
                    },
                )
            )
        except ModelCallError:
            logger.exception(
                "Fallback responder model call failed for session %s",
                bot.session_id,
            )
            await bot.send("抱歉，我现在暂时无法回复。")
            return True

        response_text = result.text.strip()
        if not response_text:
            logger.warning(
                "Fallback responder produced an empty response for session %s",
                bot.session_id,
            )
            return False

        await bot.send(response_text)
        if bot._msg_log_id is not None and bot.last_response_log_id is not None:
            try:
                self._database.ai_interactions.attach_message_links(
                    result.execution_id,
                    trigger_id=bot._msg_log_id,
                    response_id=bot.last_response_log_id,
                )
            except Exception:
                logger.exception(
                    "Failed to attach message links for execution %s",
                    result.execution_id,
                )
        return True

    # ── Interceptor registration ─────────────────────────────────────

    def add_interceptor(self, interceptor: Interceptor, priority: int = 100) -> None:
        self._interceptors.append((priority, interceptor))
        self._interceptors.sort(key=lambda x: x[0])

    # ── Main entry point ─────────────────────────────────────────────

    async def process_event(
        self,
        event: UnifiedEvent,
        adapter: BaseAdapter,
    ) -> None:
        """Process a single incoming event through the full pipeline.

        Implements dual-track dispatching:

        **Message Track** (message-created, message-updated, etc.):
          1. Parse XML content into MessageElement AST
          2. Build MessageContext with interceptors
          3. Check for pending wait_for_input
          4. Resolve commands and dispatch handlers
          5. Emit event to event bus for post-processing

        **Notice Track** (guild-member-added, friend-request, etc.):
          1. Skip message parsing (no payload)
          2. Skip interceptors (not needed for system events)
          3. Emit directly to event bus with structured resources
          4. Handlers extract resource objects (guild, user, operator, etc.)
        """

        # Fast path for notice events — emit directly to event bus
        if event.is_notice_event:
            logger.debug(
                "Processing notice event: type=%s, self_id=%s, platform=%s",
                event.type,
                event.self_id,
                event.platform,
            )
            # Emit notice to event bus with the raw event (handlers can access resources)
            await self._event_bus.emit(event.type, event)
            return

        # ────────────────────────────────────────────────────────────────
        # Message Event Processing (traditional message track)
        # ────────────────────────────────────────────────────────────────

        # Stage 1: Parse message content into AST
        message = Message()
        if event.message_content:
            message = Message.from_xml(event.message_content)

        # Compute session_id early so we can use it before get_or_create().
        session_id = build_session_id(adapter.instance_id, event)

        # Stage 3c (fast path): wait_for_input resolution — must be handled
        # *before* acquiring the session lock.  The suspended handler already
        # holds the lock; resolving its Future lets it resume and release the
        # lock naturally.  We do not log or process this message further here
        # because the resumed handler owns its continuation.
        if self._waiting_registry.is_waiting(session_id):
            if self._waiting_registry.resolve(session_id, message.get_text()):
                logger.debug("Delivered wait_for_input response for session %s", session_id)
                return

        # Acquire the per-session lock for the remainder of processing.
        # This serialises stateful processing for the same session so that
        # coroutines cannot interleave reads and writes to session state.
        async with self._session_manager.session_lock(session_id):
            bot, should_run_fallback = await self._process_message_event(
                event,
                adapter,
                message,
                session_id,
            )

        if bot is None:
            return

        # Run fallback responder outside the session lock so slow model calls
        # do not serialize the entire session event stream.
        if should_run_fallback:
            await self._run_fallback_responder(bot)

        if bot.command_match is None:
            logger.debug(
                "Processed event in %.1fms (session=%s, command=%s)",
                bot.elapsed_ms,
                bot.session.id,
                "none",
            )

    async def _process_message_event(
        self,
        event: UnifiedEvent,
        adapter: BaseAdapter,
        message: Message,
        session_id: str,
    ) -> tuple[MessageContext | None, bool]:
        """Inner handler for message events, called while holding the session lock.

        Returns:
            tuple[MessageContext | None, bool]:
                - MessageContext when processing reaches dispatch stages, else None.
                - True when fallback responder should run after lock release.
        """

        # Stage 2: Context enrichment
        session = self._session_manager.get_or_create(adapter.instance_id, event)
        session.touch()

        permissions = self._permission_engine.resolve(
            instance_id=adapter.instance_id,
            session_id=session.id,
            user_id=event.sender_id or "",
            session_base_group=session.permission_group,
        )

        bot = MessageContext(
            event=event,
            message=message,
            session=session,
            adapter=adapter,
            permissions=permissions,
            waiting_registry=self._waiting_registry,
            database=self._database,
            context_manager=self._context_manager,
        )

        # Persist incoming user message to message_logs
        if self._database is not None and event.is_message_event:
            try:
                is_mentioned = any(
                    el.type == "at" and el.attrs.get("id") == event.self_id
                    for el in message.elements
                )
                content_json = json.dumps(
                    [el.model_dump(mode="json") for el in message.elements],
                    ensure_ascii=False,
                )
                msg_log_id = self._database.message_logs.insert(
                    record := MessageLogRecord(
                        session_id=session.id,
                        platform_msg_id=event.message.id if event.message is not None else "",
                        sender_id=event.sender_id or "",
                        sender_name=((event.user.name or "") if event.user is not None else ""),
                        content_json=content_json,
                        raw_text=message.get_text(),
                        role="user",
                        is_read=False,
                        is_mentioned=is_mentioned,
                        created_at=time.time() * 1000,
                    )
                )
                record.id = msg_log_id
                if self._context_manager is not None:
                    self._context_manager.track_message_record(record, platform=event.platform)
                bot._msg_log_id = msg_log_id
            except Exception:
                logger.exception("Failed to persist user message to message_logs")

        if self._audit_logger:
            self._audit_logger.log_message(
                event_type=event.type,
                plugin_id="",
                user_id=event.sender_id or "",
                session_id=session.id,
                instance_id=adapter.instance_id,
                metadata={
                    "platform": event.platform,
                    "modality": summarize_message_modalities(bot.elements),
                    "message_id": event.message.id if event.message is not None else "",
                },
            )

        # Stage 3: Interceptors
        for _priority, interceptor in self._interceptors:
            try:
                allow = await interceptor(bot)
                if not allow:
                    logger.debug("Interceptor blocked event: %s", interceptor.__name__)
                    return None, False
            except Exception:
                logger.exception("Interceptor error: %s", interceptor.__name__)
                return None, False

        # Stage 3b: Check if session is muted
        if session.is_muted:
            logger.debug("Session %s is muted, skipping", session.id)
            return None, False

        # Stage 3c: Command resolution
        plain_text = bot.text
        match = self._command_registry.resolve(plain_text, session.config.prefixes)

        if match is not None:
            bot.command_match = match

            # Permission check for command
            permission_granted = True
            if match.command.permission:
                permission_granted = bot.has_permission(match.command.permission)
                if not permission_granted:
                    logger.debug(
                        "Permission denied for %s: requires %s",
                        match.command.name,
                        match.command.permission,
                    )
                    await bot.send(f"权限不足：需要 {match.command.permission}")

                    # Audit log: permission denied
                    if self._audit_logger:
                        self._audit_logger.log_command(
                            command_name=match.command.name,
                            plugin_id=match.command.owner,
                            user_id=event.sender_id or "",
                            session_id=session.id,
                            instance_id=adapter.instance_id,
                            permission_required=match.command.permission,
                            permission_granted=False,
                            execution_time_ms=bot.elapsed_ms,
                            success=False,
                            error="Permission denied",
                        )
                    return None, False

            # Execute command handler
            cmd_start = time.monotonic()
            cmd_time = 0.0
            success = True
            error = ""

            try:
                handler_result = await match.command.handler(bot, match.raw_args)
                if handler_result is not None:
                    logger.warning(
                        "Command handler %s returned a value that was ignored; use bot.send()",
                        match.command.name,
                    )
                cmd_time = (time.monotonic() - cmd_start) * 1000.0
                logger.debug(
                    "Command %s (plugin=%s) executed in %.1fms",
                    match.command.name,
                    match.command.owner,
                    cmd_time,
                )
            except Exception as e:
                cmd_time = (time.monotonic() - cmd_start) * 1000.0
                success = False
                error = str(e)
                logger.exception("Command handler error: %s", match.command.name)

            # Audit log: command executed (for both success and error cases)
            if self._audit_logger:
                self._audit_logger.log_command(
                    command_name=match.command.name,
                    plugin_id=match.command.owner,
                    user_id=event.sender_id or "",
                    session_id=session.id,
                    instance_id=adapter.instance_id,
                    permission_required=match.command.permission,
                    permission_granted=permission_granted,
                    execution_time_ms=cmd_time,
                    success=success,
                    error=error,
                    metadata={
                        "raw_args": match.raw_args[:100]
                        if match.raw_args
                        else "",  # Truncate for safety
                        "message_count": len(bot._sent_messages),
                    },
                )

            # Persist session state changes made by the command handler.
            # (Previously missing — state modifications were lost after commands.)
            self._session_manager.update(session)
            return bot, False

        # Stage 3d: No command matched — dispatch to event bus
        event_results = await self._event_bus.emit(event.type, bot)
        if event_results:
            logger.warning(
                "Event handlers returned values for %s; return values are ignored, use bot.send()",
                event.type,
            )

        # Stage 4: Post-processing
        self._session_manager.update(session)

        # For group messages with attention scheduler active, route to attention
        # system instead of the legacy fallback responder.
        if (
            self._attention_scheduler is not None
            and not bot.is_private
            and not bot._sent_messages
            and not bot.is_stopped
            and bot._msg_log_id is not None
        ):
            is_mentioned = any(
                el.type == "at" and el.attrs.get("id") == event.self_id
                for el in message.elements
            )
            # Fire-and-forget: attention accumulation runs async
            asyncio.create_task(
                self._attention_scheduler.on_message(
                    session_id,
                    bot._msg_log_id,
                    event.sender_id or "",
                    is_mentioned=is_mentioned,
                    is_reply_to_bot=bot.is_reply_to_bot(),
                ),
                name=f"attention-{session_id}",
            )
            return bot, False  # Don't run fallback responder

        return bot, (not bot._sent_messages and not bot.is_stopped)
