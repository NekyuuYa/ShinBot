"""Message workflow pipeline — the core processing engine.

Implements the message workflow specification (01_message_workflow.md).

Pipeline stages:
  1. Ingress & Normalization: adapter → UnifiedEvent + MessageElement AST
  2. Context Enrichment: session resolution, permission merge, context build
  3. Workflow Dispatching: mute gate → interceptors → command/event bus
  4. Post-processing: state sync, attention scheduling, read markers
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING

from shinbot.core.bot_config import ATTENTION_DISABLED_PROFILE, select_response_profile
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.dispatch.message_context import (
    Interceptor,
    MessageContext,
    WaitingInputRegistry,
)
from shinbot.core.message_analysis import is_self_mentioned
from shinbot.core.platform.adapter_manager import AdapterManager, BaseAdapter
from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine
from shinbot.core.state.session import SessionManager, build_session_id
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import Message
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.logger import get_logger
from shinbot.utils.resource_ingress import summarize_message_modalities

if TYPE_CHECKING:
    from shinbot.agent.attention.scheduler import AttentionScheduler
    from shinbot.agent.context import ContextManager
    from shinbot.agent.media import MediaInspectionRunner, MediaService
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)


class MessagePipeline:
    """The main message processing pipeline.

    Orchestrates the full lifecycle:
      1. Parse message content and resolve pending wait_for_input
      2. Build MessageContext and persist the incoming message log
      3. Apply the mute gate and interceptors
      4. Ingest media and update context/audit observers for continuing messages
      5. Dispatch to command handler or event bus
      6. Post-processing (state sync, attention scheduling, read markers)
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
        attention_scheduler: AttentionScheduler | None = None,
        media_service: MediaService | None = None,
        media_inspection_runner: MediaInspectionRunner | None = None,
    ):
        self._adapter_manager = adapter_manager
        self._session_manager = session_manager
        self._permission_engine = permission_engine
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._audit_logger = audit_logger
        self._database = database
        self._context_manager = context_manager
        self._attention_scheduler = attention_scheduler
        self._media_service = media_service
        self._media_inspection_runner = media_inspection_runner
        self._interceptors: list[tuple[int, Interceptor]] = []
        self._waiting_registry = WaitingInputRegistry()

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
          2. Resolve pending wait_for_input before acquiring the session lock
          3. Build MessageContext and persist the incoming message log
          4. Apply mute/interceptor gates
          5. Resolve commands or emit the event to the event bus

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
        # Message event processing
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
            bot = await self._process_message_event(
                event,
                adapter,
                message,
                session_id,
            )

        if bot is None:
            return

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
    ) -> MessageContext | None:
        """Inner handler for message events, called while holding the session lock."""

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

        observed_at = time.time()
        persisted_record: MessageLogRecord | None = None

        # Persist incoming user message to message_logs before any early exits.
        if self._database is not None and event.is_message_event:
            try:
                content_json = json.dumps(
                    [el.model_dump(mode="json") for el in message.elements],
                    ensure_ascii=False,
                )
                msg_log_id = self._database.message_logs.insert(
                    record := MessageLogRecord(
                        session_id=session.id,
                        platform_msg_id=event.message.id if event.message is not None else "",
                        sender_id=event.sender_id or "",
                        sender_name=event.sender_name or "",
                        content_json=content_json,
                        raw_text=message.get_text(self_id=event.self_id),
                        role="user",
                        is_read=False,
                        is_mentioned=is_self_mentioned(message, event.self_id),
                        created_at=observed_at * 1000,
                    )
                )
                record.id = msg_log_id
                bot._msg_log_id = msg_log_id
                persisted_record = record
            except Exception:
                logger.exception("Failed to persist user message to message_logs")

        # Stage 3a: Built-in mute gate
        if session.is_muted:
            logger.debug("Session %s is muted, skipping", session.id)
            return None

        # Stage 3b: Interceptors
        for _priority, interceptor in self._interceptors:
            try:
                allow = await interceptor(bot)
                if not allow:
                    logger.debug("Interceptor blocked event: %s", interceptor.__name__)
                    return None
            except Exception:
                logger.exception("Interceptor error: %s", interceptor.__name__)
                return None

        if self._media_service is not None and bot._msg_log_id is not None:
            try:
                ingested_items = self._media_service.ingest_message_media(
                    session_id=session.id,
                    sender_id=event.sender_id or "",
                    platform_msg_id=event.message.id if event.message is not None else "",
                    elements=message.elements,
                    message_log_id=bot._msg_log_id,
                    seen_at=observed_at,
                )
                if self._media_inspection_runner is not None and any(
                    item.should_request_inspection for item in ingested_items
                ):
                    self._media_inspection_runner.schedule_items(
                        instance_id=adapter.instance_id,
                        session_id=session.id,
                        items=ingested_items,
                    )
            except Exception:
                logger.exception(
                    "Failed to ingest media fingerprints for session %s",
                    session.id,
                )
        if persisted_record is not None and self._context_manager is not None:
            self._context_manager.track_message_record(persisted_record, platform=event.platform)

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
                    return None

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
            return bot

        # Stage 3d: No command matched — dispatch to event bus
        event_results = await self._event_bus.emit(event.type, bot)
        if event_results:
            logger.warning(
                "Event handlers returned values for %s; return values are ignored, use bot.send()",
                event.type,
            )

        # Stage 4: Post-processing
        self._session_manager.update(session)

        # Route natural-language sessions to the attention scheduler. Different
        # immediacy requirements are expressed through response profiles on the
        # same workflow engine.
        response_profile = self._resolve_response_profile(bot)
        handled_by_attention = False
        if self._attention_scheduler is not None:
            handled_by_attention = self._attention_scheduler.schedule_message(
                session_id,
                bot._msg_log_id,
                event.sender_id or "",
                response_profile=response_profile,
                message=message,
                self_platform_id=event.self_id,
                is_reply_to_bot=bot.is_reply_to_bot(),
                already_handled=bool(bot._sent_messages),
                is_stopped=bot.is_stopped,
            )
        if not handled_by_attention and self._database is not None and bot._msg_log_id is not None:
            self._database.message_logs.mark_read(bot._msg_log_id)
            if self._context_manager is not None:
                self._context_manager.mark_read_until(session_id, bot._msg_log_id)
        return bot

    def _resolve_response_profile(self, bot: MessageContext) -> str:
        if self._database is None:
            return ATTENTION_DISABLED_PROFILE if bot.is_private else "balanced"

        bot_config = self._database.bot_configs.get_by_instance_id(bot.adapter.instance_id)
        return select_response_profile(
            bot_config,
            is_private=bot.is_private,
            is_mentioned=bot.is_mentioned,
            is_reply_to_bot=bot.is_reply_to_bot(),
        )
