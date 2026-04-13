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
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any

from shinbot.core.adapter_manager import AdapterManager, BaseAdapter, MessageHandle
from shinbot.core.audit import AuditLogger
from shinbot.core.command import CommandMatch, CommandRegistry
from shinbot.core.event_bus import EventBus
from shinbot.core.permission import PermissionEngine, check_permission
from shinbot.core.session import Session, SessionManager
from shinbot.models.elements import Message, MessageElement
from shinbot.models.events import UnifiedEvent

logger = logging.getLogger(__name__)


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
    ):
        self.event = event
        self.message = message
        self.session = session
        self.adapter = adapter
        self.permissions = permissions
        self._waiting_registry = waiting_registry

        # Command resolution result (set during dispatch)
        self.command_match: CommandMatch | None = None

        # Tracking
        self.start_time: float = time.monotonic()
        self._sent_messages: list[MessageHandle] = []
        self._stopped: bool = False

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
        return handle

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
        return handle

    def stop(self) -> None:
        """Signal that processing should stop (no further handlers)."""
        self._stopped = True

    @property
    def is_stopped(self) -> bool:
        return self._stopped

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000

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
    ):
        self._adapter_manager = adapter_manager
        self._session_manager = session_manager
        self._permission_engine = permission_engine
        self._command_registry = command_registry
        self._event_bus = event_bus
        self._audit_logger = audit_logger
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
        """Process a single incoming event through the full pipeline."""

        # Stage 1: Parse message content into AST
        message = Message()
        if event.message_content:
            message = Message.from_xml(event.message_content)

        # Stage 2: Context enrichment
        session = self._session_manager.get_or_create(adapter.instance_id, event)
        session.touch()

        permissions = self._permission_engine.resolve(
            instance_id=adapter.instance_id,
            session_id=session.id,
            user_id=event.sender_id or "",
            session_base_group=session.permission_group,
        )

        ctx = MessageContext(
            event=event,
            message=message,
            session=session,
            adapter=adapter,
            permissions=permissions,
            waiting_registry=self._waiting_registry,
        )

        # Stage 3: Interceptors
        for _priority, interceptor in self._interceptors:
            try:
                allow = await interceptor(ctx)
                if not allow:
                    logger.debug("Interceptor blocked event: %s", interceptor.__name__)
                    return
            except Exception:
                logger.exception("Interceptor error: %s", interceptor.__name__)
                return

        # Stage 3b: Check if session is muted
        if session.is_muted:
            logger.debug("Session %s is muted, skipping", session.id)
            return

        # Stage 3c: Check for pending wait_for_input — deliver and stop
        if event.is_message_event and self._waiting_registry.is_waiting(session.id):
            resolved = self._waiting_registry.resolve(session.id, ctx.text)
            if resolved:
                logger.debug("Delivered wait_for_input response for session %s", session.id)
                return

        # Stage 3d: Command resolution (only for message events)
        if event.is_message_event:
            plain_text = ctx.text
            match = self._command_registry.resolve(plain_text, session.config.prefixes)

            if match is not None:
                ctx.command_match = match

                # Permission check for command
                permission_granted = True
                if match.command.permission:
                    permission_granted = ctx.has_permission(match.command.permission)
                    if not permission_granted:
                        logger.debug(
                            "Permission denied for %s: requires %s",
                            match.command.name,
                            match.command.permission,
                        )
                        await ctx.send(f"权限不足：需要 {match.command.permission}")

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
                                execution_time_ms=ctx.elapsed_ms,
                                success=False,
                                error="Permission denied",
                            )
                        return

                # Execute command handler
                cmd_start = time.monotonic()
                try:
                    handler_result = await match.command.handler(ctx, match.raw_args)
                    if handler_result is not None:
                        logger.warning(
                            "Command handler %s returned a value that was ignored; use ctx.send()",
                            match.command.name,
                        )
                    cmd_time = (time.monotonic() - cmd_start) * 1000.0
                    success = True
                    error = ""

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

                # Audit log: command executed
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
                            "message_count": len(ctx._sent_messages),
                        },
                    )

                return

        # Stage 3d: No command matched — dispatch to event bus
        event_results = await self._event_bus.emit(event.type, ctx)
        if event_results:
            logger.warning(
                "Event handlers returned values for %s; return values are ignored, use ctx.send()",
                event.type,
            )

        # Stage 4: Post-processing
        self._session_manager.update(session)

        logger.debug(
            "Processed event in %.1fms (session=%s, command=%s)",
            ctx.elapsed_ms,
            session.id,
            ctx.command_match.command.name if ctx.command_match else "none",
        )
