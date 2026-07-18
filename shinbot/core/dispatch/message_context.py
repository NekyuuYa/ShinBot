"""Message handler context and interactive input primitives."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from shinbot.core.dispatch.agent_identity import SessionKey, SessionKeyFactory
from shinbot.core.message_analysis import is_self_mentioned
from shinbot.core.message_routes import CommandMatch
from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.core.security.permission import check_permission
from shinbot.core.state.session import Session
from shinbot.persistence.records import MessageLogRecord
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.logger import get_logger

if TYPE_CHECKING:
    from shinbot.core.application.bots_config import BotBindingConfig, BotServiceConfig
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__, source="dispatch", color="cyan")


@dataclass(slots=True, frozen=True)
class WaitingInputScope:
    """Bind one legacy waiter to its base and optional Actor routing identity."""

    legacy_session_id: str
    session_key: SessionKey | None = None

    def __post_init__(self) -> None:
        """Normalize the base session identifier used by legacy waiters."""

        legacy_session_id = str(self.legacy_session_id or "").strip()
        if not legacy_session_id:
            raise ValueError("legacy_session_id must not be empty")
        if self.session_key is not None and not isinstance(self.session_key, SessionKey):
            raise TypeError("session_key must be a SessionKey or None")
        object.__setattr__(self, "legacy_session_id", legacy_session_id)

    @classmethod
    def from_routing_identity(
        cls,
        *,
        legacy_session_id: str,
        bot_id: str = "",
        bot_session_id: str = "",
    ) -> WaitingInputScope:
        """Build the exact waiter scope from the same identity ingress owns."""

        return cls(
            legacy_session_id=legacy_session_id,
            session_key=SessionKeyFactory().create(
                bot_config_id=bot_id,
                bot_id=bot_id,
                bot_session_id=bot_session_id,
                base_session_id=legacy_session_id,
            ),
        )

    def matches(self, other: WaitingInputScope) -> bool:
        """Return whether two scopes may safely share one legacy waiter slot."""

        if self.legacy_session_id != other.legacy_session_id:
            return False
        return self.session_key == other.session_key


@dataclass(slots=True, frozen=True)
class WaitingInputLease:
    """Opaque single-slot claim held by one interactive handler."""

    scope: WaitingInputScope
    token: str
    future: asyncio.Future[str]
    managed: bool


@dataclass(slots=True, frozen=True)
class WaitingInputLeaseInspection:
    """Read-only local ownership facts used before a lifecycle freeze."""

    scope: WaitingInputScope
    managed: bool
    owner_task_done: bool


class WaitingInputConsumeDisposition(StrEnum):
    """Outcome from one atomic attempt to consume a legacy waiter."""

    CONSUMED = "consumed"
    ABSENT = "absent"
    FROZEN = "frozen"
    SCOPE_MISMATCH = "scope_mismatch"


@dataclass(slots=True, frozen=True)
class WaitingInputFreezeTicket:
    """Opaque local freeze authority for one scoped legacy waiter slot."""

    scope: WaitingInputScope
    cutover_id: str
    token: str


@dataclass(slots=True, frozen=True)
class WaitingInputQuiescenceReceipt:
    """Observable result from waiting for a locally frozen waiter to stop."""

    ticket: WaitingInputFreezeTicket
    quiescent: bool
    reason: str = ""


class WaitingInputConflict(RuntimeError):
    """Raised when a session attempts to register more than one live waiter."""


class WaitingInputFrozen(RuntimeError):
    """Raised when a local cutover freeze rejects a new waiter registration."""


class WaitingInputScopeConflict(RuntimeError):
    """Raised when one base session is associated with conflicting scopes."""


class WaitingInputFreezeError(RuntimeError):
    """Raised when a freeze ticket no longer names the current local freeze."""


@dataclass(slots=True)
class _WaitingInputLeaseState:
    """Mutable lifecycle evidence retained until the handler releases its lease."""

    lease: WaitingInputLease
    owner_task: asyncio.Task[Any] | None
    finalized: asyncio.Event
    open: bool = True


@dataclass(slots=True)
class _WaitingInputFreezeState:
    """One immutable snapshot of leases that must drain before a cutover."""

    ticket: WaitingInputFreezeTicket
    leases: tuple[_WaitingInputLeaseState, ...]


class WaitingInputRegistry:
    """Track one tokenized interactive waiter slot per legacy base session.

    The registry is intentionally process-local. Its freeze/quiescence API is
    a future lifecycle-controller primitive, not a replacement for a durable
    admission fence or a cross-process ingress barrier.

    A freeze is deliberately base-session-wide. Legacy handlers and their
    session lock share only that base identity, so a local drain cannot accept
    another bot scope for the same base session while one scope is frozen.
    """

    def __init__(self) -> None:
        self._open_by_session: dict[str, _WaitingInputLeaseState] = {}
        self._lease_by_session: dict[str, _WaitingInputLeaseState] = {}
        self._lease_by_token: dict[str, _WaitingInputLeaseState] = {}
        self._freeze_by_session: dict[str, _WaitingInputFreezeState] = {}

    def is_waiting(self, session_id: str) -> bool:
        """Return whether a session has an open waiter that ingress may consume."""

        state = self._open_by_session.get(session_id)
        return state is not None and state.open

    def open_scope(self, session_id: str) -> WaitingInputScope | None:
        """Return the scope of one open waiter without consuming it."""

        state = self._open_by_session.get(session_id)
        return state.lease.scope if state is not None and state.open else None

    def active_lease_inspection(
        self,
        session_id: str,
    ) -> WaitingInputLeaseInspection | None:
        """Return drain-relevant ownership facts for a live handler lease."""

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id must not be empty")
        state = self._lease_by_session.get(normalized_session_id)
        if state is None:
            return None
        owner_task = state.owner_task
        return WaitingInputLeaseInspection(
            scope=state.lease.scope,
            managed=state.lease.managed,
            owner_task_done=owner_task is None or owner_task.done(),
        )

    def is_frozen(self, session_id: str) -> bool:
        """Return whether local lifecycle control froze this base session slot."""

        return session_id in self._freeze_by_session

    def active_freeze_ticket(
        self,
        session_id: str,
    ) -> WaitingInputFreezeTicket | None:
        """Return the current local waiter freeze without changing its state."""

        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id must not be empty")
        frozen = self._freeze_by_session.get(normalized_session_id)
        return None if frozen is None else frozen.ticket

    def register(self, session_id: str) -> asyncio.Future[str]:
        """Register an unscoped compatibility waiter and return its Future.

        New framework code should call :meth:`acquire` and always release the
        returned lease in ``finally``. Compatibility waiters are auto-released
        on Future completion and therefore cannot establish handler quiescence.
        """

        lease = self.acquire(
            WaitingInputScope(session_id),
            track_owner=False,
        )
        lease.future.add_done_callback(lambda _future: self.release(lease))
        return lease.future

    def acquire(
        self,
        scope: WaitingInputScope,
        *,
        owner_task: asyncio.Task[Any] | None = None,
        track_owner: bool = True,
    ) -> WaitingInputLease:
        """Acquire one scoped lease for an interactive handler.

        Raises:
            WaitingInputConflict: If an earlier handler still owns the slot.
            WaitingInputFrozen: If lifecycle control froze the base session.
        """

        if not isinstance(scope, WaitingInputScope):
            raise TypeError("scope must be a WaitingInputScope")
        session_id = scope.legacy_session_id
        if session_id in self._freeze_by_session:
            raise WaitingInputFrozen(
                f"session {session_id!r} is frozen for lifecycle quiescence"
            )
        existing = self._lease_by_session.get(session_id)
        if existing is not None:
            if not existing.lease.scope.matches(scope):
                raise WaitingInputScopeConflict(
                    f"session {session_id!r} already belongs to another waiter scope"
                )
            raise WaitingInputConflict(
                f"session {session_id!r} already waits for user input"
            )
        if track_owner and owner_task is None:
            owner_task = asyncio.current_task()
        managed = track_owner and owner_task is not None
        if not managed:
            owner_task = None
        loop = asyncio.get_event_loop()
        future: asyncio.Future[str] = loop.create_future()
        lease = WaitingInputLease(
            scope=scope,
            token=uuid.uuid4().hex,
            future=future,
            managed=managed,
        )
        state = _WaitingInputLeaseState(
            lease=lease,
            owner_task=owner_task,
            finalized=asyncio.Event(),
        )
        self._open_by_session[session_id] = state
        self._lease_by_session[session_id] = state
        self._lease_by_token[lease.token] = state
        return lease

    def try_consume_open(
        self,
        scope: WaitingInputScope,
        text: str,
    ) -> WaitingInputConsumeDisposition:
        """Atomically consume an open waiter only when its scope still matches."""

        if not isinstance(scope, WaitingInputScope):
            raise TypeError("scope must be a WaitingInputScope")
        session_id = scope.legacy_session_id
        if session_id in self._freeze_by_session:
            return WaitingInputConsumeDisposition.FROZEN
        state = self._open_by_session.get(session_id)
        if state is None or not state.open:
            return WaitingInputConsumeDisposition.ABSENT
        if not state.lease.scope.matches(scope):
            return WaitingInputConsumeDisposition.SCOPE_MISMATCH
        if state.lease.future.done():
            # A timeout or cancellation may finish the Future before its
            # owning handler reaches ``finally``. It is no longer eligible for
            # delivery, but its exact lease remains until that cleanup runs.
            state.open = False
            self._open_by_session.pop(session_id, None)
            return WaitingInputConsumeDisposition.ABSENT
        state.open = False
        self._open_by_session.pop(session_id, None)
        if not state.lease.future.done():
            state.lease.future.set_result(text)
        return WaitingInputConsumeDisposition.CONSUMED

    def resolve(self, session_id: str, text: str) -> bool:
        """Consume one compatibility waiter and return whether it accepted text."""

        return (
            self.try_consume_open(WaitingInputScope(session_id), text)
            is WaitingInputConsumeDisposition.CONSUMED
        )

    def cancel(self, session_id: str) -> None:
        """Cancel one unscoped compatibility waiter by its legacy session id."""

        state = self._open_by_session.get(session_id)
        if state is None or state.lease.scope.session_key is not None:
            return
        self._open_by_session.pop(session_id, None)
        state.open = False
        if not state.lease.future.done():
            state.lease.future.cancel()

    def release(self, lease: WaitingInputLease) -> bool:
        """Release one exact lease after its owning handler exits its wait block."""

        state = self._lease_by_token.get(lease.token)
        if state is None or state.lease is not lease:
            return False
        session_id = lease.scope.legacy_session_id
        if self._open_by_session.get(session_id) is state:
            self._open_by_session.pop(session_id, None)
        if self._lease_by_session.get(session_id) is state:
            self._lease_by_session.pop(session_id, None)
        self._lease_by_token.pop(lease.token, None)
        state.open = False
        if not state.lease.future.done():
            state.lease.future.cancel()
        state.finalized.set()
        return True

    def freeze(
        self,
        scope: WaitingInputScope,
        *,
        cutover_id: str,
    ) -> WaitingInputFreezeTicket:
        """Freeze one local slot, reject new waiters, and cancel its open Future."""

        if not isinstance(scope, WaitingInputScope):
            raise TypeError("scope must be a WaitingInputScope")
        normalized_cutover_id = str(cutover_id or "").strip()
        if not normalized_cutover_id:
            raise ValueError("cutover_id must not be empty")
        session_id = scope.legacy_session_id
        existing_freeze = self._freeze_by_session.get(session_id)
        if existing_freeze is not None:
            if (
                existing_freeze.ticket.scope == scope
                and existing_freeze.ticket.cutover_id == normalized_cutover_id
            ):
                return existing_freeze.ticket
            raise WaitingInputFrozen(
                f"session {session_id!r} is already frozen for another cutover"
            )
        state = self._lease_by_session.get(session_id)
        if state is not None and not state.lease.scope.matches(scope):
            raise WaitingInputScopeConflict(
                f"session {session_id!r} has a waiter for another scope"
            )
        ticket = WaitingInputFreezeTicket(
            scope=scope,
            cutover_id=normalized_cutover_id,
            token=uuid.uuid4().hex,
        )
        leases = (state,) if state is not None else ()
        self._freeze_by_session[session_id] = _WaitingInputFreezeState(
            ticket=ticket,
            leases=leases,
        )
        if state is not None and state.open:
            self._open_by_session.pop(session_id, None)
            state.open = False
            if not state.lease.future.done():
                state.lease.future.cancel()
        return ticket

    async def await_quiescent(
        self,
        ticket: WaitingInputFreezeTicket,
        *,
        timeout: float | None,
    ) -> WaitingInputQuiescenceReceipt:
        """Wait for frozen leases and their managed handler tasks to finish."""

        frozen = self._require_freeze(ticket)
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be non-negative or None")
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout

        unmanaged = tuple(
            state.lease.token for state in frozen.leases if not state.lease.managed
        )
        if unmanaged:
            return WaitingInputQuiescenceReceipt(
                ticket=ticket,
                quiescent=False,
                reason="unmanaged_waiter",
            )
        owner_tasks = tuple(
            state.owner_task
            for state in frozen.leases
            if state.owner_task is not None
        )
        current_task = asyncio.current_task()
        if current_task is not None and current_task in owner_tasks:
            return WaitingInputQuiescenceReceipt(
                ticket=ticket,
                quiescent=False,
                reason="owner_task_is_current",
            )
        finalized = tuple(
            state.finalized.wait()
            for state in frozen.leases
            if not state.finalized.is_set()
        )
        if finalized and not await self._await_all(finalized, deadline):
            if not self._freeze_is_current(ticket):
                return self._freeze_lost_receipt(ticket)
            return WaitingInputQuiescenceReceipt(
                ticket=ticket,
                quiescent=False,
                reason="lease_release_timeout",
            )
        if not self._freeze_is_current(ticket):
            return self._freeze_lost_receipt(ticket)
        task_waits = tuple(asyncio.shield(task) for task in owner_tasks if not task.done())
        if task_waits and not await self._await_all(task_waits, deadline):
            if not self._freeze_is_current(ticket):
                return self._freeze_lost_receipt(ticket)
            return WaitingInputQuiescenceReceipt(
                ticket=ticket,
                quiescent=False,
                reason="owner_task_timeout",
            )
        if not self._freeze_is_current(ticket):
            return self._freeze_lost_receipt(ticket)
        return WaitingInputQuiescenceReceipt(ticket=ticket, quiescent=True)

    def thaw(self, ticket: WaitingInputFreezeTicket) -> bool:
        """Release a locally quiescent freeze without restoring cancelled input.

        Raises:
            WaitingInputFreezeError: If the snapshot cannot yet prove local
                handler quiescence.
        """

        frozen = self._require_freeze(ticket)
        if not self._freeze_snapshot_is_quiescent(frozen):
            raise WaitingInputFreezeError(
                "cannot thaw a freeze before every scoped handler has stopped"
            )
        self._freeze_by_session.pop(frozen.ticket.scope.legacy_session_id, None)
        return True

    def _require_freeze(
        self,
        ticket: WaitingInputFreezeTicket,
    ) -> _WaitingInputFreezeState:
        if not isinstance(ticket, WaitingInputFreezeTicket):
            raise TypeError("ticket must be a WaitingInputFreezeTicket")
        frozen = self._freeze_by_session.get(ticket.scope.legacy_session_id)
        if frozen is None or frozen.ticket != ticket:
            raise WaitingInputFreezeError("freeze ticket is no longer active")
        return frozen

    def _freeze_is_current(self, ticket: WaitingInputFreezeTicket) -> bool:
        """Return whether one receipt still refers to the active freeze epoch."""

        frozen = self._freeze_by_session.get(ticket.scope.legacy_session_id)
        return frozen is not None and frozen.ticket == ticket

    @staticmethod
    def _freeze_snapshot_is_quiescent(frozen: _WaitingInputFreezeState) -> bool:
        """Return whether a frozen snapshot can safely permit a new waiter."""

        for state in frozen.leases:
            if not state.finalized.is_set() or not state.lease.managed:
                return False
            if state.owner_task is None or not state.owner_task.done():
                return False
        return True

    @staticmethod
    def _freeze_lost_receipt(
        ticket: WaitingInputFreezeTicket,
    ) -> WaitingInputQuiescenceReceipt:
        """Return a negative receipt when the lifecycle epoch changed mid-drain."""

        return WaitingInputQuiescenceReceipt(
            ticket=ticket,
            quiescent=False,
            reason="freeze_lost",
        )

    async def _await_all(
        self,
        awaitables: tuple[Awaitable[Any], ...],
        deadline: float | None,
    ) -> bool:
        """Await a group against one absolute timeout without cancelling it."""

        if not awaitables:
            return True
        timeout = None
        if deadline is not None:
            timeout = max(0.0, deadline - asyncio.get_running_loop().time())
        try:
            await asyncio.wait_for(
                asyncio.gather(*awaitables, return_exceptions=True),
                timeout=timeout,
            )
        except TimeoutError:
            return False
        return True


class MessageContext:
    """Rich context object passed to message handlers and interceptors."""

    def __init__(
        self,
        event: UnifiedEvent,
        message: Message,
        session: Session,
        adapter: BaseAdapter,
        permissions: set[str],
        waiting_registry: WaitingInputRegistry | None = None,
        database: DatabaseManager | None = None,
    ):
        self.event = event
        self.message = message
        self.session = session
        self.adapter = adapter
        self.permissions = permissions
        self._waiting_registry = waiting_registry
        self._database = database
        self.bot_service_config: BotServiceConfig | None = None
        self.bot_binding_config: BotBindingConfig | None = None
        self.bot_session_id: str = ""

        self.command_match: CommandMatch | None = None

        self.start_time: float = time.monotonic()
        self._sent_messages: list[MessageHandle] = []
        self._assistant_log_ids: list[int] = []
        self._stopped: bool = False
        self._msg_log_id: int | None = None

    @property
    def text(self) -> str:
        """Plain text content of the message."""
        return self.message.get_text(self_id=self.event.self_id)

    @property
    def elements(self) -> list[MessageElement]:
        """MessageElement AST nodes contained in this message."""
        return self.message.elements

    @property
    def user_id(self) -> str:
        """Sender's platform user ID."""
        return self.event.sender_id or ""

    @property
    def session_id(self) -> str:
        """Session identifier for the current conversation."""
        return self.session.id

    @property
    def platform(self) -> str:
        """Platform name (e.g. ``onebot_v11``, ``satori``)."""
        return self.event.platform

    @property
    def bot_id(self) -> str:
        """Bot service config ID, or empty string if not set."""
        return self.bot_service_config.id if self.bot_service_config is not None else ""

    @property
    def bot_binding_id(self) -> str:
        """Bot binding config ID, or empty string if not set."""
        return self.bot_binding_config.id if self.bot_binding_config is not None else ""

    @property
    def is_private(self) -> bool:
        """True when the message originates from a private (DM) session."""
        return self.session.is_private

    @property
    def is_mentioned(self) -> bool:
        """True when the bot was @-mentioned in this message."""
        return is_self_mentioned(self.message, self.event.self_id)

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

    def has_permission(self, permission: str) -> bool:
        """Check whether the sender holds a specific permission.

        Args:
            permission: Permission identifier to verify (e.g. ``admin``).

        Returns:
            True if the sender's permission set includes *permission*.
        """
        return check_permission(permission, self.permissions)

    async def send(
        self,
        content: str | Message | list[MessageElement],
    ) -> MessageHandle:
        """Send a response to the current session."""
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

        quote = MessageElement.quote(self.event.message.id)

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
        """Insert an assistant message row into message_logs."""
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
        return record.id

    def stop(self) -> None:
        """Signal that processing should stop."""
        self._stopped = True

    @property
    def is_stopped(self) -> bool:
        """True if ``stop()`` has been called on this context."""
        return self._stopped

    @property
    def elapsed_ms(self) -> float:
        """Milliseconds elapsed since handler start."""
        return (time.monotonic() - self.start_time) * 1000

    @property
    def last_response_log_id(self) -> int | None:
        """Row ID of the last assistant message logged, or None."""
        if not self._assistant_log_ids:
            return None
        return self._assistant_log_ids[-1]

    async def wait_for_input(
        self,
        prompt: str = "",
        timeout: float | None = 60.0,
    ) -> str:
        """Suspend the current handler and wait for the next message in this session."""
        if self._waiting_registry is None:
            raise RuntimeError("wait_for_input is not available in this context")

        if prompt:
            await self.send(prompt)

        scope = WaitingInputScope.from_routing_identity(
            legacy_session_id=self.session_id,
            bot_id=self.bot_id,
            bot_session_id=self.bot_session_id,
        )
        lease = self._waiting_registry.acquire(scope)
        try:
            if timeout is not None:
                return await asyncio.wait_for(lease.future, timeout=timeout)
            return await lease.future
        finally:
            # Token matching ensures a finished handler cannot release a
            # later handler's waiter.
            self._waiting_registry.release(lease)


Interceptor = Callable[[MessageContext], Coroutine[Any, Any, bool]]
