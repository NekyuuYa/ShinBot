"""Message handler context and interactive input primitives."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

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
    from shinbot.persistence.engine import DatabaseManager

logger = get_logger(__name__)


class WaitingInputRegistry:
    """Tracks sessions that are waiting for the next user message."""

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
        return self._stopped

    @property
    def elapsed_ms(self) -> float:
        return (time.monotonic() - self.start_time) * 1000

    @property
    def last_response_log_id(self) -> int | None:
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

        fut = self._waiting_registry.register(self.session_id)
        if timeout is not None:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        return await fut


Interceptor = Callable[[MessageContext], Coroutine[Any, Any, bool]]
