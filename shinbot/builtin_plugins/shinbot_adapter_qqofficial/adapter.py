"""QQ Official Bot adapter for ShinBot.

This adapter talks directly to QQ Official APIs without using third-party SDKs:
  - Access token: POST /app/getAppAccessToken (bots.qq.com)
  - Gateway:      GET /gateway/bot + WebSocket lifecycle
  - Messaging:    channel/direct/group/c2c OpenAPI endpoints

Ingress payloads are normalized into UnifiedEvent + MessageElement AST,
and egress uses ShinBot's BaseAdapter send/call_api contract.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from urllib.parse import quote

import httpx
import websockets
import websockets.exceptions
from pydantic import BaseModel, Field

from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, Guild, Member, User
from shinbot.utils.logger import get_logger
from shinbot.utils.resource_ingress import download_resource_elements
from shinbot.utils.satori_parser import elements_to_xml

logger = get_logger(__name__)

# QQ Official gateway opcodes
OP_DISPATCH = 0
OP_HEARTBEAT = 1
OP_IDENTIFY = 2
OP_RESUME = 6
OP_RECONNECT = 7
OP_INVALID_SESSION = 9
OP_HELLO = 10
OP_HEARTBEAT_ACK = 11

DEFAULT_INTENTS = (1 << 9) | (1 << 12) | (1 << 25) | (1 << 30)

_MESSAGE_EVENTS: set[str] = {
    "AT_MESSAGE_CREATE",
    "MESSAGE_CREATE",
    "GROUP_AT_MESSAGE_CREATE",
    "C2C_MESSAGE_CREATE",
    "DIRECT_MESSAGE_CREATE",
}

_NOTICE_EVENT_TYPES: dict[str, str] = {
    "GROUP_ADD_ROBOT": "guild-member-added",
    "GROUP_DEL_ROBOT": "guild-member-deleted",
    "GROUP_MSG_REJECT": "notice-group-msg-reject",
    "GROUP_MSG_RECEIVE": "notice-group-msg-receive",
    "FRIEND_ADD": "friend-added",
    "FRIEND_DEL": "friend-deleted",
    "C2C_MSG_REJECT": "notice-c2c-msg-reject",
    "C2C_MSG_RECEIVE": "notice-c2c-msg-receive",
}

_MEDIA_FILE_TYPE: dict[str, int] = {
    "img": 1,
    "video": 2,
    "audio": 3,
    "file": 4,
}

_MENTION_PATTERN = re.compile(r"<@!?(?P<id>[^>]+)>")


class QQOfficialConfig(BaseModel):
    app_id: str = Field(default="")
    app_secret: str = Field(default="")
    intents: int = Field(default=DEFAULT_INTENTS, ge=0)
    sandbox: bool = Field(default=False)
    api_base: str = Field(default="")
    token_base: str = Field(default="https://bots.qq.com")
    ws_url: str = Field(default="")
    reconnect_delay: float = Field(default=5.0, ge=0.0)
    max_reconnects: int = Field(default=-1)
    request_timeout: float = Field(default=20.0, gt=0.0)
    heartbeat_jitter: float = Field(default=0.05, ge=0.0, le=1.0)
    download_resources: bool = Field(default=False)
    resource_cache_dir: str = Field(default="data/temp/resources")
    proactive_msg_seq_min: int = Field(default=1, ge=1)
    proactive_msg_seq_max: int = Field(default=10000, ge=1)


@dataclass(slots=True)
class SessionRoute:
    scene: Literal["channel", "group", "c2c", "direct"]
    channel_id: str | None = None
    guild_id: str | None = None
    group_openid: str | None = None
    openid: str | None = None
    last_message_id: str | None = None
    last_event_id: str | None = None


@dataclass(slots=True)
class MessageRoute:
    session_id: str
    scene: Literal["channel", "group", "c2c", "direct"]
    channel_id: str | None = None
    guild_id: str | None = None
    group_openid: str | None = None
    openid: str | None = None


class QQOfficialAdapter(BaseAdapter):
    def __init__(self, instance_id: str, platform: str, config: QQOfficialConfig):
        super().__init__(instance_id=instance_id, platform=platform)
        self.config = config

        self._running = False
        self._ws: Any | None = None
        self._http: httpx.AsyncClient | None = None
        self._recv_task: asyncio.Task[Any] | None = None
        self._heartbeat_task: asyncio.Task[Any] | None = None

        self._seq: int | None = None
        self._session_id: str = ""
        self._gateway_shards: int = 1
        self._self_id: str = ""

        self._access_token: str = ""
        self._token_expire_at: float = 0.0
        self._token_lock = asyncio.Lock()

        self._session_routes: dict[str, SessionRoute] = {}
        self._message_routes: dict[str, MessageRoute] = {}

        self._resource_cache_dir = Path(self.config.resource_cache_dir)
        self._resource_cache_dir.mkdir(parents=True, exist_ok=True)

    # ── BaseAdapter interface ────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._http = httpx.AsyncClient(timeout=self.config.request_timeout)
        self._recv_task = asyncio.create_task(
            self._connection_loop(),
            name=f"qqofficial-{self.instance_id}",
        )
        logger.info("QQOfficial adapter %s started", self.instance_id)

    async def shutdown(self) -> None:
        self._running = False

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._http is not None:
            await self._http.aclose()
            self._http = None

        logger.info("QQOfficial adapter %s shut down", self.instance_id)

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        route = self._resolve_route(target_session)
        response = await self._send_with_route(route, elements)

        message_id = ""
        if isinstance(response, dict):
            raw_id = response.get("id")
            if raw_id is not None:
                message_id = str(raw_id)

        if message_id:
            self._message_routes[message_id] = MessageRoute(
                session_id=target_session,
                scene=route.scene,
                channel_id=route.channel_id,
                guild_id=route.guild_id,
                group_openid=route.group_openid,
                openid=route.openid,
            )
            route.last_message_id = message_id
            self._session_routes[target_session] = route

        return MessageHandle(
            message_id=message_id,
            adapter_ref=self,
            platform_data={"session_id": target_session},
        )

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        if method == "channel.message.create":
            channel_id = str(params.get("channel_id", ""))
            content = str(params.get("content", ""))
            elements = Message.from_xml(content).elements if content else []
            session_id = self._session_id_from_channel(channel_id)
            handle = await self.send(session_id, elements)
            return [{"id": handle.message_id}] if handle.message_id else []

        if method == "message.delete":
            return await self._delete_message(params)

        if method == "message.update":
            session_id = str(params.get("session_id", ""))
            message_id = str(params.get("message_id", ""))
            elements = params.get("elements", [])

            if message_id:
                await self._delete_message({"message_id": message_id, "session_id": session_id})

            if session_id and isinstance(elements, list):
                return await self.send(session_id, elements)
            return {"ok": True}

        if method == "member.kick":
            guild_id = str(params.get("guild_id") or params.get("group_id") or "")
            user_id = str(params.get("user_id") or "")
            return await self._request(
                "DELETE",
                f"/guilds/{_path_segment(guild_id)}/members/{_path_segment(user_id)}",
            )

        if method == "member.mute":
            guild_id = str(params.get("guild_id") or params.get("group_id") or "")
            user_id = str(params.get("user_id") or "")
            mute_payload = {
                "mute_seconds": str(params.get("duration", 0)),
            }
            return await self._request(
                "PATCH",
                f"/guilds/{_path_segment(guild_id)}/members/{_path_segment(user_id)}/mute",
                json_payload=mute_payload,
            )

        if method == "internal.qqofficial.request":
            internal_method = str(params.get("http_method", "GET")).upper()
            path = str(params.get("path", ""))
            query = params.get("params")
            payload = params.get("json")
            return await self._request(
                internal_method,
                path,
                query=query if isinstance(query, dict) else None,
                json_payload=payload,
            )

        if method.startswith("internal.qqofficial."):
            action = method.split(".", 2)[-1]
            return await self._request(
                "POST",
                f"/{action.replace('.', '/')}",
                json_payload=params,
            )

        return await self._request(
            "POST",
            f"/{method.replace('.', '/')}",
            json_payload=params,
        )

    async def get_capabilities(self) -> dict[str, Any]:
        return {
            "elements": [
                "text",
                "at",
                "img",
                "audio",
                "video",
                "file",
                "quote",
                "sb:ark",
                "qq:markdown",
                "qq:keyboard",
            ],
            "actions": [
                "channel.message.create",
                "message.delete",
                "message.update",
                "member.kick",
                "member.mute",
                "internal.qqofficial.request",
            ],
            "limits": {},
            "platform": self.platform,
        }

    # ── Gateway lifecycle ────────────────────────────────────────────

    async def _connection_loop(self) -> None:
        attempts = 0
        while self._running:
            try:
                await self._connect_and_receive()
                if attempts > 0:
                    logger.info("QQOfficial %s reconnected successfully", self.instance_id)
                attempts = 0
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if not self._running:
                    break
                attempts += 1
                logger.warning(
                    "QQOfficial %s reconnecting in %.1fs (attempt %d): %s",
                    self.instance_id,
                    self.config.reconnect_delay,
                    attempts,
                    exc,
                )
                if self.config.max_reconnects >= 0 and attempts > self.config.max_reconnects:
                    logger.error("QQOfficial %s reached max reconnect attempts", self.instance_id)
                    break
                await asyncio.sleep(self.config.reconnect_delay)

    async def _connect_and_receive(self) -> None:
        ws_url = await self._resolve_ws_url()
        async with websockets.connect(ws_url) as ws:
            self._ws = ws
            logger.info("QQOfficial %s connected to %s", self.instance_id, ws_url)
            try:
                async for raw in ws:
                    await self._handle_gateway_raw(raw)
            finally:
                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                self._heartbeat_task = None
                self._ws = None

    async def _resolve_ws_url(self) -> str:
        if self.config.ws_url.strip():
            return self.config.ws_url.strip()
        data = await self._request("GET", "/gateway/bot")
        if not isinstance(data, dict) or not data.get("url"):
            raise RuntimeError(f"Invalid gateway response: {data!r}")
        shards = data.get("shards")
        if isinstance(shards, int) and shards > 0:
            self._gateway_shards = shards
        return str(data["url"])

    async def _handle_gateway_raw(self, raw: str | bytes) -> None:
        try:
            payload = json.loads(raw)
        except Exception:
            logger.warning("QQOfficial %s received non-JSON gateway frame", self.instance_id)
            return

        op = payload.get("op")
        seq = payload.get("s")
        if isinstance(seq, int):
            self._seq = seq

        if op == OP_HELLO:
            data = payload.get("d") if isinstance(payload.get("d"), dict) else {}
            await self._handle_hello(data)
            return

        if op == OP_HEARTBEAT_ACK:
            return

        if op == OP_RECONNECT:
            raise RuntimeError("Gateway requested reconnect")

        if op == OP_INVALID_SESSION:
            self._session_id = ""
            self._seq = None
            await asyncio.sleep(random.uniform(1.0, 2.0))
            await self._identify()
            return

        if op == OP_DISPATCH:
            event_type = str(payload.get("t") or "")
            data = payload.get("d") if isinstance(payload.get("d"), dict) else {}
            await self._handle_dispatch(event_type, data)

    async def _handle_hello(self, data: dict[str, Any]) -> None:
        heartbeat_ms = _safe_int(data.get("heartbeat_interval")) or 30000
        heartbeat_seconds = max(1.0, heartbeat_ms / 1000.0)

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(heartbeat_seconds),
            name=f"qqofficial-heartbeat-{self.instance_id}",
        )

        if self._session_id and self._seq is not None:
            await self._resume()
        else:
            await self._identify()

    async def _identify(self) -> None:
        await self._ensure_access_token()
        if self._ws is None:
            return

        shard_count = max(self._gateway_shards, 1)

        payload = {
            "op": OP_IDENTIFY,
            "d": {
                "token": f"QQBot {self._access_token}",
                "intents": self.config.intents,
                "shard": [0, shard_count],
            },
        }
        await self._ws.send(json.dumps(payload))

    async def _resume(self) -> None:
        await self._ensure_access_token()
        if self._ws is None:
            return
        payload = {
            "op": OP_RESUME,
            "d": {
                "token": f"QQBot {self._access_token}",
                "session_id": self._session_id,
                "seq": self._seq or 0,
            },
        }
        await self._ws.send(json.dumps(payload))

    async def _heartbeat_loop(self, interval_seconds: float) -> None:
        # Add small jitter to avoid synchronized bursts after reconnect storms.
        if self.config.heartbeat_jitter > 0:
            jitter = interval_seconds * self.config.heartbeat_jitter * random.random()
            await asyncio.sleep(jitter)

        while self._running and self._ws is not None:
            try:
                await self._ws.send(json.dumps({"op": OP_HEARTBEAT, "d": self._seq}))
            except websockets.exceptions.ConnectionClosed:
                return
            except Exception:
                logger.exception("QQOfficial %s heartbeat failed", self.instance_id)
                return
            await asyncio.sleep(interval_seconds)

    # ── Ingress decode ───────────────────────────────────────────────

    async def _handle_dispatch(self, event_type: str, data: dict[str, Any]) -> None:
        if event_type == "READY":
            self._session_id = str(data.get("session_id", ""))
            user = data.get("user", {}) if isinstance(data.get("user"), dict) else {}
            self._self_id = str(user.get("id", ""))
            logger.info(
                "QQOfficial %s READY: self_id=%s session_id=%s",
                self.instance_id,
                self._self_id,
                self._session_id,
            )
            return

        if self._event_callback is None:
            return

        event = await self._decode_dispatch_event(event_type, data)
        if event is None:
            return

        if self.config.download_resources and event.message is not None and event.message.content:
            try:
                message = Message.from_xml(event.message.content)
                elements = await download_resource_elements(
                    message.elements, self._resource_cache_dir
                )
                event.message = event.message.model_copy(
                    update={"content": elements_to_xml(elements)}
                )
            except Exception:
                logger.exception(
                    "QQOfficial %s failed to download message resources", self.instance_id
                )

        try:
            await self._event_callback(event)
        except Exception:
            logger.exception("QQOfficial %s event callback raised", self.instance_id)

    async def _decode_dispatch_event(
        self,
        event_type: str,
        data: dict[str, Any],
    ) -> UnifiedEvent | None:
        normalized = event_type.upper()

        if normalized in _MESSAGE_EVENTS:
            return await self._decode_message_event(normalized, data)

        if normalized in _NOTICE_EVENT_TYPES:
            return self._decode_notice_event(normalized, data)

        return None

    async def _decode_message_event(
        self,
        event_type: str,
        data: dict[str, Any],
    ) -> UnifiedEvent:
        author = data.get("author", {}) if isinstance(data.get("author"), dict) else {}
        message_id = str(data.get("id", ""))
        event_id = str(data.get("event_id", ""))
        timestamp = self._normalize_timestamp(data.get("timestamp"))

        user: User
        member: Member | None = None
        channel: Channel
        guild: Guild | None = None
        route: SessionRoute

        if event_type == "GROUP_AT_MESSAGE_CREATE":
            group_openid = str(data.get("group_openid", ""))
            user_id = str(author.get("member_openid") or "")
            user = User(id=user_id)
            channel = Channel(id=group_openid, type=0)
            guild = Guild(id=group_openid)
            member = Member(user=user)
            route = SessionRoute(
                scene="group",
                group_openid=group_openid,
                channel_id=group_openid,
                guild_id=group_openid,
                last_message_id=message_id,
                last_event_id=event_id,
            )
        elif event_type == "C2C_MESSAGE_CREATE":
            user_id = str(author.get("user_openid") or "")
            user = User(id=user_id)
            channel = Channel(id=f"private:{user_id}", type=1)
            route = SessionRoute(
                scene="c2c",
                openid=user_id,
                last_message_id=message_id,
                last_event_id=event_id,
            )
        elif event_type == "DIRECT_MESSAGE_CREATE":
            user_id = str(author.get("id") or "")
            guild_id = str(data.get("guild_id") or "")
            user = User(id=user_id, name=author.get("username"))
            channel = Channel(id=f"private:{user_id}", type=1)
            guild = Guild(id=guild_id) if guild_id else None
            route = SessionRoute(
                scene="direct",
                guild_id=guild_id or None,
                openid=user_id or None,
                last_message_id=message_id,
                last_event_id=event_id,
            )
        else:
            # AT_MESSAGE_CREATE / MESSAGE_CREATE (channel context)
            user_id = str(author.get("id") or "")
            channel_id = str(data.get("channel_id") or "")
            guild_id = str(data.get("guild_id") or "")
            user = User(id=user_id, name=author.get("username"))
            channel = Channel(id=channel_id, type=0)
            guild = Guild(id=guild_id) if guild_id else None
            member_data = data.get("member") if isinstance(data.get("member"), dict) else {}
            member = Member(nick=member_data.get("nick"), user=user)
            route = SessionRoute(
                scene="channel",
                channel_id=channel_id,
                guild_id=guild_id or None,
                last_message_id=message_id,
                last_event_id=event_id,
            )

        elements = self._message_elements_from_payload(data)
        content_xml = elements_to_xml(elements)

        event = UnifiedEvent(
            id=_safe_int(data.get("id")),
            type="message-created",
            self_id=self._self_id,
            platform=self.platform,
            timestamp=timestamp,
            user=user,
            member=member,
            channel=channel,
            guild=guild,
            message=MessagePayload(id=message_id, content=content_xml),
            event_id=event_id or None,
        )

        session_id = self._session_id_from_event(event)
        self._session_routes[session_id] = route

        if message_id:
            self._message_routes[message_id] = MessageRoute(
                session_id=session_id,
                scene=route.scene,
                channel_id=route.channel_id,
                guild_id=route.guild_id,
                group_openid=route.group_openid,
                openid=route.openid,
            )

        return event

    def _decode_notice_event(self, event_type: str, data: dict[str, Any]) -> UnifiedEvent:
        notice_type = _NOTICE_EVENT_TYPES[event_type]
        timestamp = self._normalize_timestamp(data.get("timestamp"))

        user_id = str(data.get("openid") or data.get("op_member_openid") or "")
        group_openid = str(data.get("group_openid") or "")

        event = UnifiedEvent(
            id=_safe_int(data.get("id")) or _safe_int(data.get("event_ts")),
            type=notice_type,
            self_id=self._self_id,
            platform=self.platform,
            timestamp=timestamp,
            user=User(id=user_id) if user_id else None,
            guild=Guild(id=group_openid) if group_openid else None,
            channel=Channel(id=group_openid, type=0) if group_openid else None,
            event_id=str(data.get("event_id") or "") or None,
            **self._event_extra_payload(data),
        )
        return event

    def _message_elements_from_payload(self, data: dict[str, Any]) -> list[MessageElement]:
        elements: list[MessageElement] = []

        message_reference = data.get("message_reference")
        if isinstance(message_reference, dict):
            ref_id = str(message_reference.get("message_id") or "")
            if ref_id:
                elements.append(MessageElement.quote(ref_id))

        content = str(data.get("content") or "")
        mentions = data.get("mentions") if isinstance(data.get("mentions"), list) else []
        elements.extend(self._content_with_mentions(content, mentions))

        attachments = data.get("attachments")
        if isinstance(attachments, list):
            for item in attachments:
                if not isinstance(item, dict):
                    continue
                url = self._normalize_attachment_url(str(item.get("url") or ""))
                if not url:
                    continue
                content_type = str(item.get("content_type") or "").lower()
                filename = item.get("filename")
                if content_type.startswith("image/"):
                    elements.append(MessageElement.img(url, name=filename))
                elif content_type.startswith("audio/"):
                    elements.append(MessageElement.audio(url, name=filename))
                elif content_type.startswith("video/"):
                    elements.append(MessageElement.video(url, name=filename))
                else:
                    elements.append(MessageElement.file(url, name=filename))

        markdown = data.get("markdown")
        if isinstance(markdown, dict):
            md_content = str(markdown.get("content") or "")
            if md_content:
                elements.append(MessageElement(type="qq:markdown", attrs={"content": md_content}))

        keyboard = data.get("keyboard")
        if isinstance(keyboard, dict):
            elements.append(
                MessageElement(
                    type="qq:keyboard",
                    attrs={"data": json.dumps(keyboard, ensure_ascii=False)},
                )
            )

        ark = data.get("ark")
        if isinstance(ark, dict):
            elements.append(
                MessageElement(
                    type="sb:ark",
                    attrs={"data": json.dumps(ark, ensure_ascii=False)},
                )
            )

        embed = data.get("embed")
        if isinstance(embed, dict):
            elements.append(
                MessageElement(
                    type="sb:ark",
                    attrs={"data": json.dumps({"embed": embed}, ensure_ascii=False)},
                )
            )

        return elements

    def _content_with_mentions(
        self,
        content: str,
        mentions: list[Any],
    ) -> list[MessageElement]:
        if not content:
            return []

        mention_ids: set[str] = set()
        for mention in mentions:
            if not isinstance(mention, dict):
                continue
            mention_id = mention.get("id")
            if mention_id is not None:
                mention_ids.add(str(mention_id))

        elements: list[MessageElement] = []
        cursor = 0

        for match in _MENTION_PATTERN.finditer(content):
            if match.start() > cursor:
                plain = content[cursor : match.start()]
                if plain:
                    elements.append(MessageElement.text(plain))

            mention_id = str(match.group("id") or "")
            if mention_id in {"all", "everyone"}:
                elements.append(MessageElement.at(type="all"))
            elif not mention_ids or mention_id in mention_ids:
                elements.append(MessageElement.at(id=mention_id))
            else:
                elements.append(MessageElement.text(match.group(0)))

            cursor = match.end()

        if cursor < len(content):
            tail = content[cursor:]
            if tail:
                elements.append(MessageElement.text(tail))

        if not elements:
            elements.append(MessageElement.text(content))
        return elements

    # ── Egress encode ────────────────────────────────────────────────

    async def _send_with_route(self, route: SessionRoute, elements: list[MessageElement]) -> Any:
        payload = await self._build_send_payload(route, elements)
        if payload is None:
            return {}

        if route.scene == "group":
            if not route.group_openid:
                raise RuntimeError("Group route missing group_openid")
            return await self._request(
                "POST",
                f"/v2/groups/{_path_segment(route.group_openid)}/messages",
                json_payload=payload,
            )

        if route.scene == "c2c":
            if not route.openid:
                raise RuntimeError("C2C route missing openid")
            return await self._request(
                "POST",
                f"/v2/users/{_path_segment(route.openid)}/messages",
                json_payload=payload,
            )

        if route.scene == "direct":
            if route.guild_id:
                return await self._request(
                    "POST",
                    f"/dms/{_path_segment(route.guild_id)}/messages",
                    json_payload=payload,
                )
            if route.openid:
                # Fallback for direct sessions that only provide the user openid.
                return await self._request(
                    "POST",
                    f"/v2/users/{_path_segment(route.openid)}/messages",
                    json_payload=payload,
                )
            raise RuntimeError("Direct route missing guild_id/openid")

        if not route.channel_id:
            raise RuntimeError("Channel route missing channel_id")
        return await self._request(
            "POST",
            f"/channels/{_path_segment(route.channel_id)}/messages",
            json_payload=payload,
        )

    async def _build_send_payload(
        self,
        route: SessionRoute,
        elements: list[MessageElement],
    ) -> dict[str, Any] | None:
        text_parts: list[str] = []
        quote_message_id: str | None = None

        markdown_payload: dict[str, Any] | None = None
        keyboard_payload: dict[str, Any] | None = None
        media_elements: list[MessageElement] = []
        channel_image_src: str | None = None

        for el in elements:
            if el.type == "text":
                text_parts.append(str(el.attrs.get("content", "")))
                continue
            if el.type == "br":
                text_parts.append("\n")
                continue
            if el.type == "at":
                text_parts.append(self._render_at(el))
                continue
            if el.type == "quote":
                if quote_message_id is None:
                    quote_message_id = str(el.attrs.get("id", "")) or None
                continue
            if el.type in _MEDIA_FILE_TYPE:
                if route.scene in {"group", "c2c", "direct"}:
                    media_elements.append(el)
                elif route.scene == "channel" and el.type == "img":
                    src = str(el.attrs.get("src", ""))
                    if src:
                        channel_image_src = src
                continue
            if el.type == "qq:markdown":
                markdown_payload = {"content": str(el.attrs.get("content", ""))}
                continue
            if el.type == "qq:keyboard":
                raw = str(el.attrs.get("data", "{}"))
                try:
                    keyboard_payload = json.loads(raw)
                except Exception:
                    keyboard_payload = {"raw": raw}
                continue
            if el.type == "sb:ark":
                text_parts.append(str(el.attrs.get("data", "")))
                continue

            # Keep unknown elements visible to users as text to avoid silent drops.
            text_parts.append(elements_to_xml([el]))

        text_content = "".join(text_parts).strip()

        payload: dict[str, Any] = {}
        if text_content:
            payload["content"] = text_content

        if quote_message_id:
            payload["message_reference"] = {
                "message_id": quote_message_id,
                "ignore_get_message_error": True,
            }

        if keyboard_payload is not None:
            payload["keyboard"] = keyboard_payload

        if markdown_payload is not None:
            payload["markdown"] = markdown_payload
            payload["msg_type"] = 2
            payload.pop("content", None)

        if route.scene in {"group", "c2c", "direct"}:
            payload["msg_seq"] = random.randint(
                self.config.proactive_msg_seq_min,
                max(self.config.proactive_msg_seq_max, self.config.proactive_msg_seq_min),
            )

        if route.last_message_id:
            payload.setdefault("msg_id", route.last_message_id)
        if route.last_event_id:
            payload.setdefault("event_id", route.last_event_id)

        if media_elements:
            media = await self._upload_media(route, media_elements[0])
            if media is not None:
                payload["media"] = media
                payload["msg_type"] = 7
                payload.pop("markdown", None)
                if text_content:
                    payload["content"] = text_content

        if channel_image_src and route.scene == "channel":
            payload["image"] = channel_image_src

        compacted = _compact(payload)
        if not compacted:
            return None
        return compacted

    async def _upload_media(
        self, route: SessionRoute, element: MessageElement
    ) -> dict[str, Any] | None:
        src = str(element.attrs.get("src") or "")
        if not src:
            return None

        if not src.startswith("http://") and not src.startswith("https://"):
            logger.warning(
                "QQOfficial %s only supports URL-based media upload for group/c2c; got %s",
                self.instance_id,
                src,
            )
            return None

        file_type = _MEDIA_FILE_TYPE.get(element.type)
        if file_type is None:
            return None

        payload = {
            "file_type": file_type,
            "url": src,
            "srv_send_msg": False,
        }

        if route.scene == "group" and route.group_openid:
            result = await self._request(
                "POST",
                f"/v2/groups/{_path_segment(route.group_openid)}/files",
                json_payload=payload,
            )
            return result if isinstance(result, dict) else None

        target_openid = route.openid
        if route.scene == "direct" and not target_openid:
            target_openid = route.openid

        if target_openid:
            result = await self._request(
                "POST",
                f"/v2/users/{_path_segment(target_openid)}/files",
                json_payload=payload,
            )
            return result if isinstance(result, dict) else None

        return None

    async def _delete_message(self, params: dict[str, Any]) -> Any:
        message_id = str(params.get("message_id") or "")
        if not message_id:
            return {"ok": False, "reason": "missing message_id"}

        session_id = str(params.get("session_id") or "")
        route: SessionRoute | None = None

        if session_id:
            route = self._session_routes.get(session_id)

        if route is None:
            message_route = self._message_routes.get(message_id)
            if message_route is not None:
                route = SessionRoute(
                    scene=message_route.scene,
                    channel_id=message_route.channel_id,
                    guild_id=message_route.guild_id,
                    group_openid=message_route.group_openid,
                    openid=message_route.openid,
                )

        channel_id = str(params.get("channel_id") or "")
        if route is None and channel_id:
            route = SessionRoute(scene="channel", channel_id=channel_id)

        if route is not None and route.scene == "channel" and route.channel_id:
            return await self._request(
                "DELETE",
                f"/channels/{_path_segment(route.channel_id)}/messages/{_path_segment(message_id)}",
                query={"hidetip": "false"},
            )

        # QQ Official currently doesn't provide a unified delete endpoint for
        # group/c2c in this adapter's baseline. Return a soft failure.
        return {"ok": False, "reason": "unsupported_route_for_delete"}

    # ── HTTP / auth helpers ──────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        json_payload: Any | None = None,
    ) -> Any:
        if self._http is None:
            raise RuntimeError("Adapter not started - call start() first")

        await self._ensure_access_token()

        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self._api_base()}{normalized_path}"
        headers = {
            "Authorization": f"QQBot {self._access_token}",
            "X-Union-Appid": self.config.app_id,
        }

        response = await self._http.request(
            method=method.upper(),
            url=url,
            params=query,
            json=json_payload,
            headers=headers,
        )

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            logger.error(
                "QQOfficial API error %s %s -> %d %s",
                method.upper(),
                normalized_path,
                response.status_code,
                response.text,
            )
            raise

        if response.status_code == 204 or not response.content:
            return {}

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type.lower():
            return response.json()
        return response.text

    async def _ensure_access_token(self) -> None:
        now = asyncio.get_running_loop().time()
        if self._access_token and now < self._token_expire_at:
            return

        async with self._token_lock:
            now = asyncio.get_running_loop().time()
            if self._access_token and now < self._token_expire_at:
                return
            await self._refresh_access_token()

    async def _refresh_access_token(self) -> None:
        if self._http is None:
            raise RuntimeError("Adapter not started - call start() first")
        if not self.config.app_id or not self.config.app_secret:
            raise RuntimeError("QQOfficial app_id/app_secret must be configured")

        token_url = f"{self.config.token_base.rstrip('/')}/app/getAppAccessToken"
        response = await self._http.post(
            token_url,
            json={"appId": self.config.app_id, "clientSecret": self.config.app_secret},
        )
        response.raise_for_status()
        data = response.json()

        token = data.get("access_token")
        expires_in = _safe_int(data.get("expires_in"))
        if not token or expires_in is None:
            raise RuntimeError(f"Invalid access token response: {data!r}")

        # Refresh slightly earlier to avoid boundary expiry during requests.
        self._access_token = str(token)
        self._token_expire_at = asyncio.get_running_loop().time() + max(expires_in - 60, 30)

    def _api_base(self) -> str:
        if self.config.api_base.strip():
            return self.config.api_base.strip().rstrip("/")
        if self.config.sandbox:
            return "https://sandbox.api.sgroup.qq.com"
        return "https://api.sgroup.qq.com"

    # ── Session / route helpers ──────────────────────────────────────

    def _resolve_route(self, session_id: str) -> SessionRoute:
        existing = self._session_routes.get(session_id)
        if existing is not None:
            return existing

        rest = self._session_suffix(session_id)

        if rest.startswith("private:"):
            openid = rest[len("private:") :]
            return SessionRoute(scene="c2c", openid=openid)

        if rest.startswith("group:"):
            group_part = rest[len("group:") :]
            if ":" in group_part:
                guild_id, channel_id = group_part.rsplit(":", 1)
                if guild_id == channel_id:
                    return SessionRoute(
                        scene="group",
                        group_openid=channel_id,
                        guild_id=guild_id,
                        channel_id=channel_id,
                    )
                return SessionRoute(scene="channel", guild_id=guild_id, channel_id=channel_id)
            return SessionRoute(
                scene="group",
                group_openid=group_part,
                guild_id=group_part,
                channel_id=group_part,
            )

        return SessionRoute(scene="channel", channel_id=rest)

    def _session_suffix(self, session_id: str) -> str:
        first_colon = session_id.find(":")
        if first_colon == -1:
            return session_id
        return session_id[first_colon + 1 :]

    def _session_id_from_event(self, event: UnifiedEvent) -> str:
        if event.is_private:
            return f"{self.instance_id}:private:{event.sender_id or ''}"

        channel_id = event.channel_id or ""
        guild_id = event.guild_id
        if guild_id:
            return f"{self.instance_id}:group:{guild_id}:{channel_id}"
        return f"{self.instance_id}:group:{channel_id}"

    def _session_id_from_channel(self, channel_id: str) -> str:
        if channel_id.startswith("private:"):
            user_id = channel_id[len("private:") :]
            return f"{self.instance_id}:private:{user_id}"
        return f"{self.instance_id}:group:{channel_id}"

    def _render_at(self, element: MessageElement) -> str:
        if element.attrs.get("type") == "all":
            return "@everyone"
        target = str(element.attrs.get("id") or "")
        if not target:
            return ""
        return f"<@!{target}>"

    def _normalize_attachment_url(self, url: str) -> str:
        if not url:
            return ""
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"https://{url.lstrip('/')}"

    def _normalize_timestamp(self, value: Any) -> int | None:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            ivalue = int(value)
            return ivalue * 1000 if ivalue < 10_000_000_000 else ivalue

        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None

            as_int = _safe_int(text)
            if as_int is not None:
                return as_int * 1000 if as_int < 10_000_000_000 else as_int

            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(text)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return int(dt.timestamp() * 1000)
            except ValueError:
                return None

        return None

    def _event_extra_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        reserved = {
            "id",
            "sn",
            "type",
            "self_id",
            "platform",
            "timestamp",
            "login",
            "user",
            "operator",
            "member",
            "channel",
            "guild",
            "message",
        }
        return {k: v for k, v in payload.items() if k not in reserved}


def _compact(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if v is not None}


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _path_segment(value: str) -> str:
    return quote(value, safe="")
