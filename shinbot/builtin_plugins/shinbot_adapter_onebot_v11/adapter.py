"""OneBot v11 WebSocket adapter for ShinBot.

This adapter ingests OneBot v11 events and normalizes them to ShinBot
UnifiedEvent + Satori-compatible message XML payloads.

Architecture:
  - Forward mode: Adapter connects to the OB11 server as a WebSocket client.
  - Reverse mode: Adapter registers with OneBotGateway (plugin-level singleton).
    The Gateway manages one websockets server per (host, port) and routes
    incoming connections by X-Self-ID header, enabling port sharing across
    multiple instances with zero conflict.
"""

from __future__ import annotations

import asyncio
import http
import json
import time
import uuid
from pathlib import Path
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse

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


class OneBotV11Config(BaseModel):
    mode: Literal["forward", "reverse"] = Field(default="forward")
    url: str = Field(default="ws://127.0.0.1:3001")
    reverse_host: str = Field(default="0.0.0.0")
    reverse_port: int | None = Field(default=None)
    reverse_path: str = Field(default="/onebot/v11")
    self_id: str = Field(default="")
    access_token: str = Field(default="")
    reconnect_delay: float = Field(default=5.0, ge=0.0)
    max_reconnects: int = Field(default=-1)
    request_timeout: float = Field(default=20.0, gt=0.0)
    forward_max_depth: int = Field(default=3, ge=0)
    auto_download_media: bool = Field(default=False)
    download_resources: bool = Field(default=False)
    resource_cache_dir: str = Field(default="data/temp/resources")
    silent_reconnect: bool = Field(default=True)
    reconnect_log_interval: float = Field(default=30.0, ge=1.0)


# ── OneBotGateway ─────────────────────────────────────────────────────────────
# Plugin-level singleton: one websockets server per (host, port).
# Multiple reverse-mode adapter instances sharing the same port are
# multiplexed through a single server and routed by X-Self-ID header.


class _GatewayEntry:
    """Manages a single (host, port) binding shared by ≥1 adapter instances."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._adapters: list[OneBotV11Adapter] = []
        self._self_id_map: dict[str, OneBotV11Adapter] = {}
        self._path_map: dict[str, list[OneBotV11Adapter]] = {}
        self._paths: set[str] = set()
        self._server: Any = None  # websockets.Server

    # ── Adapter registry ──────────────────────────────────────────────

    def add(self, adapter: OneBotV11Adapter, path: str) -> None:
        self._adapters.append(adapter)
        norm = _norm_path(path)
        self._paths.add(norm)
        self._path_map.setdefault(norm, []).append(adapter)
        if adapter.config.self_id:
            self._self_id_map[adapter.config.self_id] = adapter

    def remove(self, adapter: OneBotV11Adapter) -> None:
        self._adapters = [a for a in self._adapters if a is not adapter]
        self._self_id_map = {k: v for k, v in self._self_id_map.items() if v is not adapter}
        for path, lst in list(self._path_map.items()):
            pruned = [a for a in lst if a is not adapter]
            if pruned:
                self._path_map[path] = pruned
            else:
                del self._path_map[path]
                self._paths.discard(path)

    def is_empty(self) -> bool:
        return not self._adapters

    def resolve(self, self_id: str, path: str) -> OneBotV11Adapter | None:
        """Return the adapter for this (self_id, path) pair.

        Routing priority:
          1. Exact X-Self-ID match within path group
          2. Single adapter on path → route unconditionally
          3. Single adapter on port → route unconditionally (fallback)
        """
        norm = _norm_path(path)
        candidates = self._path_map.get(norm, self._adapters)

        if self_id:
            if self_id in self._self_id_map:
                mapped = self._self_id_map[self_id]
                if mapped in candidates:
                    return mapped
        if len(candidates) == 1:
            return candidates[0]
        if candidates is not self._adapters and len(self._adapters) == 1:
            return self._adapters[0]
        return None

    # ── Server lifecycle ──────────────────────────────────────────────

    async def start(self) -> None:
        entry = self  # captured by closures below

        async def _check_request(connection: Any, request: Any) -> Any | None:
            """Reject at HTTP level if path doesn't match any registered adapter."""
            req_path = _norm_path(getattr(request, "path", "/") or "/")
            if entry._paths and req_path not in entry._paths:
                logger.debug(
                    "Gateway [%s:%d] rejected path=%s (registered: %s)",
                    entry.host,
                    entry.port,
                    req_path,
                    entry._paths,
                )
                return connection.respond(http.HTTPStatus.NOT_FOUND, "Not Found\n")
            return None

        async def _handler(websocket: Any) -> None:
            req = getattr(websocket, "request", None)
            headers = getattr(req, "headers", {}) if req else {}
            path = getattr(req, "path", "/") or "/"
            self_id = (headers.get("X-Self-ID") or "").strip()
            client_role = (headers.get("X-Client-Role") or "").strip()

            logger.debug(
                "Gateway [%s:%d] incoming connection: path=%s X-Self-ID=%r",
                entry.host,
                entry.port,
                path,
                self_id,
            )

            adapter = entry.resolve(self_id, path)
            if adapter is None:
                logger.warning(
                    "Gateway [%s:%d] no adapter matched X-Self-ID=%r path=%s — closing",
                    entry.host,
                    entry.port,
                    self_id,
                    path,
                )
                await websocket.close(1008, "No matching instance for X-Self-ID")
                return

            if not _check_token(adapter, headers, path):
                logger.warning(
                    "Gateway [%s:%d] token validation failed for instance %s",
                    entry.host,
                    entry.port,
                    adapter.instance_id,
                )
                await websocket.close(1008, "Unauthorized")
                return

            # Bind runtime self_id for future routing when not pre-configured
            if self_id and self_id not in entry._self_id_map:
                entry._self_id_map[self_id] = adapter

            await adapter.accept_connection(websocket, self_id=self_id, client_role=client_role)

        try:
            self._server = await websockets.serve(
                _handler,
                self.host,
                self.port,
                process_request=_check_request,
            )
        except OSError as exc:
            logger.error(
                "[ERROR] Gateway failed to bind %s:%d — %s",
                self.host,
                self.port,
                exc,
            )
            raise

        logger.info("OneBotGateway listening on %s:%d", self.host, self.port)

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=5.0)
            except TimeoutError:
                pass
            self._server = None
        logger.info("OneBotGateway stopped on %s:%d", self.host, self.port)


def _norm_path(path: str) -> str:
    """Normalize a URL path: strip trailing slash, ensure leading slash."""
    p = path.strip()
    if not p.startswith("/"):
        p = f"/{p}"
    return p.rstrip("/") or "/"


def _check_token(adapter: OneBotV11Adapter, headers: Any, path: str) -> bool:
    """Validate access_token from headers or query string."""
    expected = adapter.config.access_token.strip()
    if not expected:
        return True

    auth = (headers.get("Authorization") or "").strip()
    if auth.lower().startswith("bearer ") and auth[7:].strip() == expected:
        return True

    if (headers.get("X-Access-Token") or "").strip() == expected:
        return True

    qs = parse_qs(urlparse(path).query)
    tokens = qs.get("access_token", [])
    if tokens and tokens[0].strip() == expected:
        return True

    return False


class OneBotGateway:
    """Singleton: one WebSocket server per (host, port).

    All reverse-mode OneBotV11Adapter instances register here.
    Port conflicts are eliminated — multiple adapters sharing a port
    result in a single server with X-Self-ID-based routing.
    """

    def __init__(self) -> None:
        self._entries: dict[tuple[str, int], _GatewayEntry] = {}
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def register(self, adapter: OneBotV11Adapter, host: str, port: int, path: str) -> None:
        async with self._get_lock():
            key = (host, port)
            if key not in self._entries:
                entry = _GatewayEntry(host=host, port=port)
                entry.add(adapter, path)
                await entry.start()  # may raise OSError — propagates to caller
                self._entries[key] = entry
            else:
                self._entries[key].add(adapter, path)
                logger.info(
                    "OneBotGateway [%s:%d] added instance %s (total: %d)",
                    host,
                    port,
                    adapter.instance_id,
                    len(self._entries[key]._adapters),
                )

    async def unregister(self, adapter: OneBotV11Adapter, host: str, port: int) -> None:
        async with self._get_lock():
            key = (host, port)
            entry = self._entries.get(key)
            if entry is None:
                return
            entry.remove(adapter)
            if entry.is_empty():
                await entry.stop()
                del self._entries[key]

    async def shutdown_all(self) -> None:
        async with self._get_lock():
            for entry in list(self._entries.values()):
                await entry.stop()
            self._entries.clear()


# Module-level Gateway singleton
_GATEWAY = OneBotGateway()


# ── Adapter ───────────────────────────────────────────────────────────────────


class OneBotV11Adapter(BaseAdapter):
    def __init__(self, instance_id: str, platform: str, config: OneBotV11Config):
        super().__init__(instance_id=instance_id, platform=platform)
        self.config = config
        self._running = False
        self._ws: Any | None = None
        self._recv_task: asyncio.Task | None = None
        self._event_tasks: set[asyncio.Task[Any]] = set()
        self._self_id: str = config.self_id
        self._detected_platform: str = platform
        self._registered_host: str | None = None
        self._registered_port: int | None = None
        self._pending: dict[str, asyncio.Future[Any]] = {}
        self._resource_cache_dir = Path(self.config.resource_cache_dir)
        self._resource_cache_dir.mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        self._running = True
        if self.config.mode == "forward":
            self._recv_task = asyncio.create_task(
                self._connection_loop(), name=f"onebot-v11-{self.instance_id}"
            )
            logger.info(
                "OneBot v11 adapter %s started in forward mode (url=%s)",
                self.instance_id,
                self.config.url,
            )
        else:
            host, port, path = self._resolve_reverse_listener_target()
            self._registered_host = host
            self._registered_port = port
            await _GATEWAY.register(self, host, port, path)
            logger.info(
                "OneBot v11 adapter %s registered with gateway on %s:%d%s",
                self.instance_id,
                host,
                port,
                path,
            )

    async def shutdown(self) -> None:
        self._running = False

        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()

        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._registered_host is not None and self._registered_port is not None:
            await _GATEWAY.unregister(self, self._registered_host, self._registered_port)
            self._registered_host = None
            self._registered_port = None

        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        self._pending.clear()

        if self._event_tasks:
            tasks = list(self._event_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._event_tasks.clear()

    async def accept_connection(
        self, websocket: Any, *, self_id: str, client_role: str = ""
    ) -> None:
        """Handle an inbound reverse WebSocket connection routed by the Gateway."""
        if self.config.self_id and self.config.self_id != self_id:
            await websocket.close(1008, "Self ID mismatch")
            raise RuntimeError(
                f"self_id mismatch: expected {self.config.self_id!r}, got {self_id!r}"
            )

        self._self_id = self_id or self._self_id
        self._detected_platform = "qq"
        self._ws = websocket
        logger.info(
            "OneBot v11 %s accepted reverse connection (self_id=%s role=%s)",
            self.instance_id,
            self._self_id,
            client_role,
        )

        try:
            async for raw in websocket:
                await self._handle_raw(raw)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as exc:
            logger.warning("OneBot v11 %s connection error: %s", self.instance_id, exc)
        finally:
            self._ws = None
            logger.info("OneBot v11 %s reverse connection closed", self.instance_id)

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        message: list[dict[str, Any]] = []
        for el in elements:
            converted = self._element_to_ob11(el)
            if converted is not None:
                message.append(converted)
        channel = self._decode_session_id(target_session)

        if channel.startswith("private:"):
            user_id = channel[len("private:") :]
            result = await self._call_ob11_api(
                "send_private_msg",
                {"user_id": int(user_id) if user_id.isdigit() else user_id, "message": message},
            )
        else:
            group_id = channel
            result = await self._call_ob11_api(
                "send_group_msg",
                {
                    "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                    "message": message,
                },
            )

        msg_id = str(result.get("message_id", "")) if isinstance(result, dict) else ""
        return MessageHandle(
            message_id=msg_id, adapter_ref=self, platform_data={"session_id": target_session}
        )

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        if method == "channel.message.create":
            channel_id = str(params.get("channel_id", ""))
            content = str(params.get("content", ""))
            elements = Message.from_xml(content).elements if content else []
            session_id = (
                f"{self.instance_id}:{channel_id}"
                if channel_id.startswith("private:")
                else f"{self.instance_id}:group:{channel_id}"
            )
            handle = await self.send(session_id, elements)
            return [{"id": handle.message_id}]

        if method == "message.delete":
            return await self._call_ob11_api("delete_msg", {"message_id": params.get("message_id")})

        if method == "message.update":
            session_id = str(params.get("session_id", ""))
            message_id = params.get("message_id")
            elements = params.get("elements", [])
            if message_id:
                await self._call_ob11_api("delete_msg", {"message_id": message_id})
            if session_id and isinstance(elements, list):
                return await self.send(session_id, elements)
            return {"ok": True}

        if method == "member.kick":
            group_id = params.get("guild_id") or params.get("group_id")
            return await self._call_ob11_api(
                "set_group_kick",
                {
                    "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                    "user_id": (
                        int(params["user_id"])
                        if str(params["user_id"]).isdigit()
                        else params["user_id"]
                    ),
                },
            )

        if method == "member.mute":
            group_id = params.get("guild_id") or params.get("group_id")
            return await self._call_ob11_api(
                "set_group_ban",
                {
                    "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                    "user_id": (
                        int(params["user_id"])
                        if str(params["user_id"]).isdigit()
                        else params["user_id"]
                    ),
                    "duration": int(params.get("duration", 0)),
                },
            )

        if method == "guild.member.list":
            group_id = params.get("guild_id") or params.get("group_id")
            return await self._call_ob11_api(
                "get_group_member_list",
                {"group_id": int(group_id) if str(group_id).isdigit() else group_id},
            )

        if method in {"guild.update", "channel.update"}:
            group_id = params.get("guild_id") or params.get("group_id") or params.get("channel_id")
            group_name = params.get("name") or params.get("group_name")
            return await self._call_ob11_api(
                "set_group_name",
                {
                    "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                    "group_name": str(group_name or ""),
                },
            )

        if method == "friend.approve":
            return await self._call_ob11_api(
                "set_friend_add_request",
                {"flag": params.get("message_id"), "approve": True},
            )

        if method.startswith("internal."):
            if method.endswith(".poke"):
                group_id = params.get("group_id") or params.get("guild_id")
                user_id = params.get("user_id")
                if group_id is not None:
                    return await self._call_ob11_api(
                        "group_poke",
                        {
                            "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                            "user_id": int(user_id) if str(user_id).isdigit() else user_id,
                        },
                    )
                return await self._call_ob11_api(
                    "friend_poke",
                    {"user_id": int(user_id) if str(user_id).isdigit() else user_id},
                )

            internal_action = method.split(".", 2)[-1]
            if internal_action == "set_group_name":
                group_id = (
                    params.get("group_id") or params.get("guild_id") or params.get("channel_id")
                )
                group_name = params.get("group_name") or params.get("name")
                return await self._call_ob11_api(
                    "set_group_name",
                    {
                        "group_id": int(group_id) if str(group_id).isdigit() else group_id,
                        "group_name": str(group_name or ""),
                    },
                )
            return await self._call_ob11_api(internal_action, params)

        return await self._call_ob11_api(method.replace(".", "_"), params)

    async def get_capabilities(self) -> dict[str, Any]:
        return {
            "modes": ["forward", "reverse"],
            "mode": self.config.mode,
            "elements": [
                "text",
                "at",
                "img",
                "emoji",
                "quote",
                "audio",
                "video",
                "file",
                "message",
                "sb:poke",
                "qq:markdown",
                "qq:keyboard",
                "qq:mface",
            ],
            "actions": [
                "channel.message.create",
                "message.delete",
                "member.kick",
                "member.mute",
                "friend.approve",
                "internal.qq.poke",
            ],
            "limits": {},
            "platform": "qq",
        }

    # ── Forward mode connection loop ──────────────────────────────────

    async def _connection_loop(self) -> None:
        attempts = 0
        last_log_ts = 0.0
        while self._running:
            try:
                await self._connect_and_receive()
                if attempts > 0:
                    logger.info("OneBot v11 %s reconnected successfully", self.instance_id)
                attempts = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                attempts += 1
                now = time.monotonic()
                should_log = (
                    not self.config.silent_reconnect
                    or attempts == 1
                    or now - last_log_ts >= self.config.reconnect_log_interval
                )
                if should_log:
                    logger.warning(
                        "OneBot v11 %s reconnecting in %.1fs (attempt %d): %s",
                        self.instance_id,
                        self.config.reconnect_delay,
                        attempts,
                        e,
                    )
                    last_log_ts = now
                if self.config.max_reconnects >= 0 and attempts > self.config.max_reconnects:
                    logger.error("OneBot v11 %s reached max reconnect attempts", self.instance_id)
                    break
                await asyncio.sleep(self.config.reconnect_delay)

    async def _connect_and_receive(self) -> None:
        headers: dict[str, str] = {}
        if self.config.access_token:
            headers["Authorization"] = f"Bearer {self.config.access_token}"

        async with websockets.connect(self.config.url, additional_headers=headers) as ws:
            self._ws = ws
            logger.info("OneBot v11 %s connected to %s", self.instance_id, self.config.url)
            async for raw in ws:
                await self._handle_raw(raw)

    # ── Message processing ────────────────────────────────────────────

    async def _handle_raw(self, raw: str | bytes) -> None:
        try:
            logger.debug("OneBot v11 %s received raw payload: %s", self.instance_id, raw)
            payload = json.loads(raw)
        except Exception:
            logger.warning("OneBot v11 %s received non-JSON payload", self.instance_id)
            return

        try:
            echo = payload.get("echo")
            if echo is not None:
                fut = self._pending.pop(str(echo), None)
                if fut is not None and not fut.done():
                    status = payload.get("status")
                    if status == "ok":
                        fut.set_result(payload.get("data", {}))
                    else:
                        fut.set_exception(
                            RuntimeError(
                                f"OneBot action failed: retcode={payload.get('retcode')} "
                                f"msg={payload.get('msg')}"
                            )
                        )
                return

            if self._event_callback is None:
                return

            task = asyncio.create_task(
                self._dispatch_event_payload(payload),
                name=f"onebot-v11-dispatch-{self.instance_id}",
            )
            self._event_tasks.add(task)
            task.add_done_callback(self._event_tasks.discard)
        except Exception as e:
            logger.error("OneBot v11 %s failed to process event: %s", self.instance_id, e)

    async def _dispatch_event_payload(self, payload: dict[str, Any]) -> None:
        """Decode and emit an event without blocking the receive loop."""
        try:
            event = await self._decode_event(payload)
            if event is None or self._event_callback is None:
                return
            await self._event_callback(event)
        except Exception as e:
            logger.error("OneBot v11 %s failed to process event: %s", self.instance_id, e)

    async def _call_ob11_api(self, action: str, params: dict[str, Any]) -> Any:
        if self._ws is None:
            raise RuntimeError("Adapter is not connected")

        echo = str(uuid.uuid4())
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[echo] = fut

        await self._ws.send(json.dumps({"action": action, "params": params, "echo": echo}))
        try:
            return await asyncio.wait_for(fut, timeout=self.config.request_timeout)
        finally:
            self._pending.pop(echo, None)

    # ── Event decoding ────────────────────────────────────────────────

    async def _decode_event(self, payload: dict[str, Any]) -> UnifiedEvent | None:
        post_type = str(payload.get("post_type", ""))
        if post_type == "message":
            return await self._decode_message_event(payload)

        if post_type == "notice":
            if payload.get("notice_type") == "notify" and payload.get("sub_type") == "poke":
                return self._decode_poke_notice(payload)
            return self._decode_notice_event(payload)

        if post_type == "request":
            return self._decode_request_event(payload)

        return None

    async def _decode_message_event(self, payload: dict[str, Any]) -> UnifiedEvent:
        user_id = str(payload.get("user_id", ""))
        message_id = str(payload.get("message_id", ""))
        message_type = str(payload.get("message_type", ""))
        sender = payload.get("sender", {}) if isinstance(payload.get("sender"), dict) else {}

        elements = await self._normalize_message(
            payload.get("message", ""),
            forward_depth=self.config.forward_max_depth,
        )
        if self.config.download_resources or self.config.auto_download_media:
            elements = await download_resource_elements(elements, self._resource_cache_dir)
        content_xml = elements_to_xml(elements)

        user = User(
            id=user_id,
            name=sender.get("nickname"),
            nick=sender.get("card") or sender.get("nickname"),
            is_bot=False,
        )

        channel: Channel
        guild: Guild | None = None
        member: Member | None = None
        if message_type == "group":
            group_id = str(payload.get("group_id", ""))
            channel = Channel(id=group_id, name=str(payload.get("group_name", "")) or None, type=0)
            guild = Guild(id=group_id)
            member = Member(nick=sender.get("card") or sender.get("nickname"))
        else:
            channel = Channel(id=f"private:{user_id}", type=1)

        return UnifiedEvent(
            id=self._safe_int(payload.get("message_id")),
            type="message-created",
            self_id=str(payload.get("self_id", "")),
            platform="qq",
            timestamp=self._normalize_timestamp(payload.get("time")),
            user=user,
            member=member,
            channel=channel,
            guild=guild,
            message=MessagePayload(id=message_id, content=content_xml),
        )

    def _decode_poke_notice(self, payload: dict[str, Any]) -> UnifiedEvent:
        user_id = str(payload.get("user_id", ""))
        target_id = str(payload.get("target_id", ""))
        group_id = payload.get("group_id")

        attrs: dict[str, Any] = {}
        if target_id:
            attrs["target"] = target_id
        attrs["type"] = "poke"
        poke_xml = elements_to_xml([MessageElement(type="sb:poke", attrs=attrs)])

        channel: Channel
        guild: Guild | None = None
        if group_id is not None:
            group = str(group_id)
            channel = Channel(id=group, type=0)
            guild = Guild(id=group)
        else:
            channel = Channel(id=f"private:{target_id or user_id}", type=1)

        return UnifiedEvent(
            id=self._safe_int(payload.get("time")),
            type="message-created",
            self_id=str(payload.get("self_id", "")),
            platform="qq",
            timestamp=self._normalize_timestamp(payload.get("time")),
            user=User(id=user_id),
            channel=channel,
            guild=guild,
            message=MessagePayload(
                id=f"poke-{int(time.time() * 1000)}-{user_id}",
                content=poke_xml,
            ),
            operator=User(id=user_id),
        )

    def _decode_notice_event(self, payload: dict[str, Any]) -> UnifiedEvent:
        notice_type = str(payload.get("notice_type", "unknown"))
        sub_type = str(payload.get("sub_type", "")).strip()
        event_type = f"notice-{notice_type}" + (f"-{sub_type}" if sub_type else "")

        if notice_type == "group_increase":
            event_type = "guild-member-added"
        elif notice_type == "group_decrease":
            event_type = "guild-member-deleted"
        elif notice_type == "friend_add":
            event_type = "friend-added"

        group_id = payload.get("group_id")
        guild = Guild(id=str(group_id)) if group_id is not None else None
        user_id = payload.get("user_id")
        operator_id = payload.get("operator_id")
        extra_payload = self._event_extra_payload(payload)

        return UnifiedEvent(
            id=self._safe_int(payload.get("time")),
            type=event_type,
            self_id=str(payload.get("self_id", "")),
            platform="qq",
            timestamp=self._normalize_timestamp(payload.get("time")),
            user=User(id=str(user_id)) if user_id is not None else None,
            operator=User(id=str(operator_id)) if operator_id is not None else None,
            guild=guild,
            channel=Channel(id=str(group_id), type=0) if group_id is not None else None,
            **extra_payload,
        )

    def _decode_request_event(self, payload: dict[str, Any]) -> UnifiedEvent:
        request_type = str(payload.get("request_type", ""))
        if request_type == "friend":
            event_type = "friend-request"
        elif request_type == "group":
            event_type = "guild-request"
        else:
            event_type = f"request-{request_type or 'unknown'}"

        group_id = payload.get("group_id")
        extra_payload = self._event_extra_payload(payload)
        return UnifiedEvent(
            id=self._safe_int(payload.get("time")),
            type=event_type,
            self_id=str(payload.get("self_id", "")),
            platform="qq",
            timestamp=self._normalize_timestamp(payload.get("time")),
            user=User(id=str(payload.get("user_id", ""))),
            guild=Guild(id=str(group_id)) if group_id is not None else None,
            channel=Channel(id=str(group_id), type=0) if group_id is not None else None,
            **extra_payload,
        )

    async def _normalize_message(
        self, message: Any, *, forward_depth: int | None = None
    ) -> list[MessageElement]:
        if forward_depth is None:
            forward_depth = self.config.forward_max_depth

        if isinstance(message, str):
            return [MessageElement.text(message)] if message else []

        if not isinstance(message, list):
            return []

        elements: list[MessageElement] = []
        for seg in message:
            if not isinstance(seg, dict):
                continue
            seg_type = str(seg.get("type", ""))
            data = seg.get("data", {})
            if not isinstance(data, dict):
                data = {}

            if seg_type == "text":
                text = str(data.get("text", ""))
                if text:
                    elements.append(MessageElement.text(text))
            elif seg_type == "at":
                qq = str(data.get("qq", ""))
                if qq == "all":
                    elements.append(MessageElement.at(type="all"))
                else:
                    elements.append(MessageElement.at(id=qq, name=data.get("name")))
            elif seg_type == "image":
                src = str(data.get("url") or data.get("file") or "")
                if src:
                    elements.append(
                        MessageElement.img(
                            src, sub_type=data.get("subType") or data.get("sub_type")
                        )
                    )
            elif seg_type == "record":
                src = str(data.get("url") or data.get("file") or "")
                if src:
                    elements.append(MessageElement.audio(src))
            elif seg_type == "video":
                src = str(data.get("url") or data.get("file") or "")
                if src:
                    elements.append(MessageElement.video(src))
            elif seg_type == "file":
                src = str(data.get("url") or data.get("file") or "")
                if src:
                    elements.append(
                        MessageElement.file(src, name=data.get("name") or data.get("file"))
                    )
            elif seg_type == "reply":
                reply_id = str(data.get("id") or data.get("message_id") or "")
                if reply_id:
                    elements.append(MessageElement.quote(reply_id))
            elif seg_type == "face":
                emoji_id = str(data.get("id", ""))
                elements.append(MessageElement.emoji(id=emoji_id if emoji_id else None))
            elif seg_type == "markdown":
                content = str(data.get("content", ""))
                elements.append(MessageElement(type="qq:markdown", attrs={"content": content}))
            elif seg_type == "keyboard":
                elements.append(
                    MessageElement(
                        type="qq:keyboard",
                        attrs={"data": json.dumps(data, ensure_ascii=False)},
                    )
                )
            elif seg_type == "mface":
                attrs = {k: v for k, v in data.items() if v is not None}
                elements.append(MessageElement(type="qq:mface", attrs=attrs))
            elif seg_type == "poke":
                attrs_poke: dict[str, Any] = {}
                if data.get("id") is not None:
                    attrs_poke["target"] = data.get("id")
                if data.get("type") is not None:
                    attrs_poke["type"] = data.get("type")
                elements.append(MessageElement(type="sb:poke", attrs=attrs_poke))
            elif seg_type == "forward":
                forward_id = str(data.get("id") or data.get("resid") or "")
                if forward_id:
                    attrs: dict[str, Any] = {"forward": "true"}
                    if forward_depth <= 0:
                        if forward_id:
                            attrs["id"] = forward_id
                        elements.append(MessageElement(type="message", attrs=attrs))
                        continue

                    nodes = await self._fetch_forward_nodes(
                        forward_id,
                        forward_depth=forward_depth - 1,
                    )
                    elements.append(MessageElement(type="message", attrs=attrs, children=nodes))
            elif seg_type in ("json", "xml", "miniapp"):
                raw_data = data.get("data", data)
                elements.append(
                    MessageElement(
                        type="sb:ark",
                        attrs={"data": json.dumps(raw_data, ensure_ascii=False)},
                    )
                )
            else:
                elements.append(
                    MessageElement.text(f"[{seg_type}]{json.dumps(data, ensure_ascii=False)}")
                )

        return elements

    async def _fetch_forward_nodes(
        self, forward_id: str, *, forward_depth: int | None = None
    ) -> list[MessageElement]:
        if forward_depth is None:
            forward_depth = self.config.forward_max_depth
        try:
            data = await self._call_ob11_api("get_forward_msg", {"id": forward_id})
        except Exception as exc:
            logger.warning("Failed to fetch forward message content for id=%s: %s", forward_id, exc)
            return []

        messages: list[dict[str, Any]] = []
        if isinstance(data, dict):
            raw_messages = data.get("messages")
            if isinstance(raw_messages, list):
                messages = [m for m in raw_messages if isinstance(m, dict)]
        elif isinstance(data, list):
            messages = [m for m in data if isinstance(m, dict)]

        nodes: list[MessageElement] = []
        for item in messages:
            node_data = self._extract_forward_node_payload(item)
            content = self._extract_forward_node_content(node_data, item)
            sender_uin, nickname = self._extract_forward_node_sender(node_data, item)
            children = await self._normalize_message(content, forward_depth=forward_depth)
            attrs: dict[str, Any] = {}
            if sender_uin is not None:
                attrs["id"] = str(sender_uin)
            if nickname is not None:
                attrs["name"] = str(nickname)
            nodes.append(MessageElement(type="message", attrs=attrs, children=children))

        return nodes

    def _extract_forward_node_payload(self, item: dict[str, Any]) -> dict[str, Any]:
        data = item.get("data")
        if isinstance(data, dict):
            return data
        return item

    def _extract_forward_node_content(self, node_data: dict[str, Any], item: dict[str, Any]) -> Any:
        for candidate in (
            node_data.get("content"),
            node_data.get("message"),
            item.get("message"),
            item.get("content"),
            item.get("raw_message"),
        ):
            if candidate is not None:
                return candidate
        return []

    def _extract_forward_node_sender(
        self, node_data: dict[str, Any], item: dict[str, Any]
    ) -> tuple[str | None, str | None]:
        sender = node_data.get("sender")
        if not isinstance(sender, dict):
            sender = item.get("sender") if isinstance(item.get("sender"), dict) else {}

        sender_uin = (
            node_data.get("uin")
            or node_data.get("user_id")
            or item.get("sender_id")
            or item.get("user_id")
            or sender.get("user_id")
        )
        nickname = (
            node_data.get("name")
            or node_data.get("nickname")
            or item.get("nickname")
            or sender.get("card")
            or sender.get("nickname")
        )
        return (
            str(sender_uin) if sender_uin is not None and str(sender_uin) else None,
            str(nickname) if nickname is not None and str(nickname) else None,
        )

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
        return {key: value for key, value in payload.items() if key not in reserved}

    # ── Helpers ───────────────────────────────────────────────────────

    def _is_absolute_ws_url(self, value: str) -> bool:
        parsed = urlparse(value.strip())
        return parsed.scheme in {"ws", "wss"} and bool(parsed.netloc)

    def _resolve_reverse_listener_target(self) -> tuple[str, int, str]:
        raw = self.config.reverse_path.strip()
        if self._is_absolute_ws_url(raw):
            parsed = urlparse(raw)
            if parsed.hostname is None or parsed.port is None:
                raise ValueError("reverse_path WebSocket URL must include host and port")
            path = _norm_path(parsed.path or "/")
            return parsed.hostname, parsed.port, path

        if self.config.reverse_port is None:
            raise ValueError("reverse_port is required when reverse_path is not a WebSocket URL")

        path = _norm_path(raw or "/onebot/v11")
        return self.config.reverse_host, self.config.reverse_port, path

    def _decode_session_id(self, session_id: str) -> str:
        colon_pos = session_id.find(":")
        if colon_pos == -1:
            return session_id
        rest = session_id[colon_pos + 1 :]

        if rest.startswith("private:"):
            return rest

        if rest.startswith("group:"):
            group_part = rest[len("group:") :]
            if ":" in group_part:
                return group_part.rsplit(":", 1)[1]
            return group_part

        return rest

    def _element_to_ob11(self, el: MessageElement) -> dict[str, Any] | None:
        if el.type == "text":
            return {"type": "text", "data": {"text": str(el.attrs.get("content", ""))}}
        if el.type == "at":
            if el.attrs.get("type") == "all":
                return {"type": "at", "data": {"qq": "all"}}
            return {"type": "at", "data": {"qq": str(el.attrs.get("id", ""))}}
        if el.type == "img":
            return {"type": "image", "data": {"file": str(el.attrs.get("src", ""))}}
        if el.type == "audio":
            return {"type": "record", "data": {"file": str(el.attrs.get("src", ""))}}
        if el.type == "video":
            return {"type": "video", "data": {"file": str(el.attrs.get("src", ""))}}
        if el.type == "file":
            return {"type": "file", "data": {"file": str(el.attrs.get("src", ""))}}
        if el.type == "quote":
            return {"type": "reply", "data": {"id": str(el.attrs.get("id", ""))}}
        if el.type == "emoji":
            return {"type": "face", "data": {"id": str(el.attrs.get("id", ""))}}
        if el.type == "sb:poke":
            return {
                "type": "poke",
                "data": {
                    "id": str(el.attrs.get("target") or el.attrs.get("id") or ""),
                    "type": str(el.attrs.get("type", "poke")),
                },
            }
        if el.type == "sb:ark":
            return {"type": "json", "data": {"data": str(el.attrs.get("data", "{}"))}}
        if el.type == "qq:markdown":
            return {"type": "markdown", "data": {"content": str(el.attrs.get("content", ""))}}
        if el.type == "qq:keyboard":
            raw = str(el.attrs.get("data", "{}"))
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"raw": raw}
            return {"type": "keyboard", "data": parsed}
        if el.type == "qq:mface":
            return {"type": "mface", "data": dict(el.attrs)}
        if el.type == "message":
            return None  # Forward node containers are ingress-only
        return {"type": "text", "data": {"text": elements_to_xml([el])}}

    def _normalize_timestamp(self, value: Any) -> int | None:
        if value is None:
            return None
        ts = self._safe_int(value)
        if ts is None:
            return None
        return ts * 1000 if ts < 10_000_000_000 else ts

    def _safe_int(self, value: Any) -> int | None:
        try:
            return int(value)
        except Exception:
            return None
