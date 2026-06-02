"""Satori protocol WebSocket adapter for ShinBot.

Connects to any Satori-compatible server (LLOneBot, official Satori, etc.)
via WebSocket and exposes the standard BaseAdapter interface.

Reference: docs/references/satori-docs/zh-CN/protocol/{events,api}.md

Wire format (signal opcodes):
  - op=0: EVENT     — incoming platform events  → UnifiedEvent
  - op=1: PING      — client → server heartbeat
  - op=2: PONG      — server → client heartbeat reply
  - op=3: IDENTIFY  — client → server authentication / session recovery
  - op=4: READY     — server → client login info on connect
  - op=5: META      — server → client metadata update (experimental)

HTTP API (send):
  POST /v1/{resource}.{method}            (e.g. /v1/message.create)
  Headers: Authorization: Bearer <token>
           Satori-Platform: <platform>
           Satori-User-ID:  <bot_id>
  Body:    {"channel_id": "...", "content": "<xml>..."}

For backward compatibility with older SDKs (e.g. early LLOneBot builds) the
legacy ``X-Platform`` / ``X-Self-ID`` headers are also emitted alongside the
standard ``Satori-*`` headers.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import websockets

from shinbot.core.platform.adapter_manager import BaseAdapter, MessageHandle
from shinbot.schema.elements import Message, MessageElement
from shinbot.schema.events import UnifiedEvent
from shinbot.utils.logger import format_log_event, get_logger
from shinbot.utils.resource_ingress import DEFAULT_MAX_RESOURCE_BYTES, download_resource_elements
from shinbot.utils.satori_parser import elements_to_xml

logger = get_logger(__name__, source="adapter:satori", color="green")

# Satori opcodes (per docs/references/satori-docs/zh-CN/protocol/events.md).
OP_EVENT = 0
OP_PING = 1
OP_PONG = 2
OP_IDENTIFY = 3
OP_READY = 4
OP_META = 5

# Heartbeat interval in seconds (the protocol specifies 10s).
HEARTBEAT_INTERVAL = 10
EVENT_QUEUE_MAXSIZE = 1024


# ── ShinBot ↔ Satori method-name translation ──────────────────────────────────
# ShinBot uses "channel.message.*" as its internal universal naming for
# message lifecycle calls (so the same method-name table works across
# OneBot v11, QQ Official, and Satori). Satori itself uses bare "message.*".
# This map is applied in both directions on the wire boundary.
_SHINBOT_TO_SATORI_METHODS: dict[str, str] = {
    "channel.message.create": "message.create",
    "channel.message.delete": "message.delete",
    "channel.message.update": "message.update",
    "channel.message.get": "message.get",
    "channel.message.list": "message.list",
}


@dataclass
class SatoriConfig:
    """Connection configuration for the Satori adapter."""

    host: str  # e.g. "localhost:5140"
    token: str = ""  # Authorization token (required if server enforces auth)
    path: str = "/v1/events"  # WebSocket endpoint path
    reconnect_delay: float = 5.0  # Seconds between reconnection attempts
    max_reconnects: int = -1  # -1 = infinite retries
    auto_download_media: bool = True
    download_file_resources: bool = False
    max_resource_bytes: int = DEFAULT_MAX_RESOURCE_BYTES
    resource_cache_dir: str = "data/temp/resources"
    silent_reconnect: bool = True
    reconnect_log_interval: float = 30.0


class SatoriAdapter(BaseAdapter):
    """Connects to a Satori-compatible WebSocket server.

    Handles the full lifecycle:
      1. WebSocket connect → send IDENTIFY → wait for READY
      2. Heartbeat (client → PING op=1, server → PONG op=2)
      3. Event dispatch to the registered callback (with `sn` tracking
         for session recovery)
      4. Graceful disconnect on shutdown()
      5. Automatic reconnection with configurable back-off
    """

    def __init__(self, instance_id: str, platform: str, config: SatoriConfig):
        super().__init__(instance_id=instance_id, platform=platform)
        self.config = config
        self._self_id: str = ""  # Populated from READY event
        self._detected_platform: str = platform  # Overwritten by READY data
        self._ws: Any | None = None
        self._running = False
        self._recv_task: asyncio.Task | None = None
        self._ping_task: asyncio.Task | None = None
        self._event_worker_task: asyncio.Task[None] | None = None
        self._event_queue: asyncio.Queue[UnifiedEvent] | None = None
        self._http: httpx.AsyncClient | None = None
        self._resource_cache_dir = Path(self.config.resource_cache_dir)
        self._resource_cache_dir.mkdir(parents=True, exist_ok=True)
        # Last seen `sn` from EVENT signals; sent back in IDENTIFY for session
        # recovery on reconnect.
        self._last_event_sn: int | None = None

    # ── BaseAdapter interface ────────────────────────────────────────

    async def start(self) -> None:
        """Start the WebSocket listener in the background."""
        self._running = True
        self._http = httpx.AsyncClient(timeout=30.0)
        self._event_queue = asyncio.Queue(maxsize=EVENT_QUEUE_MAXSIZE)
        self._event_worker_task = asyncio.create_task(
            self._event_worker_loop(),
            name=f"satori-events-{self.instance_id}",
        )
        self._recv_task = asyncio.create_task(
            self._connection_loop(), name=f"satori-{self.instance_id}"
        )
        logger.info(
            format_log_event(
                "adapter.connection.starting",
                adapter="satori",
                instance_id=self.instance_id,
                endpoint=self.config.host,
            )
        )

    async def shutdown(self) -> None:
        """Gracefully close the WebSocket and cancel background tasks."""
        self._running = False
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
        if self._event_worker_task and not self._event_worker_task.done():
            self._event_worker_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._event_worker_task is not None:
            try:
                await self._event_worker_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(
                    "Satori %s: event worker terminated unexpectedly", self.instance_id
                )
            self._event_worker_task = None
        self._event_queue = None
        if self._http:
            await self._http.aclose()
        logger.info("Satori adapter %s shut down", self.instance_id)

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        """Serialize elements to Satori XML and POST to message.create."""
        channel_id = self._decode_session_id(target_session)
        xml_content = elements_to_xml(elements)

        response_data = await self.call_api(
            "channel.message.create",
            {"channel_id": channel_id, "content": xml_content},
        )

        # Satori returns a list of created messages
        messages = response_data if isinstance(response_data, list) else []
        msg_id = messages[0]["id"] if messages else ""

        return MessageHandle(
            message_id=msg_id,
            adapter_ref=self,
            platform_data={"session_id": target_session},
        )

    async def call_api(self, method: str, params: dict[str, Any]) -> Any:
        """Call a Satori standard API method via HTTP POST.

        ShinBot's universal method names (``channel.message.*``) are
        translated to Satori's bare names (``message.*``) at the wire
        boundary. Internal methods use ``internal.{platform}.{action}``
        and are routed under ``/v1/internal/{action}`` per the spec.
        """
        if self._http is None:
            raise RuntimeError("Adapter not started — call start() first")

        params = dict(params)
        params.pop("session_id", None)
        if method == "message.update" and isinstance(params.get("elements"), list):
            params["content"] = elements_to_xml(params.pop("elements"))
        if method == "channel.message.update" and isinstance(params.get("elements"), list):
            params["content"] = elements_to_xml(params.pop("elements"))

        # Translate ShinBot universal method names to Satori native ones.
        wire_method = _SHINBOT_TO_SATORI_METHODS.get(method, method)

        # Build the URL path. Satori HTTP API keeps the dot in
        # `{resource}.{method}` form and uses `/v1/internal/{action}` for
        # platform-internal calls.
        base = f"http://{self.config.host}"
        if wire_method.startswith("internal."):
            # `internal.{platform}.{action}` → `/v1/internal/{action}`
            # The `{platform}` segment is dropped because the call is already
            # scoped by the Satori-Platform header.
            internal_action = wire_method.split(".", 2)[-1]
            path = f"/v1/internal/{internal_action.replace('.', '/')}"
        else:
            path = f"/v1/{wire_method}"

        headers = self._build_headers()
        try:
            resp = await self._http.post(
                f"{base}{path}",
                json=params,
                headers=headers,
            )
            resp.raise_for_status()
            if resp.content:
                return resp.json()
            return {}
        except httpx.HTTPStatusError as e:
            logger.error(
                "Satori API error: %s %s → %d %s",
                method,
                params,
                e.response.status_code,
                e.response.text,
            )
            raise
        except Exception as e:
            logger.error("Satori API call failed: %s: %s", method, e)
            raise

    async def get_capabilities(self) -> dict[str, Any]:
        """Return capabilities based on the features announced in READY."""
        return {
            "elements": [
                "text",
                "at",
                "sharp",
                "img",
                "emoji",
                "quote",
                "audio",
                "video",
                "file",
                "br",
                "message",
            ],
            "actions": [
                "channel.message.create",
                "message.get",
                "message.delete",
                "message.update",
                "guild.get",
                "guild.list",
                "guild.member.get",
                "guild.member.list",
                "guild.member.kick",
                "guild.member.mute",
                "reaction.create",
                "reaction.delete",
            ],
            "limits": {},
            "platform": self._detected_platform,
        }

    # ── Connection lifecycle ─────────────────────────────────────────

    async def _connection_loop(self) -> None:
        """Maintain a persistent WebSocket connection with auto-reconnect."""
        attempt = 0
        last_log_ts = 0.0
        while self._running:
            try:
                await self._connect_and_receive()
                if attempt > 0:
                    logger.info(
                        format_log_event(
                            "adapter.connection.reconnected",
                            adapter="satori",
                            instance_id=self.instance_id,
                            attempts=attempt,
                        )
                    )
                attempt = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                attempt += 1
                now = asyncio.get_running_loop().time()
                should_log = (
                    not self.config.silent_reconnect
                    or attempt == 1
                    or now - last_log_ts >= self.config.reconnect_log_interval
                )
                if should_log:
                    logger.warning(
                        format_log_event(
                            "adapter.connection.retry",
                            adapter="satori",
                            instance_id=self.instance_id,
                            retry_after_seconds=f"{self.config.reconnect_delay:.1f}",
                            attempt=attempt,
                            error_code=type(e).__name__,
                        )
                    )
                    last_log_ts = now
                if self.config.max_reconnects >= 0 and attempt > self.config.max_reconnects:
                    logger.error(
                        format_log_event(
                            "adapter.connection.failed",
                            adapter="satori",
                            instance_id=self.instance_id,
                            reason="max_reconnects_reached",
                            attempts=attempt,
                        )
                    )
                    break
                await asyncio.sleep(self.config.reconnect_delay)

    async def _connect_and_receive(self) -> None:
        """Open a single WebSocket session and process it until disconnect.

        Per the Satori protocol the client must send an IDENTIFY signal
        within 10s of opening the WebSocket. The SDK then replies with
        READY and starts pushing EVENT signals.
        """
        ws_url = f"ws://{self.config.host}{self.config.path}"
        # The Authorization header on the WebSocket handshake itself is
        # optional in the spec — the canonical authentication path is the
        # `token` field of the IDENTIFY signal. We still pass it to remain
        # compatible with older SDK builds that gate the upgrade on it.
        headers = {}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            self._ws = ws
            logger.info(
                format_log_event(
                    "adapter.connection.connected",
                    adapter="satori",
                    instance_id=self.instance_id,
                    endpoint=ws_url,
                )
            )

            # IDENTIFY: send authentication + optional sequence number for
            # session recovery on reconnect.
            await self._send_identify(ws)

            # Start client-side heartbeat (PING op=1, every HEARTBEAT_INTERVAL).
            self._ping_task = asyncio.create_task(self._heartbeat(ws))

            try:
                async for raw in ws:
                    await self._handle_raw(raw)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                self._notify_connection_state(False)
                self._ws = None

    async def _send_identify(self, ws: Any) -> None:
        """Send the IDENTIFY signal (op=3) right after connection.

        ``token`` is omitted if the SDK is not configured for auth, per spec.
        ``sn`` is included when we have a previously seen sequence number,
        triggering server-side session recovery.
        """
        body: dict[str, Any] = {}
        if self.config.token:
            body["token"] = self.config.token
        if self._last_event_sn is not None:
            body["sn"] = self._last_event_sn
        payload: dict[str, Any] = {"op": OP_IDENTIFY}
        if body:
            payload["body"] = body
        await ws.send(json.dumps(payload))

    async def _heartbeat(self, ws: Any) -> None:
        """Send periodic PING (op=1) to keep the connection alive."""
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            try:
                await ws.send(json.dumps({"op": OP_PING}))
            except Exception:
                break

    async def _handle_raw(self, raw: str | bytes) -> None:
        """Parse and dispatch a raw WebSocket message."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Satori %s: received non-JSON message", self.instance_id)
            return

        op = data.get("op")
        body = data.get("body", {}) or {}

        if op == OP_READY:
            await self._handle_ready(body)
        elif op == OP_EVENT:
            await self._handle_event(body)
        elif op == OP_PING:
            # The protocol places PING on the client→server direction, but
            # some servers proxy ping frames back. Reply with PONG to be safe.
            if self._ws is not None:
                try:
                    await self._ws.send(json.dumps({"op": OP_PONG}))
                except Exception:
                    pass
        elif op == OP_PONG:
            pass  # Heartbeat response, no action needed
        elif op == OP_META:
            # Metadata update (experimental). Currently informational only;
            # accept and ignore the proxy_urls payload without crashing.
            pass

    async def _handle_ready(self, body: dict[str, Any]) -> None:
        """Process READY event: extract bot login info."""
        logins = body.get("logins", [])
        if logins:
            login = logins[0]
            user = login.get("user", {})
            self._self_id = user.get("id", "")
            self._detected_platform = login.get("platform", self.platform)
            self._notify_connection_state(True)
            logger.info(
                format_log_event(
                    "adapter.connection.ready",
                    adapter="satori",
                    instance_id=self.instance_id,
                    platform=self._detected_platform,
                    self_id=self._self_id,
                )
            )

    async def _handle_event(self, body: dict[str, Any]) -> None:
        """Parse a Satori event body into UnifiedEvent and dispatch.

        Implements dual-track design: message events (with content to parse)
        and notice events (with structured resources) are handled separately
        by message ingress.

        The adapter's responsibility is to:
          1. Validate the Satori JSON into UnifiedEvent with proper resource models
          2. Track the latest `sn` for session recovery on reconnect
          3. Emit the event as-is (message or notice)
          4. Let message ingress handle dual-track dispatching
        """
        if self._event_callback is None:
            return

        try:
            event = UnifiedEvent.model_validate(body)
        except Exception as e:
            logger.warning("Satori %s: failed to parse event body: %s", self.instance_id, e)
            return

        # Track the latest sequence number for session recovery on reconnect.
        if event.sn is not None:
            self._last_event_sn = event.sn

        should_download_resources = (
            self.config.auto_download_media or self.config.download_file_resources
        )
        if should_download_resources and event.message is not None and event.message.content:
            try:
                message = Message.from_xml(event.message.content)
                elements = await download_resource_elements(
                    message.elements,
                    self._resource_cache_dir,
                    download_media=self.config.auto_download_media,
                    download_files=self.config.download_file_resources,
                    max_bytes=self.config.max_resource_bytes,
                )
                event.message = event.message.model_copy(
                    update={"content": elements_to_xml(elements)}
                )
            except Exception:
                logger.exception("Satori %s failed to download message resources", self.instance_id)

        if self._event_queue is not None:
            self._enqueue_event(event)
            return

        await self._dispatch_event(event)

    async def _event_worker_loop(self) -> None:
        """Drain normalized events and invoke callback outside receive loop."""
        if self._event_queue is None:
            return

        while True:
            event = await self._event_queue.get()
            await self._dispatch_event(event)

    def _enqueue_event(self, event: UnifiedEvent) -> None:
        """Enqueue event without blocking receive loop.

        On overflow, drop the oldest queued event and keep the newest one.
        """
        if self._event_queue is None:
            return

        try:
            self._event_queue.put_nowait(event)
            return
        except asyncio.QueueFull:
            pass

        try:
            _ = self._event_queue.get_nowait()
        except asyncio.QueueEmpty:
            logger.warning(
                "Satori %s event queue overflow; dropping event %s", self.instance_id, event.type
            )
            return

        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning(
                "Satori %s event queue overflow; dropping event %s", self.instance_id, event.type
            )

    async def _dispatch_event(self, event: UnifiedEvent) -> None:
        if self._event_callback is None:
            return
        try:
            await self._event_callback(event)
        except Exception:
            logger.exception("Satori %s: event callback raised", self.instance_id)

    # ── Helpers ──────────────────────────────────────────────────────

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers per the Satori spec.

        Emits the standard ``Satori-Platform`` / ``Satori-User-ID`` headers
        and also the legacy ``X-Platform`` / ``X-Self-ID`` aliases for
        backward compatibility with older SDK builds.
        """
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        if self._detected_platform:
            headers["Satori-Platform"] = self._detected_platform
            headers["X-Platform"] = self._detected_platform  # legacy alias
        if self._self_id:
            headers["Satori-User-ID"] = self._self_id
            headers["X-Self-ID"] = self._self_id  # legacy alias
        return headers

    def _decode_session_id(self, session_id: str) -> str:
        """Convert ShinBot session URN to Satori channel_id.

        Mapping:
          {inst}:private:{user_id}             → "private:{user_id}"
          {inst}:group:{channel_id}            → "{channel_id}"
          {inst}:group:{guild_id}:{channel_id} → "{channel_id}"
        """
        # Strip the instance_id prefix
        colon_pos = session_id.find(":")
        if colon_pos == -1:
            return session_id
        rest = session_id[colon_pos + 1 :]  # "private:user" or "group:chan" or "group:g:c"

        if rest.startswith("private:"):
            return rest  # "private:{user_id}" is already the Satori channel_id for LLOneBot

        if rest.startswith("group:"):
            group_part = rest[len("group:") :]  # "{channel_id}" or "{guild}:{channel}"
            if ":" in group_part:
                return group_part.rsplit(":", 1)[1]  # Last segment = channel_id
            return group_part

        return rest
