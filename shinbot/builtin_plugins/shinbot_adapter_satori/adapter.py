"""Satori protocol WebSocket adapter for ShinBot.

Connects to any Satori-compatible server (LLOneBot, official Satori, etc.)
via WebSocket and exposes the standard BaseAdapter interface.

Wire format:
  - op=0: EVENT — incoming platform events → UnifiedEvent
  - op=4: READY — server sends login info on connect
  - op=3: PING — client heartbeat
  - op=6: PONG — server heartbeat reply

HTTP API (send):
  POST /v1/channel.message.create
  Headers: Authorization: Bearer <token>
           X-Platform: <platform>
           X-Self-ID: <bot_id>
  Body:    {"channel_id": "...", "content": "<xml>..."}
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import websockets

from shinbot.core.adapter_manager import BaseAdapter, MessageHandle
from shinbot.models.elements import Message, MessageElement
from shinbot.models.events import UnifiedEvent
from shinbot.utils.logger import get_logger
from shinbot.utils.resource_ingress import download_resource_elements
from shinbot.utils.satori_parser import elements_to_xml

logger = get_logger(__name__)

# Satori opcodes
OP_EVENT = 0
OP_PING = 3
OP_READY = 4
OP_PONG = 6

# Heartbeat interval in seconds (slightly less than typical server timeout)
HEARTBEAT_INTERVAL = 10


@dataclass
class SatoriConfig:
    """Connection configuration for the Satori adapter."""

    host: str  # e.g. "localhost:5140"
    token: str = ""  # Authorization token (required if server enforces auth)
    path: str = "/v1/events"  # WebSocket endpoint path
    reconnect_delay: float = 5.0  # Seconds between reconnection attempts
    max_reconnects: int = -1  # -1 = infinite retries
    download_resources: bool = False
    resource_cache_dir: str = "data/temp/resources"
    silent_reconnect: bool = True
    reconnect_log_interval: float = 30.0


class SatoriAdapter(BaseAdapter):
    """Connects to a Satori-compatible WebSocket server.

    Handles the full lifecycle:
      1. WebSocket connect + READY handshake
      2. Heartbeat (PING/PONG)
      3. Event dispatch to the registered callback
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
        self._http: httpx.AsyncClient | None = None
        self._resource_cache_dir = Path(self.config.resource_cache_dir)
        self._resource_cache_dir.mkdir(parents=True, exist_ok=True)

    # ── BaseAdapter interface ────────────────────────────────────────

    async def start(self) -> None:
        """Start the WebSocket listener in the background."""
        self._running = True
        self._http = httpx.AsyncClient(timeout=30.0)
        self._recv_task = asyncio.create_task(
            self._connection_loop(), name=f"satori-{self.instance_id}"
        )
        logger.info("Satori adapter %s started (host=%s)", self.instance_id, self.config.host)

    async def shutdown(self) -> None:
        """Gracefully close the WebSocket and cancel background tasks."""
        self._running = False
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._http:
            await self._http.aclose()
        logger.info("Satori adapter %s shut down", self.instance_id)

    async def send(self, target_session: str, elements: list[MessageElement]) -> MessageHandle:
        """Serialize elements to Satori XML and POST to channel.message.create."""
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

        Standard methods use dotted names (e.g. "channel.message.create").
        Internal methods use the "internal.{platform}.{action}" namespace.
        """
        if self._http is None:
            raise RuntimeError("Adapter not started — call start() first")

        params = dict(params)
        params.pop("session_id", None)
        if method == "message.update" and isinstance(params.get("elements"), list):
            params["content"] = elements_to_xml(params.pop("elements"))

        # Convert dotted method to URL path (e.g. channel.message.create → /v1/channel.message.create)
        base = f"http://{self.config.host}"
        if method.startswith("internal."):
            # internal.{platform}.{action} → /v1/internal/{action}
            path = "/v1/" + method.replace(".", "/", 1).replace(".", "/", 1)
        else:
            path = f"/v1/{method.replace('.', '/')}"

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
                    logger.info("Satori %s reconnected successfully", self.instance_id)
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
                        "Satori %s reconnecting in %.1fs (attempt %d): %s",
                        self.instance_id,
                        self.config.reconnect_delay,
                        attempt,
                        e,
                    )
                    last_log_ts = now
                if self.config.max_reconnects >= 0 and attempt > self.config.max_reconnects:
                    logger.error(
                        "Satori %s: max reconnect attempts reached, giving up", self.instance_id
                    )
                    break
                await asyncio.sleep(self.config.reconnect_delay)

    async def _connect_and_receive(self) -> None:
        """Open a single WebSocket session and process it until disconnect."""
        ws_url = f"ws://{self.config.host}{self.config.path}"
        headers = {}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"

        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            self._ws = ws
            logger.info("Satori %s connected to %s", self.instance_id, ws_url)

            # Start heartbeat
            self._ping_task = asyncio.create_task(self._heartbeat(ws))

            try:
                async for raw in ws:
                    await self._handle_raw(raw)
            finally:
                if self._ping_task and not self._ping_task.done():
                    self._ping_task.cancel()
                self._ws = None

    async def _heartbeat(self, ws: Any) -> None:
        """Send periodic PING to keep the connection alive."""
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
        body = data.get("body", {})

        if op == OP_READY:
            await self._handle_ready(body)
        elif op == OP_EVENT:
            await self._handle_event(body)
        elif op == OP_PONG:
            pass  # Heartbeat response, no action needed

    async def _handle_ready(self, body: dict[str, Any]) -> None:
        """Process READY event: extract bot login info."""
        logins = body.get("logins", [])
        if logins:
            login = logins[0]
            user = login.get("user", {})
            self._self_id = user.get("id", "")
            self._detected_platform = login.get("platform", self.platform)
            logger.info(
                "Satori %s READY: platform=%s self_id=%s",
                self.instance_id,
                self._detected_platform,
                self._self_id,
            )

    async def _handle_event(self, body: dict[str, Any]) -> None:
        """Parse a Satori event body into UnifiedEvent and dispatch.

        Implements dual-track design: message events (with content to parse)
        and notice events (with structured resources) are handled separately
        by the pipeline.

        The adapter's responsibility is to:
          1. Validate the Satori JSON into UnifiedEvent with proper resource models
          2. Emit the event as-is (message or notice)
          3. Let the pipeline handle the dual-track dispatching
        """
        if self._event_callback is None:
            return

        try:
            event = UnifiedEvent.model_validate(body)
        except Exception as e:
            logger.warning("Satori %s: failed to parse event body: %s", self.instance_id, e)
            return

        if self.config.download_resources and event.message is not None and event.message.content:
            try:
                message = Message.from_xml(event.message.content)
                elements = await download_resource_elements(
                    message.elements,
                    self._resource_cache_dir,
                )
                event.message = event.message.model_copy(
                    update={"content": elements_to_xml(elements)}
                )
            except Exception:
                logger.exception("Satori %s failed to download message resources", self.instance_id)

        try:
            await self._event_callback(event)
        except Exception:
            logger.exception("Satori %s: event callback raised", self.instance_id)

    def _enrich_poke(self, event: UnifiedEvent, raw_body: dict[str, Any]) -> UnifiedEvent:
        """Deprecated: This method is kept for backward compatibility only.

        The new dual-track design preserves notice events as-is with their
        structured resource payloads instead of converting them to message events.

        Notice events like poke, guild-member-added, etc. are now dispatched
        directly through the EventBus with their resource fields populated.

        This method is no longer called by _handle_event and can be removed
        in a future version.
        """
        logger.debug(
            "Satori %s: _enrich_poke called but not used in dual-track design", self.instance_id
        )
        return event

    # ── Helpers ──────────────────────────────────────────────────────

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        if self._detected_platform:
            headers["X-Platform"] = self._detected_platform
        if self._self_id:
            headers["X-Self-ID"] = self._self_id
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
