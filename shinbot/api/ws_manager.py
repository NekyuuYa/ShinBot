"""WebSocket connection managers and async log broadcast infrastructure."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from shinbot.utils.logger import (
    display_log_level,
    get_logger,
    log_record_source,
    parse_log_event,
    should_emit_log_record,
)

logger = get_logger(__name__, source="api.ws", color="bright_blue")


class ConnectionManager:
    """Fan-out broadcaster for a set of active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        """Accept and register a new WebSocket client.

        If the socket is still in the CONNECTING state it is accepted first.
        The client is then added to the tracked connection set.

        Args:
            ws: The incoming WebSocket connection to register.
        """
        if ws.application_state == WebSocketState.CONNECTING:
            await ws.accept()
        self._connections.add(ws)
        logger.debug("WS client connected (%d total)", len(self._connections))

    def disconnect(self, ws: WebSocket) -> None:
        """Remove a WebSocket client from the tracked connection set.

        If the client is not currently tracked the call is a safe no-op.

        Args:
            ws: The WebSocket connection to unregister.
        """
        self._connections.discard(ws)
        logger.debug("WS client disconnected (%d total)", len(self._connections))

    async def broadcast(self, data: Any) -> None:
        """Send a JSON-serialisable payload to every connected client.

        Clients are notified concurrently so a slow or failed connection
        does not block delivery to healthy ones.  Any client that errors
        during send is automatically disconnected.

        Args:
            data: JSON-serialisable payload (dict, list, str, etc.) to send.
        """
        connections = list(self._connections)
        if not connections:
            return
        # Send to all clients concurrently so a slow or dead client does not
        # block delivery to healthy ones.
        results = await asyncio.gather(
            *[ws.send_json(data) for ws in connections],
            return_exceptions=True,
        )
        for ws, result in zip(connections, results, strict=False):
            if isinstance(result, Exception):
                self._connections.discard(ws)

    @property
    def count(self) -> int:
        """Return the number of currently connected WebSocket clients."""
        return len(self._connections)


class _AsyncLogHandler(logging.Handler):
    """Logging handler that drains into an asyncio.Queue for WebSocket fan-out."""

    def __init__(self, queue: asyncio.Queue) -> None:  # type: ignore[type-arg]
        super().__init__()
        self._queue = queue
        self.formatter = logging.Formatter("%(message)s")

    def emit(self, record: logging.LogRecord) -> None:
        if not should_emit_log_record(record):
            return
        try:
            msg = self.format(record).strip()
            payload = {
                "ts": int(record.created),
                "timestamp": int(record.created * 1000),
                "level": display_log_level(record),
                "logger": record.name,
                "source": log_record_source(record),
                "message": msg,
            }
            structured = parse_log_event(msg)
            if structured:
                payload.update(structured)
            try:
                self._queue.put_nowait(payload)
            except asyncio.QueueFull:
                pass  # drop on back-pressure; WebSocket consumers are optional
        except Exception:
            self.handleError(record)


# ── Module-level singletons ──────────────────────────────────────────

log_manager = ConnectionManager()
status_manager = ConnectionManager()

_log_queue: asyncio.Queue | None = None  # type: ignore[type-arg]
_log_handler: _AsyncLogHandler | None = None
_log_queue_loop: asyncio.AbstractEventLoop | None = None


def get_log_queue() -> asyncio.Queue:  # type: ignore[type-arg]
    """Return the module-level async log queue, creating it if needed.

    The queue is scoped to the running event loop.  If called from a
    different loop the previous queue is replaced so that handlers always
    target the correct loop.

    Returns:
        An ``asyncio.Queue`` of log payloads (max size 1000).
    """
    global _log_queue, _log_queue_loop
    current_loop = asyncio.get_running_loop()
    if _log_queue is None or _log_queue_loop is not current_loop:
        _log_queue = asyncio.Queue(maxsize=1000)
        _log_queue_loop = current_loop
        if _log_handler is not None:
            _log_handler._queue = _log_queue
    return _log_queue


def install_log_handler() -> None:
    """Attach a non-blocking async handler to the root logger (idempotent)."""
    global _log_handler
    if _log_handler is not None:
        return  # already installed
    queue = get_log_queue()
    _log_handler = _AsyncLogHandler(queue)
    _log_handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(_log_handler)
    logger.debug("Async log handler installed for WebSocket streaming")


async def log_broadcaster() -> None:
    """Background task: dequeue log records and broadcast to all /ws/logs clients."""
    queue = get_log_queue()
    while True:
        try:
            payload = await queue.get()
            if log_manager.count > 0:
                await log_manager.broadcast(payload)
        except asyncio.CancelledError:
            break
        except Exception:
            pass  # never crash the broadcaster
