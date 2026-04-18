"""WebSocket connection managers and async log broadcast infrastructure."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from shinbot.utils.logger import display_log_level, shorten_logger_name

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Fan-out broadcaster for a set of active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        if ws.application_state == WebSocketState.CONNECTING:
            await ws.accept()
        self._connections.add(ws)
        logger.debug("WS client connected (%d total)", len(self._connections))

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        logger.debug("WS client disconnected (%d total)", len(self._connections))

    async def broadcast(self, data: Any) -> None:
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
        return len(self._connections)


class _AsyncLogHandler(logging.Handler):
    """Logging handler that drains into an asyncio.Queue for WebSocket fan-out."""

    def __init__(self, queue: asyncio.Queue) -> None:  # type: ignore[type-arg]
        super().__init__()
        self._queue = queue
        self.formatter = logging.Formatter("%(message)s")

    def emit(self, record: logging.LogRecord) -> None:
        # Keep downgraded transport noise visible in the dashboard, but still
        # avoid forwarding raw low-level transport debug chatter.
        if record.name.startswith(("uvicorn", "websockets")):
            if record.levelno < logging.INFO and not record.__dict__.get(
                "_shinbot_downgraded", False
            ):
                return

        try:
            msg = self.format(record).strip()
            payload = {
                "ts": int(record.created),
                "timestamp": int(record.created * 1000),
                "level": display_log_level(record),
                "logger": record.name,
                "source": shorten_logger_name(record.name),
                "message": msg,
            }
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
