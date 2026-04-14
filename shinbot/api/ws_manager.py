"""WebSocket connection managers and async log broadcast infrastructure."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Fan-out broadcaster for a set of active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)
        logger.debug("WS client connected (%d total)", len(self._connections))

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)
        logger.debug("WS client disconnected (%d total)", len(self._connections))

    async def broadcast(self, data: Any) -> None:
        dead: list[WebSocket] = []
        for ws in list(self._connections):
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._connections.discard(ws)

    @property
    def count(self) -> int:
        return len(self._connections)


class _AsyncLogHandler(logging.Handler):
    """Logging handler that drains into an asyncio.Queue for WebSocket fan-out."""

    def __init__(self, queue: asyncio.Queue) -> None:  # type: ignore[type-arg]
        super().__init__()
        self._queue = queue
        self.formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def emit(self, record: logging.LogRecord) -> None:
        # 核心修复：禁止转发 uvicorn 和 websockets 的低级别日志，防止回放死循环
        if record.name.startswith(("uvicorn", "websockets")):
            if record.levelno < logging.INFO:
                return

        try:
            msg = self.format(record)
            payload = {
                "ts": int(record.created),
                "level": record.levelname,
                "logger": record.name,
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


def get_log_queue() -> asyncio.Queue:  # type: ignore[type-arg]
    global _log_queue
    if _log_queue is None:
        _log_queue = asyncio.Queue(maxsize=1000)
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
