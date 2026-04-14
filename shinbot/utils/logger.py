"""Unified logging utilities for ShinBot.

Provides:
- Colored console output
- Hook slot for WebSocket log fan-out (registered by api layer at startup)
- Namespaced helper loggers for plugins
"""

from __future__ import annotations

import logging
from collections.abc import Callable

_CONFIGURED = False
_log_handler_installer: Callable[[], None] | None = None
_NOISY_THIRD_PARTY_LOGGERS = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "websockets",
    "websockets.client",
    "websockets.server",
)


def register_log_handler_installer(fn: Callable[[], None]) -> None:
    """Register a callback that installs additional log handlers.

    Called by the API layer during its own initialization to bridge
    root logs to the WebSocket fan-out queue. This keeps utils free
    of upward imports into shinbot.api.
    """
    global _log_handler_installer
    _log_handler_installer = fn


class _ColorFormatter(logging.Formatter):
    _RESET = "\033[0m"
    _COLORS = {
        logging.DEBUG: "\033[36m",
        logging.INFO: "\033[32m",
        logging.WARNING: "\033[33m",
        logging.ERROR: "\033[31m",
        logging.CRITICAL: "\033[35m",
    }

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        color = self._COLORS.get(record.levelno, "")
        if not color:
            return message
        return f"{color}{message}{self._RESET}"


def setup_logging(level_name: str = "INFO") -> None:
    """Configure root logging once with console + websocket handlers."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    level = getattr(logging, level_name.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(
        _ColorFormatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(console)
    _configure_third_party_loggers(level)

    # Bridge root logs to /ws/logs fan-out queue if the API layer
    # registered an installer via register_log_handler_installer().
    if _log_handler_installer is not None:
        try:
            _log_handler_installer()
        except Exception:
            pass

    _CONFIGURED = True


def _configure_third_party_loggers(root_level: int) -> None:
    """Clamp verbose dependency loggers so app DEBUG doesn't spam transport internals."""
    third_party_level = max(root_level, logging.INFO)
    for name in _NOISY_THIRD_PARTY_LOGGERS:
        logger = logging.getLogger(name)
        logger.setLevel(third_party_level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def get_plugin_logger(plugin_id: str) -> logging.Logger:
    return get_logger(f"shinbot.plugin.{plugin_id}")
