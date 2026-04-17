"""Unified logging utilities for ShinBot.

Provides:
- Colored console output
- Hook slot for WebSocket log fan-out (registered by api layer at startup)
- Namespaced helper loggers for plugins
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

_CONFIGURED = False
_log_handler_installer: Callable[[], None] | None = None
_ORIGINAL_LOG_RECORD_FACTORY = logging.getLogRecordFactory()
_LOGGER_PREFIX = "shinbot."
_NOISY_THIRD_PARTY_LOGGERS = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "websockets",
    "websockets.client",
    "websockets.server",
)


def should_downgrade_noisy_log(record: logging.LogRecord) -> bool:
    if record.levelno > logging.INFO:
        return False
    return any(record.name.startswith(name) for name in _NOISY_THIRD_PARTY_LOGGERS)


def _downgrade_noisy_log_record(record: logging.LogRecord) -> logging.LogRecord:
    if should_downgrade_noisy_log(record):
        record.levelno = logging.DEBUG
        record.levelname = "DEBUG"
        record.__dict__["_shinbot_downgraded"] = True
    return record


def _log_record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
    record = _ORIGINAL_LOG_RECORD_FACTORY(*args, **kwargs)
    return _downgrade_noisy_log_record(record)


def display_log_level(record: logging.LogRecord) -> str:
    return normalize_log_level(record.levelname)


def normalize_log_level(level_name: str) -> str:
    """Normalize stdlib logging names for compact display."""
    upper = level_name.upper()
    if upper == "WARNING":
        return "WARN"
    if upper == "CRITICAL":
        return "ERROR"
    return upper


def shorten_logger_name(logger_name: str, *, keep_parts: int = 3) -> str:
    """Keep logger names short enough for fast visual scanning."""
    normalized = logger_name.strip()
    if not normalized:
        return "root"

    if normalized.startswith(_LOGGER_PREFIX):
        normalized = normalized[len(_LOGGER_PREFIX) :]

    parts = [part for part in normalized.split(".") if part]
    if not parts:
        return "root"
    if len(parts) <= keep_parts:
        return ".".join(parts)
    return ".".join(parts[-keep_parts:])


def _stringify_log_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list, tuple, set)):
        if isinstance(value, set):
            value = sorted(value)
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def format_log_event(event: str, /, **fields: Any) -> str:
    """Build compact key-value log lines without noisy empty fields."""
    parts = [event]
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (dict, list, tuple, set)) and not value:
            continue
        parts.append(f"{key}={_stringify_log_value(value)}")
    return " | ".join(parts)


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


class _ReadableContextFilter(logging.Filter):
    def __init__(self, *, keep_logger_parts: int = 3) -> None:
        super().__init__()
        self._keep_logger_parts = keep_logger_parts

    def filter(self, record: logging.LogRecord) -> bool:
        record.level_tag = display_log_level(record)
        record.short_name = shorten_logger_name(record.name, keep_parts=self._keep_logger_parts)
        return True


def setup_logging(level_name: str = "INFO") -> None:
    """Configure root logging once with console + websocket handlers."""
    global _CONFIGURED
    if _CONFIGURED:
        return

    logging.setLogRecordFactory(_log_record_factory)

    level = getattr(logging, level_name.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setLevel(level)
    console.addFilter(_ReadableContextFilter())
    console.setFormatter(
        _ColorFormatter(
            "%(asctime)s | %(level_tag)-5s | %(short_name)-30s | %(message)s",
            datefmt="%H:%M:%S",
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
