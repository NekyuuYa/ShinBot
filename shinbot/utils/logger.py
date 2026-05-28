"""Unified logging utilities for ShinBot.

Provides:
- Colored console output
- Hook slot for WebSocket log fan-out (registered by api layer at startup)
- Namespaced helper loggers for plugins
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Literal, TextIO
from unicodedata import combining, east_asian_width

_log_handler_installer: Callable[[], None] | None = None
_LOGGER_PREFIX = "shinbot."
_NOISY_THIRD_PARTY_LOGGERS = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "uvicorn.asgi",
    "uvicorn.protocols",
    "uvicorn.protocols.http",
    "uvicorn.protocols.websockets",
    "uvicorn.protocols.websockets.websockets_impl",
    "websockets",
    "websockets.client",
    "websockets.server",
    "websockets.legacy",
    "websockets.legacy.client",
    "websockets.legacy.server",
    "websockets.protocol",
)
_CONSOLE_SOURCE_WIDTH = 20
_CONSOLE_LOG_FORMAT = "%(asctime)s %(level_tag)-5s %(source_tag)s | %(message)s"
_CONSOLE_DATE_FORMAT = "%H:%M:%S"
_LOG_LEVEL_NAMES = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_THIRD_PARTY_NOISE_POLICIES = ("off", "debug", "on")
ThirdPartyNoisePolicy = Literal["off", "debug", "on"]


@dataclass(frozen=True, slots=True)
class LogSourceRegistration:
    source: str
    color: str = ""

    def to_payload(self, logger_name: str) -> dict[str, str]:
        """Serialize the registration to a JSON-friendly dict."""
        return {
            "loggerName": logger_name,
            "source": self.source,
            "color": self.color,
        }


def display_log_level(record: logging.LogRecord) -> str:
    """Return the normalized level name for display from a log record."""
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


def format_console_source(source: str, *, width: int = _CONSOLE_SOURCE_WIDTH) -> str:
    """Return a fixed-width logger label for aligned console output."""

    if width <= 0:
        return ""
    label = str(source or "root").strip() or "root"
    if _display_width(label) > width:
        if width <= 3:
            label = _take_right_cells(label, width)
        else:
            label = "..." + _take_right_cells(label, width - 3)
    return label + (" " * max(0, width - _display_width(label)))


def _display_width(text: str) -> int:
    """Return terminal cell width for ASCII/CJK text without ANSI escape codes."""

    width = 0
    for char in text:
        if combining(char):
            continue
        width += 2 if east_asian_width(char) in {"F", "W"} else 1
    return width


def _take_right_cells(text: str, width: int) -> str:
    if width <= 0:
        return ""
    used = 0
    chars: list[str] = []
    for char in reversed(text):
        char_width = 0 if combining(char) else 2 if east_asian_width(char) in {"F", "W"} else 1
        if used + char_width > width:
            break
        chars.append(char)
        used += char_width
    return "".join(reversed(chars))


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


def parse_log_event(message: str) -> dict[str, Any]:
    """Parse a compact ``format_log_event`` message for log stream filters."""

    text = str(message or "").strip()
    if not text:
        return {}
    parts = [part.strip() for part in text.split(" | ") if part.strip()]
    if not parts:
        return {}
    fields: dict[str, Any] = {}
    for part in parts[1:]:
        key, separator, value = part.partition("=")
        if not separator:
            continue
        normalized_key = key.strip()
        if not normalized_key:
            continue
        fields[normalized_key] = _parse_log_field_value(value.strip())
    return {"event": parts[0], "fields": fields}


def _parse_log_field_value(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    if not value:
        return ""
    if value[0] in "[{\"":
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def is_third_party_noise(record: logging.LogRecord) -> bool:
    """Return whether a record is low-level transport/library chatter."""

    if record.levelno >= logging.WARNING:
        return False
    return any(_logger_matches(record.name, name) for name in _NOISY_THIRD_PARTY_LOGGERS)


def should_emit_log_record(record: logging.LogRecord) -> bool:
    """Return whether a record should be shown by ShinBot log surfaces."""

    return runtime_log_manager.should_emit(record)


def _logger_matches(logger_name: str, namespace: str) -> bool:
    return logger_name == namespace or logger_name.startswith(f"{namespace}.")


def register_log_handler_installer(fn: Callable[[], None]) -> None:
    """Register a callback that installs additional log handlers.

    Called by the API layer during its own initialization to bridge
    root logs to the WebSocket fan-out queue. This keeps utils free
    of upward imports into shinbot.api.
    """
    global _log_handler_installer
    _log_handler_installer = fn


class RuntimeLogManager:
    """Own runtime logging setup, display sources, and third-party noise policy."""

    def __init__(self) -> None:
        self._configured = False
        self._source_by_logger_name: dict[str, LogSourceRegistration] = {}
        self._third_party_noise_policy: ThirdPartyNoisePolicy = "debug"

    def setup_logging(
        self,
        level_name: str = "INFO",
        *,
        third_party_noise: str = "debug",
    ) -> None:
        """Configure root logging with console and optional WebSocket handlers."""

        self.set_third_party_noise_policy(third_party_noise)
        if self._configured:
            self.set_root_log_level(level_name)
            return

        level = _level_from_name(level_name, default=logging.INFO)
        root = logging.getLogger()
        root.setLevel(level)
        root.addHandler(self.build_console_handler(level))

        if _log_handler_installer is not None:
            try:
                _log_handler_installer()
            except Exception:
                pass

        self._configured = True

    def build_console_handler(
        self,
        level: int,
        *,
        stream: TextIO | None = None,
        use_color: bool = True,
    ) -> logging.Handler:
        """Build a console logging handler with optional color support.

        Args:
            level: Minimum log level for the handler.
            stream: Output stream (defaults to stderr).
            use_color: Whether to emit ANSI color codes.

        Returns:
            A configured logging.Handler instance.
        """
        console = logging.StreamHandler(stream)
        console.setLevel(level)
        console.addFilter(_ReadableContextFilter(self))
        formatter_cls = _ColorFormatter if use_color else _PlainFormatter
        console.setFormatter(
            formatter_cls(
                _CONSOLE_LOG_FORMAT,
                datefmt=_CONSOLE_DATE_FORMAT,
            )
        )
        console._shinbot_console_handler = True  # type: ignore[attr-defined]
        return console

    def replace_console_handler(
        self,
        *,
        stream: TextIO | None = None,
        use_color: bool = True,
    ) -> None:
        """Replace ShinBot's console handler while keeping other handlers attached."""

        root = logging.getLogger()
        for handler in list(self.iter_console_handlers(root)):
            root.removeHandler(handler)
        root.addHandler(self.build_console_handler(root.level, stream=stream, use_color=use_color))

    def set_root_log_level(self, level_name: str) -> str:
        """Update root and console handler levels."""

        normalized = normalize_root_log_level(level_name)
        level = getattr(logging, normalized, None)
        if not isinstance(level, int):
            raise ValueError(f"Unsupported log level: {level_name}")

        root = logging.getLogger()
        root.setLevel(level)
        for handler in root.handlers:
            if getattr(handler, "_shinbot_console_handler", False):
                handler.setLevel(level)
        return normalized

    def set_third_party_noise_policy(
        self,
        policy: str,
        *,
        strict: bool = False,
    ) -> ThirdPartyNoisePolicy:
        """Set how low-level dependency chatter is displayed."""

        normalized = normalize_third_party_noise_policy(policy, strict=strict)
        self._third_party_noise_policy = normalized
        return self._third_party_noise_policy

    def third_party_noise_policy(self) -> ThirdPartyNoisePolicy:
        """Return the current third-party noise policy."""
        return self._third_party_noise_policy

    def apply_runtime_config(
        self,
        *,
        level_name: str | None = None,
        third_party_noise: str | None = None,
    ) -> dict[str, Any]:
        """Apply partial logging configuration and return the updated snapshot.

        Args:
            level_name: New root log level, or None to leave unchanged.
            third_party_noise: New noise policy, or None to leave unchanged.

        Returns:
            The current logging state after applying changes.
        """
        if level_name is not None:
            self.set_root_log_level(level_name)
        if third_party_noise is not None:
            self.set_third_party_noise_policy(third_party_noise, strict=True)
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        """Return runtime logging state for APIs and operator tools."""

        root = logging.getLogger()
        return {
            "level": logging.getLevelName(root.level),
            "effectiveLevel": logging.getLevelName(root.getEffectiveLevel()),
            "thirdPartyNoise": self._third_party_noise_policy,
            "sourceWidth": _CONSOLE_SOURCE_WIDTH,
            "availableLevels": list(_LOG_LEVEL_NAMES),
            "availableThirdPartyNoise": list(_THIRD_PARTY_NOISE_POLICIES),
            "availableColors": sorted(_ANSI_COLORS),
            "sources": self.source_payloads(),
            "handlers": self.handler_payloads(root),
        }

    def source_payloads(self) -> list[dict[str, str]]:
        """Return all registered source payloads for API responses."""
        return [
            registration.to_payload(logger_name)
            for logger_name, registration in sorted(self._source_by_logger_name.items())
        ]

    @staticmethod
    def handler_payloads(root: logging.Logger) -> list[dict[str, Any]]:
        """Return a summary of all handlers attached to the root logger."""
        handlers: list[dict[str, Any]] = []
        for handler in root.handlers:
            handlers.append(
                {
                    "type": type(handler).__name__,
                    "level": logging.getLevelName(handler.level),
                    "console": bool(getattr(handler, "_shinbot_console_handler", False)),
                }
            )
        return handlers

    def should_emit(self, record: logging.LogRecord) -> bool:
        """Return whether a record should be shown by ShinBot log surfaces."""

        if not is_third_party_noise(record):
            return True
        if self._third_party_noise_policy == "off":
            return False
        if self._third_party_noise_policy == "on":
            return True
        return logging.getLogger().getEffectiveLevel() <= logging.DEBUG

    def register_source(self, logger_name: str, source: str, *, color: str = "") -> None:
        """Register a compact display source for a logger name or namespace."""

        normalized_name = str(logger_name or "").strip()
        normalized_source = str(source or "").strip()
        if not normalized_name or not normalized_source:
            return
        self._source_by_logger_name[normalized_name] = LogSourceRegistration(
            source=normalized_source,
            color=_normalize_source_color(color),
        )

    def record_source(self, record: logging.LogRecord) -> str:
        """Resolve a display source for a log record."""

        explicit = str(getattr(record, "shinbot_source", "") or "").strip()
        if explicit:
            return explicit
        registered = self.source_for_logger(record.name)
        if registered:
            return registered.source
        return shorten_logger_name(record.name)

    def record_source_color(self, record: logging.LogRecord) -> str:
        """Resolve the optional source color for a log record."""

        explicit = _normalize_source_color(getattr(record, "shinbot_source_color", ""))
        if explicit:
            return explicit
        registered = self.source_for_logger(record.name)
        return registered.color if registered is not None else ""

    def source_for_logger(self, logger_name: str) -> LogSourceRegistration | None:
        """Return the longest registered source alias for one logger name."""

        normalized = str(logger_name or "").strip()
        best_name = ""
        best_registration: LogSourceRegistration | None = None
        for candidate, source in self._source_by_logger_name.items():
            if normalized == candidate or normalized.startswith(f"{candidate}."):
                if len(candidate) > len(best_name):
                    best_name = candidate
                    best_registration = source
        return best_registration

    @staticmethod
    def iter_console_handlers(root: logging.Logger) -> Iterator[logging.Handler]:
        """Yield only ShinBot-marked console handlers from the root logger."""
        for handler in root.handlers:
            if getattr(handler, "_shinbot_console_handler", False):
                yield handler


_LEVEL_COLORS = {
    logging.DEBUG: "cyan",
    logging.INFO: "green",
    logging.WARNING: "yellow",
    logging.ERROR: "red",
    logging.CRITICAL: "magenta",
}
_ANSI_COLORS = {
    "black": "\033[30m",
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "blue": "\033[34m",
    "magenta": "\033[35m",
    "cyan": "\033[36m",
    "white": "\033[37m",
    "gray": "\033[90m",
    "grey": "\033[90m",
    "bright_black": "\033[90m",
    "bright_red": "\033[91m",
    "bright_green": "\033[92m",
    "bright_yellow": "\033[93m",
    "bright_blue": "\033[94m",
    "bright_magenta": "\033[95m",
    "bright_cyan": "\033[96m",
    "bright_white": "\033[97m",
}
_ANSI_RESET = "\033[0m"


def _normalize_source_color(color: Any) -> str:
    normalized = str(color or "").strip().lower().replace("-", "_")
    return normalized if normalized in _ANSI_COLORS else ""


def _color_text(text: str, color: str) -> str:
    code = _ANSI_COLORS.get(_normalize_source_color(color))
    if not code:
        return text
    return f"{code}{text}{_ANSI_RESET}"


def _format_prefixed_message(prefix: str, plain_prefix: str, message: str) -> str:
    lines = str(message).replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if not lines:
        return prefix.rstrip()
    indent = " " * _display_width(plain_prefix)
    rendered = [f"{prefix}{lines[0]}"]
    rendered.extend(f"{indent}{line}" for line in lines[1:])
    return "\n".join(rendered)


class _ConsoleFormatter(logging.Formatter):
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        *,
        use_color: bool,
    ) -> None:
        super().__init__(fmt, datefmt=datefmt)
        self._use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        message = record.message
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
            if record.exc_text:
                message = f"{message}\n{record.exc_text}" if message else record.exc_text
        if record.stack_info:
            stack_info = self.formatStack(record.stack_info)
            message = f"{message}\n{stack_info}" if message else stack_info

        time_tag = self.formatTime(record, self.datefmt)
        level_tag = str(getattr(record, "level_tag", display_log_level(record))).ljust(5)
        source_tag = str(
            getattr(record, "source_tag", format_console_source(shorten_logger_name(record.name)))
        )

        plain_prefix = f"{time_tag} {level_tag} {source_tag} | "
        if self._use_color:
            level_tag = _color_text(level_tag, _LEVEL_COLORS.get(record.levelno, ""))
            source_tag = _color_text(source_tag, str(getattr(record, "source_color", "")))
        prefix = f"{time_tag} {level_tag} {source_tag} | "
        return _format_prefixed_message(prefix, plain_prefix, message)


class _ColorFormatter(_ConsoleFormatter):
    def __init__(self, fmt: str | None = None, datefmt: str | None = None) -> None:
        super().__init__(fmt, datefmt=datefmt, use_color=True)


class _PlainFormatter(_ConsoleFormatter):
    def __init__(self, fmt: str | None = None, datefmt: str | None = None) -> None:
        super().__init__(fmt, datefmt=datefmt, use_color=False)


class _ReadableContextFilter(logging.Filter):
    def __init__(
        self,
        manager: RuntimeLogManager,
        *,
        keep_logger_parts: int = 3,
    ) -> None:
        super().__init__()
        self._manager = manager
        self._keep_logger_parts = keep_logger_parts

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._manager.should_emit(record):
            return False
        record.level_tag = display_log_level(record)
        record.short_name = shorten_logger_name(record.name, keep_parts=self._keep_logger_parts)
        record.source_label = self._manager.record_source(record)
        record.source_tag = format_console_source(record.source_label)
        record.source_color = self._manager.record_source_color(record)
        return True


runtime_log_manager = RuntimeLogManager()


def build_console_handler(
    level: int,
    *,
    stream: TextIO | None = None,
    use_color: bool = True,
) -> logging.Handler:
    """Build a console handler using the global runtime log manager.

    Args:
        level: Minimum log level for the handler.
        stream: Output stream (defaults to stderr).
        use_color: Whether to emit ANSI color codes.

    Returns:
        A configured logging.Handler instance.
    """
    return runtime_log_manager.build_console_handler(
        level,
        stream=stream,
        use_color=use_color,
    )


def register_log_source(logger_name: str, source: str, *, color: str = "") -> None:
    """Register a compact display source for a logger name or namespace."""

    runtime_log_manager.register_source(logger_name, source, color=color)


def log_record_source(record: logging.LogRecord) -> str:
    """Return the display source used by console and WebSocket log streams."""

    return runtime_log_manager.record_source(record)


def setup_logging(level_name: str = "INFO", *, third_party_noise: str = "debug") -> None:
    """Configure root logging once with console + websocket handlers."""
    runtime_log_manager.setup_logging(level_name, third_party_noise=third_party_noise)


def set_third_party_noise_policy(policy: str) -> ThirdPartyNoisePolicy:
    """Set how low-level dependency chatter is displayed."""
    return runtime_log_manager.set_third_party_noise_policy(policy)


def logging_runtime_snapshot() -> dict[str, Any]:
    """Return runtime logging state for APIs and operator tools."""
    return runtime_log_manager.snapshot()


def apply_logging_runtime_config(
    *,
    level_name: str | None = None,
    third_party_noise: str | None = None,
) -> dict[str, Any]:
    """Apply runtime logging config and return the updated state."""
    return runtime_log_manager.apply_runtime_config(
        level_name=level_name,
        third_party_noise=third_party_noise,
    )


def replace_console_handler(*, stream: TextIO | None = None, use_color: bool = True) -> None:
    """Replace ShinBot's console handler while keeping other handlers attached."""
    runtime_log_manager.replace_console_handler(stream=stream, use_color=use_color)


def set_root_log_level(level_name: str) -> str:
    """Update root and console handler levels."""
    return runtime_log_manager.set_root_log_level(level_name)


def get_logger(name: str, *, source: str = "", color: str = "") -> logging.Logger:
    """Return a namespaced logger with an optional compact display source.

    Args:
        name: Logger name (typically a dotted namespace).
        source: Short display label for console output.
        color: ANSI color name for the source label.
    """
    if source:
        register_log_source(name, source, color=color)
    return logging.getLogger(name)


def get_plugin_logger(plugin_id: str, *, color: str = "") -> logging.Logger:
    """Return a logger pre-configured for a specific plugin.

    Args:
        plugin_id: The plugin's unique identifier.
        color: ANSI color name for the console source label.
    """
    return get_logger(f"shinbot.plugin.{plugin_id}", source=f"plugin:{plugin_id}", color=color)


def normalize_root_log_level(level_name: str) -> str:
    """Normalize and validate a log level name for use with the root logger.

    Raises:
        ValueError: If the level name is not a supported log level.
    """
    normalized = str(level_name or "").strip().upper()
    if normalized == "WARN":
        normalized = "WARNING"
    if normalized not in _LOG_LEVEL_NAMES:
        raise ValueError(f"Unsupported log level: {level_name}")
    return normalized


def normalize_third_party_noise_policy(
    policy: str,
    *,
    strict: bool = False,
) -> ThirdPartyNoisePolicy:
    """Normalize a third-party noise policy string.

    Args:
        policy: The policy string to normalize.
        strict: If True, raise ValueError on unsupported policies.
            Otherwise fall back to 'debug'.

    Returns:
        A valid ThirdPartyNoisePolicy value.
    """
    normalized = str(policy or "debug").strip().lower()
    if normalized not in _THIRD_PARTY_NOISE_POLICIES:
        if strict:
            raise ValueError(f"Unsupported third-party noise policy: {policy}")
        normalized = "debug"
    return normalized  # type: ignore[return-value]


def _level_from_name(level_name: str, *, default: int) -> int:
    try:
        normalized = normalize_root_log_level(level_name)
    except ValueError:
        return default
    level = getattr(logging, normalized, default)
    return level if isinstance(level, int) else default
