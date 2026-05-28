"""File logging configuration and handler helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from typing import Any

DEFAULT_LOG_FILE = Path("logs") / "shinbot.log"
DEFAULT_ROTATION_WHEN = "midnight"
DEFAULT_BACKUP_COUNT = 14
DEFAULT_MAX_BYTES = 10 * 1024 * 1024
_ROTATION_ALIASES = {
    "daily": "midnight",
    "day": "midnight",
    "midnight": "midnight",
    "hourly": "H",
    "hour": "H",
    "h": "H",
}


@dataclass(frozen=True, slots=True)
class FileLogConfig:
    """Normalized file logging configuration."""

    enabled: bool = True
    path: Path = DEFAULT_LOG_FILE
    when: str = DEFAULT_ROTATION_WHEN
    interval: int = 1
    backup_count: int = DEFAULT_BACKUP_COUNT
    max_bytes: int = DEFAULT_MAX_BYTES
    utc: bool = False
    encoding: str = "utf-8"

    def resolved_path(self, data_dir: Path | str) -> Path:
        """Resolve the configured log path relative to the runtime data dir."""
        configured = Path(self.path)
        if configured.is_absolute():
            return configured
        return Path(data_dir) / configured


def parse_file_log_config(raw: Any) -> FileLogConfig:
    """Normalize a ``[logging.file]`` config table.

    Missing config enables persistent runtime logs with conservative size-based
    rotation. ``false`` can be used as a shorthand for disabling file logs.
    """
    if raw is False:
        return FileLogConfig(enabled=False)
    if raw is None:
        return FileLogConfig()
    if not isinstance(raw, dict):
        return FileLogConfig()

    enabled = _as_bool(raw.get("enabled"), default=True)
    path = _as_path(raw.get("path"), default=DEFAULT_LOG_FILE)
    when = _normalize_rotation_when(raw.get("when", DEFAULT_ROTATION_WHEN))
    interval = max(1, _as_int(raw.get("interval"), default=1))
    backup_count = max(0, _as_int(raw.get("backup_count"), default=DEFAULT_BACKUP_COUNT))
    max_bytes = max(0, _as_int(raw.get("max_bytes"), default=DEFAULT_MAX_BYTES))
    utc = _as_bool(raw.get("utc"), default=False)
    encoding = str(raw.get("encoding") or "utf-8")
    return FileLogConfig(
        enabled=enabled,
        path=path,
        when=when,
        interval=interval,
        backup_count=backup_count,
        max_bytes=max_bytes,
        utc=utc,
        encoding=encoding,
    )


def build_file_log_handler(
    config: FileLogConfig,
    *,
    data_dir: Path | str,
    level: int,
    record_filter: logging.Filter,
    formatter: logging.Formatter,
) -> logging.Handler | None:
    """Build a time-rotating file log handler from normalized config."""
    if not config.enabled:
        return None

    path = config.resolved_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    if config.max_bytes > 0:
        handler: logging.Handler = RotatingFileHandler(
            path,
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
            encoding=config.encoding,
        )
    else:
        handler = TimedRotatingFileHandler(
            path,
            when=config.when,
            interval=config.interval,
            backupCount=config.backup_count,
            encoding=config.encoding,
            utc=config.utc,
        )
    handler.setLevel(level)
    handler.addFilter(record_filter)
    handler.setFormatter(formatter)
    handler._shinbot_file_handler = True  # type: ignore[attr-defined]
    handler._shinbot_file_path = str(path)  # type: ignore[attr-defined]
    return handler


def _normalize_rotation_when(value: Any) -> str:
    normalized = str(value or DEFAULT_ROTATION_WHEN).strip()
    alias = _ROTATION_ALIASES.get(normalized.lower())
    return alias if alias is not None else normalized


def _as_path(value: Any, *, default: Path) -> Path:
    if value is None:
        return default
    text = str(value).strip()
    return Path(text) if text else default


def _as_int(value: Any, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default
