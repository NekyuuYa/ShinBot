"""Audit logging — tracks command execution with timing and permission details."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shinbot.utils.logger import format_log_event

if TYPE_CHECKING:
    from shinbot.persistence.repos import AuditRepository

logger = logging.getLogger(__name__)

# Audit logger is separate to allow it to go to a different sink if configured
audit_logger = logging.getLogger("shinbot.audit")


def _metadata_key_count(metadata: dict[str, Any]) -> int | None:
    if not metadata:
        return None
    return len(metadata)


@dataclass
class AuditLog:
    """A single audit log entry for command execution."""

    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    entry_type: str = "command"
    command_name: str = ""
    plugin_id: str = ""
    user_id: str = ""
    session_id: str = ""
    instance_id: str = ""
    permission_required: str = ""
    permission_granted: bool = False
    execution_time_ms: float = 0.0
    success: bool = False
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Convert to JSON for logging/persistence."""
        return json.dumps(asdict(self), ensure_ascii=False)


class AuditLogger:
    """Centralized audit logging for command execution."""

    _MAX_LOG_AGE_DAYS: int = 30
    _MAX_FILE_SIZE_BYTES: int = 50 * 1024 * 1024  # 50 MB

    def __init__(
        self,
        data_dir: Path | str | None = None,
        *,
        audit_repo: AuditRepository | None = None,
    ) -> None:
        """Initialize audit logger, optionally with file persistence.

        Args:
            data_dir: Optional directory to persist audit logs to.
        """
        self._audit_repo = audit_repo
        self._data_dir: Path | None = None
        self._last_cleanup_date: str | None = None
        if data_dir:
            audit_path = Path(data_dir) / "audit"
            audit_path.mkdir(parents=True, exist_ok=True)
            self._data_dir = audit_path

    def log_command(
        self,
        command_name: str,
        plugin_id: str,
        user_id: str,
        session_id: str,
        instance_id: str,
        *,
        permission_required: str = "",
        permission_granted: bool = False,
        execution_time_ms: float = 0.0,
        success: bool = False,
        error: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> AuditLog:
        """Log a command execution.

        Args:
            command_name: Name of the command executed.
            plugin_id: ID of the plugin that owns the command.
            user_id: User who executed the command.
            session_id: Session in which command was executed.
            instance_id: Bot instance ID.
            permission_required: Permission required for the command (if any).
            permission_granted: Whether the user had the required permission.
            execution_time_ms: Total execution time in milliseconds.
            success: Whether the command executed successfully.
            error: Error message if execution failed.
            metadata: Optional additional metadata.

        Returns:
            The AuditLog entry that was created.
        """
        entry = AuditLog(
            entry_type="command",
            command_name=command_name,
            plugin_id=plugin_id,
            user_id=user_id,
            session_id=session_id,
            instance_id=instance_id,
            permission_required=permission_required,
            permission_granted=permission_granted,
            execution_time_ms=execution_time_ms,
            success=success,
            error=error,
            metadata=metadata or {},
        )

        # Keep runtime log output compact; full payload is persisted separately.
        audit_logger.info(
            format_log_event(
                "audit.command",
                command=entry.command_name,
                plugin=entry.plugin_id,
                user=entry.user_id,
                session=entry.session_id,
                instance=entry.instance_id,
                permission=entry.permission_required,
                granted=entry.permission_granted if entry.permission_required else None,
                ok=entry.success,
                ms=round(entry.execution_time_ms, 2),
                error=entry.error,
                metadata_keys=_metadata_key_count(entry.metadata),
            )
        )
        if self._audit_repo:
            self._audit_repo.insert(asdict(entry))

        # Optionally persist to disk
        if self._data_dir:
            self._persist_to_disk(entry)

        return entry

    def log_message(
        self,
        *,
        event_type: str,
        plugin_id: str,
        user_id: str,
        session_id: str,
        instance_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> AuditLog:
        """Log an inbound message event with modality statistics."""
        entry = AuditLog(
            entry_type="message",
            command_name=event_type,
            plugin_id=plugin_id,
            user_id=user_id,
            session_id=session_id,
            instance_id=instance_id,
            metadata=metadata or {},
        )

        audit_logger.info(
            format_log_event(
                "audit.message",
                event=event_type,
                plugin=plugin_id,
                user=user_id,
                session=session_id,
                instance=instance_id,
                metadata_keys=_metadata_key_count(entry.metadata),
            )
        )
        if self._audit_repo:
            self._audit_repo.insert(asdict(entry))
        if self._data_dir:
            self._persist_to_disk(entry)

        return entry

    def _persist_to_disk(self, entry: AuditLog) -> None:
        """Persist audit entry to disk (daily rotating files)."""
        if not self._data_dir:
            return

        # Use daily log files: audit_YYYY-MM-DD.jsonl
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        log_file = self._data_dir / f"audit_{today}.jsonl"

        # Guard against unbounded file growth
        try:
            if log_file.exists() and log_file.stat().st_size >= self._MAX_FILE_SIZE_BYTES:
                logger.warning(
                    "Audit log %s exceeds %d bytes; skipping write",
                    log_file,
                    self._MAX_FILE_SIZE_BYTES,
                )
                return
        except Exception:
            logger.exception("Failed to check audit log file size for %s", log_file)
            return

        try:
            with log_file.open("a", encoding="utf-8") as f:
                f.write(entry.to_json() + "\n")
        except Exception:
            logger.exception("Failed to persist audit log to %s", log_file)

        # Run cleanup once per day on the first write of a new day
        if self._last_cleanup_date != today:
            self._last_cleanup_date = today
            self._cleanup_old_logs()

    def _cleanup_old_logs(self) -> None:
        """Remove audit log files older than ``_MAX_LOG_AGE_DAYS``.

        Age is determined from the date embedded in the filename
        (``audit_YYYY-MM-DD.jsonl``), not from filesystem mtime.
        Failures are logged but never propagate to the caller.
        """
        if not self._data_dir:
            return

        try:
            cutoff = datetime.now(UTC).date() - timedelta(days=self._MAX_LOG_AGE_DAYS)
            removed = 0
            for path in self._data_dir.glob("audit_*.jsonl"):
                stem = path.stem  # e.g. "audit_2026-06-28"
                date_part = stem.removeprefix("audit_")
                try:
                    file_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                except ValueError:
                    continue
                if file_date < cutoff:
                    path.unlink()
                    removed += 1
            if removed:
                logger.info(
                    "Cleaned up %d audit log file(s) older than %d days",
                    removed,
                    self._MAX_LOG_AGE_DAYS,
                )
        except Exception:
            logger.exception("Failed to clean up old audit log files")
