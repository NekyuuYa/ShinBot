"""Health tracking for supervised Agent runtime services."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum


class RuntimeServiceStatus(StrEnum):
    """Lifecycle status for one expected Agent background service."""

    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPED = "stopped"


@dataclass(slots=True, frozen=True)
class RuntimeServiceHealthSnapshot:
    """Read-only health state exposed to diagnostics and management APIs."""

    service_name: str
    status: RuntimeServiceStatus
    started_at: float = 0.0
    last_scan_started_at: float = 0.0
    last_success_at: float = 0.0
    last_error_at: float = 0.0
    last_error_code: str = ""
    last_error_message: str = ""
    consecutive_failures: int = 0
    scan_count: int = 0
    success_count: int = 0


class RuntimeServiceHealth:
    """Mutable event-loop-local health tracker for a supervised service."""

    def __init__(self, service_name: str) -> None:
        self._service_name = service_name
        self._status = RuntimeServiceStatus.STOPPED
        self._started_at = 0.0
        self._last_scan_started_at = 0.0
        self._last_success_at = 0.0
        self._last_error_at = 0.0
        self._last_error_code = ""
        self._last_error_message = ""
        self._consecutive_failures = 0
        self._scan_count = 0
        self._success_count = 0

    def start(self, *, now: float | None = None) -> None:
        """Mark the expected service as starting."""

        checked_at = time.time() if now is None else now
        self._status = RuntimeServiceStatus.STARTING
        self._started_at = checked_at
        self._last_scan_started_at = 0.0
        self._last_success_at = 0.0
        self._last_error_at = 0.0
        self._last_error_code = ""
        self._last_error_message = ""
        self._consecutive_failures = 0
        self._scan_count = 0
        self._success_count = 0

    def scan_started(self, *, now: float | None = None) -> None:
        """Record the start of one service iteration."""

        self._last_scan_started_at = time.time() if now is None else now
        self._scan_count += 1

    def succeeded(self, *, now: float | None = None) -> None:
        """Record a successful iteration and recover degraded health."""

        self._status = RuntimeServiceStatus.RUNNING
        self._last_success_at = time.time() if now is None else now
        self._consecutive_failures = 0
        self._success_count += 1

    def failed(self, error: BaseException, *, now: float | None = None) -> None:
        """Record an iteration failure without declaring the service stopped."""

        self._status = RuntimeServiceStatus.DEGRADED
        self._last_error_at = time.time() if now is None else now
        self._last_error_code = type(error).__name__
        self._last_error_message = str(error)
        self._consecutive_failures += 1

    def stop(self) -> None:
        """Mark the service as intentionally stopped."""

        self._status = RuntimeServiceStatus.STOPPED

    def snapshot(self) -> RuntimeServiceHealthSnapshot:
        """Return the current immutable health snapshot."""

        return RuntimeServiceHealthSnapshot(
            service_name=self._service_name,
            status=self._status,
            started_at=self._started_at,
            last_scan_started_at=self._last_scan_started_at,
            last_success_at=self._last_success_at,
            last_error_at=self._last_error_at,
            last_error_code=self._last_error_code,
            last_error_message=self._last_error_message,
            consecutive_failures=self._consecutive_failures,
            scan_count=self._scan_count,
            success_count=self._success_count,
        )


def supervised_backoff_seconds(
    *,
    base_seconds: float,
    consecutive_failures: int,
    maximum_seconds: float = 60.0,
) -> float:
    """Return bounded exponential backoff for a supervised loop."""

    base = max(0.01, base_seconds)
    maximum = max(0.01, maximum_seconds)
    exponent = max(0, consecutive_failures - 1)
    try:
        candidate = base * (2.0**exponent)
    except OverflowError:
        return maximum
    return min(maximum, candidate)


__all__ = [
    "RuntimeServiceHealth",
    "RuntimeServiceHealthSnapshot",
    "RuntimeServiceStatus",
    "supervised_backoff_seconds",
]
