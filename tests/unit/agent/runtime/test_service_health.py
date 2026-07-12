from __future__ import annotations

import pytest

from shinbot.agent.runtime.service_health import (
    RuntimeServiceHealth,
    RuntimeServiceStatus,
    supervised_backoff_seconds,
)


def test_runtime_service_health_tracks_failure_and_recovery() -> None:
    health = RuntimeServiceHealth("review_due_timer")

    assert health.snapshot().status == RuntimeServiceStatus.STOPPED

    health.start(now=10.0)
    health.scan_started(now=11.0)
    health.failed(ValueError("temporary failure"), now=12.0)

    degraded = health.snapshot()
    assert degraded.status == RuntimeServiceStatus.DEGRADED
    assert degraded.started_at == 10.0
    assert degraded.last_scan_started_at == 11.0
    assert degraded.last_error_at == 12.0
    assert degraded.last_error_code == "ValueError"
    assert degraded.last_error_message == "temporary failure"
    assert degraded.consecutive_failures == 1
    assert degraded.scan_count == 1
    assert degraded.success_count == 0

    health.scan_started(now=13.0)
    health.succeeded(now=14.0)

    recovered = health.snapshot()
    assert recovered.status == RuntimeServiceStatus.RUNNING
    assert recovered.last_success_at == 14.0
    assert recovered.consecutive_failures == 0
    assert recovered.scan_count == 2
    assert recovered.success_count == 1
    # Recovery clears the active failure streak, not the last diagnostic.
    assert recovered.last_error_at == 12.0
    assert recovered.last_error_code == "ValueError"
    assert recovered.last_error_message == "temporary failure"

    health.stop()

    assert health.snapshot().status == RuntimeServiceStatus.STOPPED


def test_runtime_service_health_start_resets_previous_run() -> None:
    health = RuntimeServiceHealth("active_chat_timer:session")
    health.start(now=1.0)
    health.scan_started(now=2.0)
    health.failed(RuntimeError("old run"), now=3.0)

    health.start(now=10.0)

    snapshot = health.snapshot()
    assert snapshot.status == RuntimeServiceStatus.STARTING
    assert snapshot.started_at == 10.0
    assert snapshot.last_scan_started_at == 0.0
    assert snapshot.last_error_at == 0.0
    assert snapshot.last_error_code == ""
    assert snapshot.consecutive_failures == 0
    assert snapshot.scan_count == 0


@pytest.mark.parametrize(
    ("consecutive_failures", "expected"),
    [
        (0, 0.5),
        (1, 0.5),
        (2, 1.0),
        (3, 2.0),
        (4, 2.0),
        (10_000, 2.0),
    ],
)
def test_supervised_backoff_is_exponential_and_bounded(
    consecutive_failures: int,
    expected: float,
) -> None:
    assert supervised_backoff_seconds(
        base_seconds=0.5,
        consecutive_failures=consecutive_failures,
        maximum_seconds=2.0,
    ) == expected


def test_supervised_backoff_normalizes_invalid_bounds() -> None:
    assert supervised_backoff_seconds(
        base_seconds=0.0,
        consecutive_failures=1,
        maximum_seconds=0.0,
    ) == 0.01


def test_supervised_backoff_honors_maximum_below_base() -> None:
    assert supervised_backoff_seconds(
        base_seconds=120.0,
        consecutive_failures=1,
        maximum_seconds=60.0,
    ) == 60.0
