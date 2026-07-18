"""Unit coverage for fenced post-commit Actor wake identities."""

from __future__ import annotations

import pytest

from shinbot.core.dispatch.agent_identity import SessionKey
from shinbot.core.dispatch.fenced_wake import (
    FencedMailboxWakeDisposition,
    FencedMailboxWakeReceipt,
    FencedMailboxWakeRequest,
)


def _key() -> SessionKey:
    """Build one stable Actor session identity."""

    return SessionKey(profile_id="bot-a", session_id="bot-a:private:user-a")


def test_unfenced_wake_request_keeps_exact_ownership_generation() -> None:
    """Existing unfenced Actor rows remain representable without ambiguity."""

    request = FencedMailboxWakeRequest(_key(), ownership_generation=3)

    assert request.ownership_generation == 3
    assert request.admission_fence_id == ""
    assert request.admission_fence_generation == 0
    assert not request.has_admission_fence


def test_fenced_wake_request_keeps_complete_admission_identity() -> None:
    """A wake request carries both halves of a committed fence identity."""

    request = FencedMailboxWakeRequest(
        _key(),
        ownership_generation=4,
        admission_fence_id=" fence-a ",
        admission_fence_generation=2,
    )

    assert request.admission_fence_id == "fence-a"
    assert request.admission_fence_generation == 2
    assert request.has_admission_fence


@pytest.mark.parametrize(
    ("ownership_generation", "fence_id", "fence_generation"),
    [
        (0, "", 0),
        (-1, "", 0),
        (True, "", 0),
        (1, "fence-a", 0),
        (1, "", 1),
        (1, "fence-a", -1),
        (1, "fence-a", True),
    ],
)
def test_wake_request_rejects_ambiguous_or_invalid_identity(
    ownership_generation: int,
    fence_id: str,
    fence_generation: int,
) -> None:
    """A target never has to infer a missing generation or fence component."""

    with pytest.raises(ValueError):
        FencedMailboxWakeRequest(
            _key(),
            ownership_generation=ownership_generation,
            admission_fence_id=fence_id,
            admission_fence_generation=fence_generation,
        )


def test_wake_request_deduplication_includes_generation_and_fence() -> None:
    """Wake debt cannot merge two incarnations that share a SessionKey."""

    current = FencedMailboxWakeRequest(
        _key(),
        ownership_generation=2,
        admission_fence_id="fence-a",
        admission_fence_generation=1,
    )
    different_generation = FencedMailboxWakeRequest(
        _key(),
        ownership_generation=3,
        admission_fence_id="fence-a",
        admission_fence_generation=1,
    )
    different_fence = FencedMailboxWakeRequest(
        _key(),
        ownership_generation=2,
        admission_fence_id="fence-b",
        admission_fence_generation=2,
    )

    assert len({current, different_generation, different_fence}) == 3


def test_receipt_distinguishes_terminal_and_retryable_wake_outcomes() -> None:
    """A target can keep temporary unavailability distinct from stale ownership."""

    request = FencedMailboxWakeRequest(_key(), ownership_generation=1)
    accepted = FencedMailboxWakeReceipt(
        request=request,
        disposition=FencedMailboxWakeDisposition.ACCEPTED,
    )
    stale = FencedMailboxWakeReceipt(
        request=request,
        disposition=FencedMailboxWakeDisposition.STALE,
    )
    deferred = FencedMailboxWakeReceipt(
        request=request,
        disposition=FencedMailboxWakeDisposition.DEFERRED,
    )

    assert accepted.disposition is FencedMailboxWakeDisposition.ACCEPTED
    assert stale.disposition is FencedMailboxWakeDisposition.STALE
    assert deferred.disposition is FencedMailboxWakeDisposition.DEFERRED
