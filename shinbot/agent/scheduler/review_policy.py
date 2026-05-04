"""Review scheduling policy boundary."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from shinbot.agent.scheduler.models import (
    ActiveReplyThreshold,
    MentionSensitivity,
    ReviewPlan,
)


@dataclass(slots=True, frozen=True)
class ReviewPolicyConfig:
    """Defaults for the first review policy implementation."""

    default_review_after_seconds: float = 900.0
    default_reason: str = "default_idle_review_interval"
    mention_sensitivity: MentionSensitivity = MentionSensitivity.NORMAL
    mention_wake_count: int = 1
    mention_wake_window_seconds: float = 60.0


class ReviewPolicy(Protocol):
    """Decide next review timing, reason, and wake sensitivity."""

    def initial_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        """Return the initial review plan for one session."""

    def plan_after_review(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        """Return the next review plan after a review completes without active chat."""


class DefaultReviewPolicy:
    """Static review policy used until LLM/dynamic review planning is introduced."""

    def __init__(self, config: ReviewPolicyConfig | None = None) -> None:
        self._config = config or ReviewPolicyConfig()

    def initial_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        return self._build_plan(session_id=session_id, now=now)

    def plan_after_review(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        return self._build_plan(session_id=session_id, now=now)

    def _build_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now + self._config.default_review_after_seconds,
            reason=self._config.default_reason,
            mention_sensitivity=self._config.mention_sensitivity,
            active_reply_threshold=ActiveReplyThreshold(
                at_count=self._config.mention_wake_count,
                window_seconds=self._config.mention_wake_window_seconds,
            ),
            updated_at=now,
        )


__all__ = ["DefaultReviewPolicy", "ReviewPolicy", "ReviewPolicyConfig"]
