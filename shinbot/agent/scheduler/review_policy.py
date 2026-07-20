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

    def plan_after_active_reply(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        """Return a fresh idle plan after active reply consumed all pending input."""


class DefaultReviewPolicy:
    """Static review policy used until LLM/dynamic review planning is introduced."""

    def __init__(self, config: ReviewPolicyConfig | None = None) -> None:
        self._config = config or ReviewPolicyConfig()

    def initial_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        """Return the initial review plan for one session.
        Args:
            session_id: The session to create a review plan for.
            now: Current timestamp in seconds since epoch.

        Returns:
            A review plan with timing and sensitivity configuration.
        """
        return self._build_plan(session_id=session_id, now=now)

    def plan_after_review(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        """Return the next review plan after a review completes without active chat.
        Args:
            session_id: The session to plan the next review for.
            now: Current timestamp in seconds since epoch.
            previous_plan: The plan that was just reviewed, if any.

        Returns:
            A new review plan for the session.
        """
        return self._build_plan(session_id=session_id, now=now)

    def plan_after_active_reply(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        """Return the next idle plan after active reply drained the inbox.

        Args:
            session_id: The session whose active reply completed.
            now: Current timestamp in seconds since epoch.
            previous_plan: The superseded plan, if one existed.

        Returns:
            A fresh review plan measured from active-reply completion.
        """
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
