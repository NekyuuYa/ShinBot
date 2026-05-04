from __future__ import annotations

from shinbot.agent.scheduler import (
    DefaultReviewPolicy,
    MentionSensitivity,
    ReviewPolicyConfig,
)


def test_default_review_policy_builds_initial_plan() -> None:
    policy = DefaultReviewPolicy(
        ReviewPolicyConfig(
            default_review_after_seconds=120.0,
            default_reason="busy_until_next_check",
            mention_sensitivity=MentionSensitivity.LOW,
            mention_wake_count=2,
            mention_wake_window_seconds=90.0,
        )
    )

    plan = policy.initial_plan(session_id="bot:group:room", now=10.0)

    assert plan.session_id == "bot:group:room"
    assert plan.next_review_at == 130.0
    assert plan.reason == "busy_until_next_check"
    assert plan.mention_sensitivity == MentionSensitivity.LOW
    assert plan.active_reply_threshold.at_count == 2
    assert plan.active_reply_threshold.window_seconds == 90.0
    assert plan.updated_at == 10.0
