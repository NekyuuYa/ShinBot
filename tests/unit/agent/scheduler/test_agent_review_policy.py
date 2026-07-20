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


def test_default_review_policy_builds_plan_after_review() -> None:
    policy = DefaultReviewPolicy(
        ReviewPolicyConfig(
            default_review_after_seconds=300.0,
            default_reason="after_review_wait",
        )
    )

    plan = policy.plan_after_review(session_id="bot:group:room", now=20.0)

    assert plan.session_id == "bot:group:room"
    assert plan.next_review_at == 320.0
    assert plan.reason == "after_review_wait"
    assert plan.updated_at == 20.0


def test_default_review_policy_builds_plan_after_active_reply() -> None:
    policy = DefaultReviewPolicy(
        ReviewPolicyConfig(
            default_review_after_seconds=180.0,
            default_reason="after_active_reply_wait",
        )
    )

    plan = policy.plan_after_active_reply(
        session_id="bot:group:room",
        now=30.0,
    )

    assert plan.session_id == "bot:group:room"
    assert plan.next_review_at == 210.0
    assert plan.reason == "after_active_reply_wait"
    assert plan.updated_at == 30.0
