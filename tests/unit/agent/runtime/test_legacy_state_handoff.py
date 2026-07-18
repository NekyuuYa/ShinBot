"""Unit coverage for the pure idle legacy-to-Actor target preparer."""

from __future__ import annotations

import hashlib

import pytest

from shinbot.agent.runtime.session_actor.legacy_state_handoff import (
    ActorV2LegacyIdleStatePreparationBlocked,
    ActorV2LegacyIdleStateTargetPreparer,
)
from shinbot.core.dispatch.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffManifest,
    ActorV2LegacyStateHandoffScope,
)
from shinbot.core.dispatch.agent_delivery import AgentRouteDelivery
from shinbot.core.dispatch.agent_identity import SessionKey


def _digest(value: str) -> str:
    """Return one stable SHA-256 digest for manifest boundary fixtures."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _delivery(key: SessionKey) -> AgentRouteDelivery:
    """Build one verified route delivery for the only unread fixture message."""

    return AgentRouteDelivery(
        session_key=key,
        bot_id="bot-a",
        bot_binding_id="binding-a",
        base_session_id="legacy-session-a",
        bot_session_id="bot-a:group:room",
        message_log_id=41,
        sender_id="user-a",
        instance_id="instance-a",
        platform="test",
        self_id="bot-self",
        is_private=False,
        is_mentioned=True,
        is_mention_to_other=False,
        is_reply_to_bot=False,
        is_poke_to_bot=False,
        is_poke_to_other=False,
        already_handled=False,
        is_stopped=False,
        trace_id="trace-a",
        observed_at=100.0,
        route_rule_id="builtin.agent_entry_fallback",
    )


def _manifest(*, route_status: str = "verified", scheduler_state: str = "idle") -> ActorV2LegacyStateHandoffManifest:
    """Build one complete v1 source manifest for pure materializer tests."""

    key = SessionKey("bot-a", "bot-a:group:room")
    delivery = _delivery(key)
    route_evidence: dict[str, object] = {
        "message_log_id": 41,
        "status": route_status,
    }
    if route_status == "verified":
        route_evidence["mailbox_payload"] = delivery.to_mailbox_payload()
    elif route_status == "ambiguous":
        route_evidence["event_ids"] = [delivery.event_id, "message-received:other"]
    source_payload: dict[str, object] = {
        "schema_version": 1,
        "scheduler_state": {
            "state": scheduler_state,
            "next_review_at": 130.0,
            "review_reason": "deferred_review",
            "mention_sensitivity": "high",
            "active_reply_threshold": {"at_count": 2, "window_seconds": 45.0},
            "active_chat_state": {},
            "state_resume": {},
            "updated_at": 100.0,
        },
        "unread_messages": [
            {
                "id": 1,
                "message_log_id": 41,
                "response_profile": "normal",
                "review_consumed": False,
                "chat_consumed": False,
            }
        ],
        "route_deliveries": [route_evidence],
        "unread_ranges": [],
        "high_priority_events": [],
        "recent_mentions": [],
        "review_summaries": [],
        "summaries": [],
    }
    return ActorV2LegacyStateHandoffManifest.create(
        manifest_id="manifest-a",
        barrier_id="barrier-a",
        core_ingress_drain_request_id="core-drain-a",
        key=key,
        scope=ActorV2LegacyStateHandoffScope(
            legacy_session_id="legacy-session-a",
            members=(key,),
        ),
        legacy_session_id="legacy-session-a",
        source_generation=5,
        migration_generation=6,
        source_payload=source_payload,
        core_ingress_digest=_digest("core"),
        legacy_quiescence_digest=_digest("legacy"),
        captured_at=101.0,
    )


def test_idle_target_preparer_builds_ownership_unbound_review_and_ledger_seeds() -> None:
    """A fully evidenced idle source can become a later atomic-finalizer input."""

    manifest = _manifest()

    prepared = ActorV2LegacyIdleStateTargetPreparer().materialize(manifest)

    assert prepared["kind"] == "actor_v2_legacy_idle_target_preparation"
    assert prepared["review_plan_seed"] == {
        "next_review_at": 130.0,
        "reason": "deferred_review",
        "mention_sensitivity": "high",
        "active_reply_threshold": {"at_count": 2, "window_seconds": 45.0},
        "updated_at": 100.0,
    }
    seed = prepared["ledger_seeds"][0]
    assert seed["legacy_review_consumed"] is False
    assert seed["legacy_chat_consumed"] is False
    assert seed["append"]["message_log_id"] == 41
    assert "ownership_generation" not in seed["append"]


@pytest.mark.parametrize(
    ("route_status", "expected_blocker"),
    [
        ("missing", "legacy_route_delivery_evidence_missing"),
        ("ambiguous", "legacy_route_delivery_evidence_ambiguous"),
    ],
)
def test_idle_target_preparer_rejects_unproven_route_delivery(
    route_status: str,
    expected_blocker: str,
) -> None:
    """No target seed can synthesize an event identity absent from the manifest."""

    with pytest.raises(ActorV2LegacyIdleStatePreparationBlocked) as blocked:
        ActorV2LegacyIdleStateTargetPreparer().materialize(
            _manifest(route_status=route_status)
        )

    assert blocked.value.blockers == (expected_blocker,)


def test_idle_target_preparer_rejects_active_legacy_scheduler_state() -> None:
    """Active workflow semantics require a later dedicated materializer."""

    with pytest.raises(ActorV2LegacyIdleStatePreparationBlocked) as blocked:
        ActorV2LegacyIdleStateTargetPreparer().materialize(
            _manifest(scheduler_state="active_chat")
        )

    assert blocked.value.blockers == ("legacy_scheduler_state_not_idle",)
