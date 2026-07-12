"""Unit coverage for versioned actor workflow completion wire contracts."""

from __future__ import annotations

import copy
import json
import math
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.external_actions import (
    ExternalActionIntent,
    ExternalActionKind,
)
from shinbot.agent.runtime.session_actor.workflow_completion import (
    EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION,
    WORKFLOW_COMPLETION_SCHEMA_VERSION,
    ActiveChatBootstrapCompletionResult,
    ActiveChatBootstrapDisposition,
    ActiveChatRoundCompletionResult,
    ActiveChatRoundOutcome,
    ActiveReplyCompletionResult,
    ExternalActionIntentBatch,
    ReviewCompletionResult,
    ReviewNextReviewOutcome,
    ReviewNextReviewOutcomeKind,
    WorkflowCompletionCodecError,
    decode_external_action_intent_batch,
    encode_external_action_intent_batch,
)


def _intent(
    ordinal: int,
    *,
    proposal_id: str | None = None,
    kind: ExternalActionKind = ExternalActionKind.SEND_REPLY,
    payload: dict[str, object] | None = None,
) -> ExternalActionIntent:
    return ExternalActionIntent(
        kind=kind,
        tool_call_id=proposal_id or f"proposal-{ordinal}",
        action_ordinal=ordinal,
        payload=payload or {"text": f"reply {ordinal}"},
    )


def _intent_batch_payload() -> dict[str, Any]:
    return {
        "schema_version": EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION,
        "intents": [
            {
                "proposal_id": "proposal-0",
                "action_ordinal": 0,
                "kind": "send_reply",
                "payload": {
                    "text": "你好",
                    "content": [{"type": "text", "text": "你好"}],
                },
            },
            {
                "proposal_id": "proposal-1",
                "action_ordinal": 1,
                "kind": "send_reaction",
                "payload": {
                    "message_log_id": 7,
                    "emoji_id": "128077",
                    "action": "add",
                },
            },
        ],
    }


def _active_reply_payload() -> dict[str, Any]:
    return {
        "schema_version": WORKFLOW_COMPLETION_SCHEMA_VERSION,
        "completion_type": "active_reply",
        "consumed_message_log_ids": [7, 8],
        "external_actions": _intent_batch_payload(),
    }


def _review_payload(*, enter_active_chat: bool) -> dict[str, Any]:
    return {
        "schema_version": WORKFLOW_COMPLETION_SCHEMA_VERSION,
        "completion_type": "review",
        "consumed_message_log_ids": [4, 7, 8],
        "external_actions": _intent_batch_payload(),
        "enter_active_chat": enter_active_chat,
        "next_review_outcome": (
            None
            if enter_active_chat
            else {
                "kind": "planned",
                "applied_delay_seconds": 120.0,
                "requested_delay_seconds": 120.0,
                "reason": "review complete",
                "fallback_reason": "",
            }
        ),
    }


def _active_chat_round_payload(*, outcome: str = "continue") -> dict[str, Any]:
    return {
        "schema_version": WORKFLOW_COMPLETION_SCHEMA_VERSION,
        "completion_type": "active_chat_round",
        "consumed_message_log_ids": [7, 8] if outcome != "retry" else [],
        "external_actions": _intent_batch_payload() if outcome != "retry" else {
            "schema_version": EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION,
            "intents": [],
        },
        "outcome": outcome,
        "interest_delta": 3.5,
        "reason": "round complete",
    }


def test_external_action_batch_json_round_trip_is_versioned_and_canonical() -> None:
    batch = ExternalActionIntentBatch(
        (
            _intent(
                0,
                payload={
                    "text": "你好",
                    "content": [{"type": "text", "text": "你好"}],
                },
            ),
            _intent(
                1,
                kind=ExternalActionKind.SEND_REACTION,
                payload={
                    "message_log_id": 7,
                    "emoji_id": "128077",
                    "action": "add",
                },
            ),
        )
    )

    encoded = batch.to_payload()
    encoded_json = batch.to_json()
    restored = ExternalActionIntentBatch.from_json(encoded_json)

    assert encoded["schema_version"] == EXTERNAL_ACTION_INTENT_BATCH_SCHEMA_VERSION
    assert json.loads(encoded_json) == encoded
    assert restored == batch
    assert restored.to_json() == encoded_json
    assert decode_external_action_intent_batch(
        encode_external_action_intent_batch(batch.intents)
    ) == batch.intents


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payload: payload.update(schema_version=2), "unsupported.*schema_version"),
        (lambda payload: payload.pop("intents"), "missing fields: intents"),
        (lambda payload: payload.update(extra=True), "unexpected fields: extra"),
        (
            lambda payload: payload["intents"][0].update(extra=True),
            "unexpected fields: extra",
        ),
        (
            lambda payload: payload["intents"][0].pop("proposal_id"),
            "missing fields: proposal_id",
        ),
    ],
)
def test_external_action_batch_rejects_schema_drift(
    mutation: object,
    message: str,
) -> None:
    payload = _intent_batch_payload()
    mutate = mutation
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(WorkflowCompletionCodecError, match=message):
        ExternalActionIntentBatch.from_payload(payload)


@pytest.mark.parametrize(
    ("ordinals", "message"),
    [
        ((1,), "expected 0, got 1"),
        ((0, 2), "expected 1, got 2"),
        ((1, 0), "expected 0, got 1"),
    ],
)
def test_external_action_batch_requires_operation_global_contiguous_ordinals(
    ordinals: tuple[int, ...],
    message: str,
) -> None:
    with pytest.raises(WorkflowCompletionCodecError, match=message):
        ExternalActionIntentBatch(tuple(_intent(value) for value in ordinals))


def test_external_action_batch_requires_unique_nonempty_stable_proposal_ids() -> None:
    with pytest.raises(WorkflowCompletionCodecError, match="duplicate.*proposal_id"):
        ExternalActionIntentBatch(
            (
                _intent(0, proposal_id="same-proposal"),
                _intent(1, proposal_id="same-proposal"),
            )
        )

    payload = _intent_batch_payload()
    payload["intents"][0]["proposal_id"] = " "
    with pytest.raises(WorkflowCompletionCodecError, match="proposal_id must not be empty"):
        ExternalActionIntentBatch.from_payload(payload)


@pytest.mark.parametrize(
    "reserved_field",
    [
        "operation_id",
        "effect_id",
        "ownership_generation",
        "instance_id",
        "target_session_id",
    ],
)
def test_external_action_batch_rejects_nested_runtime_reserved_fields(
    reserved_field: str,
) -> None:
    intent = _intent(
        0,
        payload={"text": "hello", "metadata": {reserved_field: "forged"}},
    )

    with pytest.raises(WorkflowCompletionCodecError, match="runtime-reserved field"):
        ExternalActionIntentBatch((intent,))

    payload = _intent_batch_payload()
    payload["intents"][0]["payload"]["metadata"] = {reserved_field: "forged"}
    with pytest.raises(WorkflowCompletionCodecError, match="runtime-reserved field"):
        ExternalActionIntentBatch.from_payload(payload)


@pytest.mark.parametrize("invalid", [math.inf, -math.inf, math.nan, object()])
def test_external_action_batch_rejects_non_json_payload_values(invalid: object) -> None:
    payload = _intent_batch_payload()
    payload["intents"][0]["payload"]["invalid"] = invalid

    with pytest.raises(WorkflowCompletionCodecError, match="JSON-compatible|finite"):
        ExternalActionIntentBatch.from_payload(payload)


def test_json_decoder_rejects_duplicate_fields() -> None:
    payload = (
        '{"schema_version":1,"schema_version":1,"completion_type":"active_reply",'
        '"consumed_message_log_ids":[],"external_actions":'
        '{"schema_version":1,"intents":[]}}'
    )

    with pytest.raises(WorkflowCompletionCodecError, match="duplicate JSON field"):
        ActiveReplyCompletionResult.from_json(payload)


def test_active_reply_completion_json_round_trip_preserves_consumption_and_intents() -> None:
    restored = ActiveReplyCompletionResult.from_payload(_active_reply_payload())

    assert restored.consumed_message_log_ids == (7, 8)
    assert [intent.action_ordinal for intent in restored.external_action_intents] == [0, 1]
    assert ActiveReplyCompletionResult.from_json(restored.to_json()) == restored
    assert restored.to_payload()["completion_type"] == "active_reply"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payload: payload.update(schema_version=3), "unsupported.*schema_version"),
        (lambda payload: payload.update(completion_type="review"), "completion_type"),
        (lambda payload: payload.pop("external_actions"), "missing fields"),
        (lambda payload: payload.update(operation_id="forged"), "unexpected fields"),
    ],
)
def test_active_reply_completion_rejects_schema_or_runtime_fields(
    mutation: object,
    message: str,
) -> None:
    payload = _active_reply_payload()
    mutate = mutation
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(WorkflowCompletionCodecError, match=message):
        ActiveReplyCompletionResult.from_payload(payload)


@pytest.mark.parametrize(
    "consumed_ids",
    [
        [0],
        [True],
        [1, 1],
        ["1"],
    ],
)
def test_completion_rejects_ambiguous_consumed_message_ids(
    consumed_ids: list[object],
) -> None:
    payload = _active_reply_payload()
    payload["consumed_message_log_ids"] = consumed_ids

    with pytest.raises(WorkflowCompletionCodecError, match="consumed message|positive integer"):
        ActiveReplyCompletionResult.from_payload(payload)


def test_review_completion_round_trips_each_exclusive_branch() -> None:
    active = ReviewCompletionResult.from_payload(_review_payload(enter_active_chat=True))
    idle = ReviewCompletionResult.from_payload(_review_payload(enter_active_chat=False))

    assert active.enter_active_chat is True
    assert active.next_review_outcome is None
    assert ReviewCompletionResult.from_json(active.to_json()) == active
    assert idle.enter_active_chat is False
    assert idle.next_review_outcome == ReviewNextReviewOutcome(
        kind=ReviewNextReviewOutcomeKind.PLANNED,
        applied_delay_seconds=120.0,
        requested_delay_seconds=120.0,
        reason="review complete",
    )
    assert ReviewCompletionResult.from_json(idle.to_json()) == idle
    assert "next_review_at" not in idle.to_json()


@pytest.mark.parametrize(
    ("enter_active_chat", "outcome"),
    [
        (True, ReviewNextReviewOutcome(ReviewNextReviewOutcomeKind.DEFAULTED, 30.0, "x")),
        (False, None),
    ],
)
def test_review_completion_requires_exactly_one_terminal_branch(
    enter_active_chat: bool,
    outcome: ReviewNextReviewOutcome | None,
) -> None:
    with pytest.raises(WorkflowCompletionCodecError, match="exactly one"):
        ReviewCompletionResult(
            enter_active_chat=enter_active_chat,
            next_review_outcome=outcome,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda outcome: outcome.update(kind="unknown"), "unsupported.*kind"),
        (lambda outcome: outcome.pop("reason"), "missing fields: reason"),
        (lambda outcome: outcome.update(next_review_at=100.0), "unexpected fields"),
        (
            lambda outcome: outcome.update(applied_delay_seconds=-1.0),
            "finite and non-negative",
        ),
        (
            lambda outcome: outcome.update(requested_delay_seconds=None),
            "planned.*requires requested",
        ),
    ],
)
def test_review_next_review_outcome_is_strict_and_relative(
    mutation: object,
    message: str,
) -> None:
    payload = _review_payload(enter_active_chat=False)
    outcome = payload["next_review_outcome"]
    assert isinstance(outcome, dict)
    mutate = mutation
    assert callable(mutate)
    mutate(outcome)

    with pytest.raises(WorkflowCompletionCodecError, match=message):
        ReviewCompletionResult.from_payload(payload)


def test_completion_decoders_do_not_mutate_input_payloads() -> None:
    active_payload = _active_reply_payload()
    review_payload = _review_payload(enter_active_chat=False)
    active_before = copy.deepcopy(active_payload)
    review_before = copy.deepcopy(review_payload)

    ActiveReplyCompletionResult.from_payload(active_payload)
    ReviewCompletionResult.from_payload(review_payload)

    assert active_payload == active_before
    assert review_payload == review_before


def test_active_chat_bootstrap_completion_is_strict_and_round_trips() -> None:
    result = ActiveChatBootstrapCompletionResult.from_payload(
        {
            "schema_version": WORKFLOW_COMPLETION_SCHEMA_VERSION,
            "completion_type": "active_chat_bootstrap",
            "disposition": "engaged",
            "reason": "recent messages are directed at the bot",
        }
    )

    assert result.disposition is ActiveChatBootstrapDisposition.ENGAGED
    assert result.to_payload()["completion_type"] == "active_chat_bootstrap"
    with pytest.raises(WorkflowCompletionCodecError, match="unexpected fields"):
        ActiveChatBootstrapCompletionResult.from_payload(
            {**result.to_payload(), "operation_id": "forged"}
        )


@pytest.mark.parametrize(
    ("outcome", "mutation", "message"),
    [
        ("continue", lambda payload: payload.update(interest_delta=101), "within"),
        ("retry", lambda payload: payload.update(consumed_message_log_ids=[7]), "retry"),
        ("retry", lambda payload: payload.update(external_actions=_intent_batch_payload()), "retry"),
        ("exit", lambda payload: payload.update(outcome="invalid"), "unsupported"),
    ],
)
def test_active_chat_round_completion_is_bounded_and_fail_closed(
    outcome: str,
    mutation: object,
    message: str,
) -> None:
    payload = _active_chat_round_payload(outcome=outcome)
    mutate = mutation
    assert callable(mutate)
    mutate(payload)

    with pytest.raises(WorkflowCompletionCodecError, match=message):
        ActiveChatRoundCompletionResult.from_payload(payload)


def test_active_chat_round_completion_round_trips() -> None:
    result = ActiveChatRoundCompletionResult.from_payload(
        _active_chat_round_payload()
    )

    assert result.outcome is ActiveChatRoundOutcome.CONTINUE
    assert result.consumed_message_log_ids == (7, 8)
    assert ActiveChatRoundCompletionResult.from_payload(result.to_payload()) == result
