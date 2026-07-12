"""Unit coverage for actor-owned external action contracts."""

from __future__ import annotations

from dataclasses import replace

import pytest

from shinbot.agent.runtime.session_actor.effect_contracts import EffectLane
from shinbot.agent.runtime.session_actor.external_actions import (
    EXTERNAL_ACTION_COMPLETION_EVENT_KIND,
    ExternalActionIntent,
    ExternalActionKind,
    ExternalActionReceiptStatus,
    ExternalActionRequest,
    builtin_external_action_effect_contract,
    builtin_external_action_effect_contracts,
    materialize_external_action_effect,
    materialize_external_action_effects,
)
from shinbot.core.dispatch.agent_identity import SessionKey


def _intent(**changes: object) -> ExternalActionIntent:
    values: dict[str, object] = {
        "kind": ExternalActionKind.SEND_REPLY,
        "tool_call_id": "tool-call-1",
        "action_ordinal": 0,
        "payload": {
            "text": "hello",
            "quote_message_log_id": 42,
            "content": [{"type": "text", "text": "hello"}],
        },
    }
    values.update(changes)
    return ExternalActionIntent(**values)  # type: ignore[arg-type]


def _request(**changes: object) -> ExternalActionRequest:
    values: dict[str, object] = {
        "key": SessionKey("profile-a", "profile-a:group:room"),
        "ownership_generation": 3,
        "operation_id": "active-chat-round-7",
        "source_event_id": "active-chat-round-completed-7",
        "instance_id": "adapter-a",
        "target_session_id": "adapter-a:group:room",
        "intent": _intent(),
    }
    values.update(changes)
    return ExternalActionRequest(**values)  # type: ignore[arg-type]


def test_action_identity_is_stable_and_runtime_owned() -> None:
    first = _request()
    replayed = _request(intent=_intent(payload=dict(_intent().payload)))

    assert replayed.effect_id == first.effect_id
    assert replayed.idempotency_key == first.idempotency_key
    assert replayed.request_digest == first.request_digest
    assert replayed.to_effect_payload()["request_digest"] == first.request_digest
    assert "ownership_generation" not in first.to_effect_payload()
    assert first.idempotency_key.startswith("external-action-idempotency:")


@pytest.mark.parametrize(
    "changed",
    [
        {"operation_id": "active-chat-round-8"},
        {"intent": _intent(tool_call_id="tool-call-2")},
        {"intent": _intent(action_ordinal=1)},
        {"key": SessionKey("profile-b", "profile-a:group:room")},
    ],
)
def test_action_identity_changes_with_authoritative_provenance(
    changed: dict[str, object],
) -> None:
    assert _request(**changed).effect_id != _request().effect_id


def test_request_digest_excludes_actor_provenance_but_not_external_target() -> None:
    baseline = _request()
    next_generation = replace(baseline, ownership_generation=4)

    assert next_generation.request_digest == baseline.request_digest
    assert next_generation.effect_id == baseline.effect_id
    assert next_generation.idempotency_key == baseline.idempotency_key
    assert replace(baseline, source_event_id="another-completion").request_digest == (
        baseline.request_digest
    )
    assert replace(
        baseline,
        instance_id="adapter-b",
        target_session_id="adapter-b:group:room",
    ).request_digest != baseline.request_digest


def test_logical_identity_is_version_and_payload_independent_but_digest_is_exact() -> None:
    baseline = _request()
    changed_contract = replace(baseline, contract_version=2)
    changed_payload = replace(
        baseline,
        intent=_intent(payload={"text": "different"}),
    )

    assert changed_contract.effect_id == baseline.effect_id
    assert changed_payload.effect_id == baseline.effect_id
    assert changed_contract.idempotency_key == baseline.idempotency_key
    assert changed_payload.idempotency_key == baseline.idempotency_key
    assert changed_contract.request_digest != baseline.request_digest
    assert changed_payload.request_digest != baseline.request_digest


@pytest.mark.parametrize(
    "reserved_field",
    [
        "claim_id",
        "contract_version",
        "effect_id",
        "idempotency_key",
        "operation_id",
        "ownership_generation",
    ],
)
def test_model_payload_cannot_override_runtime_fences(reserved_field: str) -> None:
    with pytest.raises(ValueError, match="runtime-reserved"):
        _intent(payload={"text": "hello", reserved_field: "model-selected"})


def test_action_payload_is_deeply_immutable_and_canonical() -> None:
    intent = _intent()

    with pytest.raises(TypeError, match="immutable"):
        intent.payload["text"] = "changed"
    with pytest.raises(TypeError, match="immutable"):
        intent.payload["content"][0]["text"] = "changed"
    assert intent.canonical_payload_json == (
        '{"content":[{"text":"hello","type":"text"}],'
        '"quote_message_log_id":42,"text":"hello"}'
    )


@pytest.mark.parametrize(
    ("changes", "error_type", "match"),
    [
        ({"tool_call_id": ""}, ValueError, "must not be empty"),
        ({"tool_call_id": 1}, TypeError, "must be a string"),
        ({"action_ordinal": -1}, ValueError, "non-negative"),
        ({"action_ordinal": True}, TypeError, "non-negative"),
        ({"payload": {1: "bad"}}, TypeError, "keys must be strings"),
        ({"payload": {"delay": float("inf")}}, ValueError, "finite"),
        ({"payload": {"value": object()}}, TypeError, "JSON-compatible"),
    ],
)
def test_action_intent_rejects_ambiguous_values(
    changes: dict[str, object],
    error_type: type[Exception],
    match: str,
) -> None:
    with pytest.raises(error_type, match=match):
        _intent(**changes)


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"ownership_generation": 0}, "positive integer"),
        ({"ownership_generation": True}, "positive integer"),
        ({"contract_version": 0}, "positive integer"),
        ({"operation_id": ""}, "must not be empty"),
        ({"instance_id": 7}, "must be a string"),
    ],
)
def test_bound_action_request_requires_complete_runtime_provenance(
    changes: dict[str, object],
    match: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match=match):
        _request(**changes)


def test_terminal_receipts_are_never_automatically_retried() -> None:
    assert ExternalActionReceiptStatus.PREPARED.automatic_retry_allowed is True
    assert (
        ExternalActionReceiptStatus.REJECTED_BEFORE_DISPATCH.automatic_retry_allowed
        is True
    )
    assert ExternalActionReceiptStatus.SUCCEEDED.automatic_retry_allowed is False
    assert (
        ExternalActionReceiptStatus.ABANDONED_BEFORE_DISPATCH.automatic_retry_allowed
        is False
    )
    assert ExternalActionReceiptStatus.UNKNOWN.automatic_retry_allowed is False
    assert ExternalActionReceiptStatus.ABANDONED_BEFORE_DISPATCH.terminal is True
    assert ExternalActionReceiptStatus.UNKNOWN.terminal is True


def test_builtin_action_contracts_are_stable_and_independent() -> None:
    contracts = builtin_external_action_effect_contracts()

    assert {contract.effect_kind for contract in contracts} == {
        kind.value for kind in ExternalActionKind
    }
    assert len({contract.signature for contract in contracts}) == len(
        ExternalActionKind
    )
    for contract in contracts:
        assert contract.lane is EffectLane.DEFAULT
        assert contract.completion_event_kind == (
            EXTERNAL_ACTION_COMPLETION_EVENT_KIND
        )
        assert builtin_external_action_effect_contract(
            contract.effect_kind,
            version=contract.version,
        ) is contract


def test_materializer_binds_actor_provenance_without_payload_fence_leakage() -> None:
    request = _request()
    effect = materialize_external_action_effect(
        key=request.key,
        ownership_generation=request.ownership_generation,
        operation_id=request.operation_id,
        source_event_id=request.source_event_id,
        instance_id=request.instance_id,
        target_session_id=request.target_session_id,
        intent=request.intent,
    )
    contract = builtin_external_action_effect_contract(request.intent.kind)

    assert effect.effect_id == request.effect_id
    assert effect.idempotency_key == request.idempotency_key
    assert effect.operation_id == request.operation_id
    assert effect.kind == request.intent.kind.value
    assert effect.contract_version == request.contract_version
    assert effect.contract_signature == contract.signature
    assert effect.payload == request.to_effect_payload()
    assert "idempotency_key" not in effect.payload
    assert "ownership_generation" not in effect.payload
    assert "profile_id" not in effect.payload
    assert "session_id" not in effect.payload


def test_materializer_preserves_logical_slot_when_exact_request_conflicts() -> None:
    baseline = _request()
    changed = replace(
        baseline,
        intent=_intent(payload={"text": "different normalized reply"}),
    )
    baseline_effect = materialize_external_action_effect(
        key=baseline.key,
        ownership_generation=baseline.ownership_generation,
        operation_id=baseline.operation_id,
        source_event_id=baseline.source_event_id,
        instance_id=baseline.instance_id,
        target_session_id=baseline.target_session_id,
        intent=baseline.intent,
    )
    changed_effect = materialize_external_action_effect(
        key=changed.key,
        ownership_generation=changed.ownership_generation,
        operation_id=changed.operation_id,
        source_event_id=changed.source_event_id,
        instance_id=changed.instance_id,
        target_session_id=changed.target_session_id,
        intent=changed.intent,
    )

    assert changed_effect.effect_id == baseline_effect.effect_id
    assert changed_effect.idempotency_key == baseline_effect.idempotency_key
    assert changed_effect.payload["request_digest"] != (
        baseline_effect.payload["request_digest"]
    )
    assert changed_effect.payload != baseline_effect.payload


def test_materializer_preserves_workflow_intent_order() -> None:
    request = _request()
    intents = (
        _intent(action_ordinal=0),
        _intent(
            kind=ExternalActionKind.SEND_POKE,
            tool_call_id="tool-call-2",
            action_ordinal=1,
            payload={"user_id": "alice"},
        ),
        _intent(
            kind=ExternalActionKind.SEND_REACTION,
            tool_call_id="tool-call-3",
            action_ordinal=2,
            payload={"message_id": "message-1", "emoji_id": "1"},
        ),
    )

    effects = materialize_external_action_effects(
        key=request.key,
        ownership_generation=request.ownership_generation,
        operation_id=request.operation_id,
        source_event_id=request.source_event_id,
        instance_id=request.instance_id,
        target_session_id=request.target_session_id,
        intents=intents,
    )

    assert [effect.kind for effect in effects] == [
        ExternalActionKind.SEND_REPLY.value,
        ExternalActionKind.SEND_POKE.value,
        ExternalActionKind.SEND_REACTION.value,
    ]
    assert len({effect.effect_id for effect in effects}) == len(intents)


@pytest.mark.parametrize(
    ("kind", "version"),
    [
        ("not-an-action", 1),
        (ExternalActionKind.SEND_REPLY, 99),
    ],
)
def test_unknown_action_contract_is_rejected(
    kind: ExternalActionKind | str,
    version: int,
) -> None:
    with pytest.raises(KeyError, match="unknown external action"):
        builtin_external_action_effect_contract(kind, version=version)
