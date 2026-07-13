from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from typing import Any

import pytest

from shinbot.agent.runtime.session_actor.recovery import (
    MAX_RECOVERY_DECISION_REASON_CODES,
    MAX_RECOVERY_DECISION_TARGETS,
    MAX_RECOVERY_GRAPH_EDGES,
    MAX_RECOVERY_GRAPH_NODES,
    MAX_RECOVERY_INVARIANTS,
    MAX_RECOVERY_JSON_NODES,
    MAX_RECOVERY_TEXT_BYTES,
    RECOVERY_DELIVERY_EVENT_KIND,
    RECOVERY_DELIVERY_EVENT_SOURCE,
    RecoveryAggregateFence,
    RecoveryCertificate,
    RecoveryContractDecodeError,
    RecoveryDecision,
    RecoveryDecisionKind,
    RecoveryDeliveryEnvelopeIdentity,
    RecoveryDeliveryPayload,
    RecoveryGraphEdge,
    RecoveryGraphNode,
    RecoveryInvariant,
    RecoveryInvariantSeverity,
    RecoverySubject,
    UnsupportedRecoveryCertificateVersion,
    UnsupportedRecoveryDeliveryVersion,
    build_recovery_certificate,
    canonical_recovery_json,
    decode_recovery_certificate,
    decode_recovery_delivery_payload,
    recovery_delivery_event_id,
)


def _subject(*, profile_id: str = "profile-a") -> RecoverySubject:
    return RecoverySubject(
        profile_id=profile_id,
        session_id="bot:group:room",
        ownership_generation=3,
    )


def _fence(*, event_sequence: int = 11) -> RecoveryAggregateFence:
    return RecoveryAggregateFence(
        state="review",
        state_revision=7,
        event_sequence=event_sequence,
        activity_generation=5,
        active_epoch=2,
        current_plan_id="plan-4",
        review_plan_revision=4,
    )


def _operation_node(*, status: str = "running") -> RecoveryGraphNode:
    return RecoveryGraphNode(
        identity="operation:review-1",
        kind="operation",
        authority="agent_session_operations",
        status=status,
        facts={
            "operation_id": "review-1",
            "input_fence": {"ledger_sequence": 9, "watermark": 42},
        },
    )


def _effect_node() -> RecoveryGraphNode:
    return RecoveryGraphNode(
        identity="effect:review-1",
        kind="effect",
        authority="agent_effect_outbox",
        status="failed",
        facts={
            "contract_version": 2,
            "effect_id": "effect-1",
            "payload_digest": "a" * 64,
        },
    )


def _edge() -> RecoveryGraphEdge:
    return RecoveryGraphEdge(
        identity="edge:operation-effect",
        source="operation:review-1",
        target="effect:review-1",
        relation="produced",
    )


def _invariant() -> RecoveryInvariant:
    return RecoveryInvariant(
        identity="invariant:missing-outcome",
        code="terminal_effect_without_live_outcome",
        severity=RecoveryInvariantSeverity.BLOCKING,
        authority="recovery_policy:v1",
        node_identity="effect:review-1",
        details={"expected_outcomes": ["completed", "failed"]},
    )


def _decision() -> RecoveryDecision:
    return RecoveryDecision(
        kind=RecoveryDecisionKind.RECORD_BLOCKER,
        reason_codes=(
            "terminal_effect_without_live_outcome",
            "review_operation_orphaned",
        ),
        target_node_identities=("effect:review-1", "operation:review-1"),
        details={"automatic_action": "none"},
    )


def _certificate(
    *,
    subject: RecoverySubject | None = None,
    fence: RecoveryAggregateFence | None = None,
    nodes: tuple[RecoveryGraphNode, ...] | None = None,
    policy_version: int = 1,
) -> RecoveryCertificate:
    return build_recovery_certificate(
        subject=subject or _subject(),
        aggregate_fence=fence or _fence(),
        nodes=nodes or (_operation_node(), _effect_node()),
        edges=(_edge(),),
        invariants=(_invariant(),),
        decision=_decision(),
        policy_version=policy_version,
    )


def test_certificate_normalizes_set_like_graph_order_deterministically() -> None:
    forward = _certificate()
    reverse = build_recovery_certificate(
        subject=_subject(),
        aggregate_fence=_fence(),
        nodes=(_effect_node(), _operation_node()),
        edges=(_edge(),),
        invariants=(_invariant(),),
        decision=RecoveryDecision(
            kind=RecoveryDecisionKind.RECORD_BLOCKER,
            reason_codes=(
                "review_operation_orphaned",
                "terminal_effect_without_live_outcome",
            ),
            target_node_identities=("operation:review-1", "effect:review-1"),
            details={"automatic_action": "none"},
        ),
    )

    assert forward.to_record() == reverse.to_record()
    assert forward.work_graph_digest == reverse.work_graph_digest
    assert forward.certificate_digest == reverse.certificate_digest
    assert [node.identity for node in forward.nodes] == [
        "effect:review-1",
        "operation:review-1",
    ]
    with pytest.raises(TypeError, match="does not support item assignment"):
        forward.nodes[0].facts["changed"] = True


def test_certificate_nested_authority_cannot_be_mutated_through_base_classes() -> None:
    certificate = _certificate()
    original_record = certificate.to_record()
    original_digest = certificate.certificate_digest
    facts = certificate.nodes[1].facts
    nested = certificate.invariants[0].details["expected_outcomes"]

    assert not isinstance(facts, dict)
    assert isinstance(nested, tuple)
    with pytest.raises(TypeError):
        dict.__setitem__(facts, "injected", True)
    with pytest.raises(TypeError):
        list.append(nested, "injected")

    assert certificate.to_record() == original_record
    assert certificate.certificate_digest == original_digest


def test_event_sequence_changes_certificate_but_not_work_graph_or_case() -> None:
    original = _certificate(fence=_fence(event_sequence=11))
    redelivery = _certificate(fence=_fence(event_sequence=12))

    assert original.work_graph_digest == redelivery.work_graph_digest
    assert original.case_identity.case_id == redelivery.case_identity.case_id
    assert original.certificate_digest != redelivery.certificate_digest


def test_semantic_graph_changes_all_derived_identity() -> None:
    running = _certificate()
    completed = _certificate(
        nodes=(_operation_node(status="completed"), _effect_node()),
    )

    assert running.work_graph_digest != completed.work_graph_digest
    assert running.case_identity.case_id != completed.case_identity.case_id
    assert running.certificate_digest != completed.certificate_digest


def test_subject_and_policy_are_outside_work_graph_but_fence_case_identity() -> None:
    original = _certificate()
    other_subject = _certificate(subject=_subject(profile_id="profile-b"))
    other_policy = _certificate(policy_version=2)

    assert original.work_graph_digest == other_subject.work_graph_digest
    assert original.work_graph_digest == other_policy.work_graph_digest
    assert original.case_identity.case_id != other_subject.case_identity.case_id
    assert original.case_identity.case_id != other_policy.case_identity.case_id
    assert original.certificate_digest != other_subject.certificate_digest
    assert original.certificate_digest != other_policy.certificate_digest


def test_delivery_cycle_changes_event_id_without_changing_case() -> None:
    certificate = _certificate()

    first = recovery_delivery_event_id(
        certificate.case_identity,
        delivery_cycle=0,
    )
    retry = recovery_delivery_event_id(
        certificate.case_identity,
        delivery_cycle=1,
    )

    assert first.endswith(":0")
    assert retry.endswith(":1")
    assert first != retry
    assert certificate.case_identity.case_id.rsplit(":", maxsplit=1)[-1] in first
    with pytest.raises(ValueError, match="non-negative integer"):
        recovery_delivery_event_id(certificate.case_identity, delivery_cycle=-1)


def test_v1_recovery_identity_golden_vectors_remain_stable() -> None:
    certificate = _certificate()

    assert certificate.work_graph_digest == (
        "b69b9c19bd27a86daddde8677953ff8639cd667a6621c7ba639fd6c53f7d865d"
    )
    assert certificate.case_identity.certificate_version == 1
    assert certificate.case_identity.case_id == (
        "recovery-case:v1:"
        "3c5fc1dd21e79fc616cafe0c6e457912a08c926cedc3f3bf9fe952a88ce0feaa"
    )
    assert certificate.certificate_digest == (
        "ed18101366b9d70867b625e559e748930872f124f7dfa05f9e91680e425b1027"
    )
    assert recovery_delivery_event_id(
        certificate.case_identity,
        delivery_cycle=0,
    ) == (
        "recovery-requested:v1:"
        "3c5fc1dd21e79fc616cafe0c6e457912a08c926cedc3f3bf9fe952a88ce0feaa:0"
    )


def test_certificate_decoder_round_trips_exact_v1_authority() -> None:
    certificate = _certificate()

    decoded = RecoveryCertificate.from_record(certificate.to_record())

    assert decoded.to_record() == certificate.to_record()
    assert decoded.certificate_digest == certificate.certificate_digest


@pytest.mark.parametrize("field_name", ["work_graph_digest", "certificate_digest"])
def test_certificate_decoder_rejects_tampered_digests(field_name: str) -> None:
    record = deepcopy(_certificate().to_record())
    record[field_name] = "0" * 64

    with pytest.raises(RecoveryContractDecodeError, match=field_name):
        decode_recovery_certificate(record)


def test_certificate_decoder_rejects_tampered_case_and_noncanonical_graph() -> None:
    case_record = deepcopy(_certificate().to_record())
    case_record["case_id"] = f"recovery-case:v1:{'0' * 64}"
    with pytest.raises(RecoveryContractDecodeError, match="case_id"):
        decode_recovery_certificate(case_record)

    reordered = deepcopy(_certificate().to_record())
    reordered["nodes"].reverse()
    with pytest.raises(RecoveryContractDecodeError, match="not canonical"):
        decode_recovery_certificate(reordered)


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("subject", "profile_id"), "profile-b"),
        (("aggregate_fence", "event_sequence"), 12),
        (("aggregate_fence", "state"), "active_reply"),
        (("nodes", 0, "status"), "completed"),
        (("edges", 0, "relation"), "consumed"),
        (("invariants", 0, "code"), "different_invariant"),
        (("decision", "details", "automatic_action"), "retry"),
        (("policy_version",), 2),
    ],
)
def test_certificate_decoder_rejects_each_authority_section_tamper(
    path: tuple[str | int, ...],
    replacement: object,
) -> None:
    record = deepcopy(_certificate().to_record())
    cursor: Any = record
    for part in path[:-1]:
        cursor = cursor[part]
    cursor[path[-1]] = replacement

    with pytest.raises(RecoveryContractDecodeError):
        decode_recovery_certificate(record)


def test_certificate_decoder_rejects_unknown_missing_and_wrong_typed_fields() -> None:
    unknown = deepcopy(_certificate().to_record())
    unknown["unexpected"] = True
    with pytest.raises(RecoveryContractDecodeError, match="unexpected"):
        decode_recovery_certificate(unknown)

    missing = deepcopy(_certificate().to_record())
    del missing["decision"]
    with pytest.raises(RecoveryContractDecodeError, match="missing"):
        decode_recovery_certificate(missing)

    wrong_type = deepcopy(_certificate().to_record())
    wrong_type["aggregate_fence"]["state_revision"] = True
    with pytest.raises(RecoveryContractDecodeError, match="JSON integer"):
        decode_recovery_certificate(wrong_type)


def test_certificate_decoder_registry_fails_closed_for_unknown_version() -> None:
    record = deepcopy(_certificate().to_record())
    record["version"] = 99

    with pytest.raises(UnsupportedRecoveryCertificateVersion, match="version: 99"):
        decode_recovery_certificate(record)


def _delivery_contract(
    *, delivery_cycle: int = 2
) -> tuple[RecoveryDeliveryPayload, RecoveryDeliveryEnvelopeIdentity]:
    certificate = _certificate()
    payload = RecoveryDeliveryPayload(
        certificate=certificate,
        delivery_cycle=delivery_cycle,
    )
    envelope = RecoveryDeliveryEnvelopeIdentity(
        event_id=payload.event_id,
        profile_id=certificate.subject.profile_id,
        session_id=certificate.subject.session_id,
        ownership_generation=certificate.subject.ownership_generation,
    )
    return payload, envelope


def test_delivery_decoder_binds_payload_to_exact_mailbox_identity() -> None:
    payload, envelope = _delivery_contract()

    decoded_envelope = RecoveryDeliveryEnvelopeIdentity.from_record(
        envelope.to_record()
    )
    decoded = RecoveryDeliveryPayload.from_record(
        payload.to_record(),
        envelope=decoded_envelope,
    )

    assert decoded.to_record() == payload.to_record()
    assert decoded.event_id == envelope.event_id
    assert decoded.case_id == payload.certificate.case_identity.case_id


def test_delivery_decoder_rejects_case_cycle_event_and_subject_tampering() -> None:
    payload, envelope = _delivery_contract()
    bad_case = deepcopy(payload.to_record())
    bad_case["case_id"] = f"recovery-case:v1:{'0' * 64}"
    with pytest.raises(RecoveryContractDecodeError, match="case_id"):
        decode_recovery_delivery_payload(bad_case, envelope=envelope)

    bad_cycle = deepcopy(payload.to_record())
    bad_cycle["delivery_cycle"] = payload.delivery_cycle + 1
    with pytest.raises(RecoveryContractDecodeError, match="event_id"):
        decode_recovery_delivery_payload(bad_cycle, envelope=envelope)

    bad_event = replace(envelope, event_id=f"{envelope.event_id}-tampered")
    with pytest.raises(RecoveryContractDecodeError, match="event_id"):
        decode_recovery_delivery_payload(payload.to_record(), envelope=bad_event)

    bad_subject = replace(envelope, profile_id="profile-b")
    with pytest.raises(RecoveryContractDecodeError, match="subject"):
        decode_recovery_delivery_payload(payload.to_record(), envelope=bad_subject)


def test_delivery_decoders_reject_unknown_version_and_envelope_shape() -> None:
    payload, envelope = _delivery_contract()
    unknown = deepcopy(payload.to_record())
    unknown["version"] = 99
    with pytest.raises(UnsupportedRecoveryDeliveryVersion, match="version: 99"):
        decode_recovery_delivery_payload(unknown, envelope=envelope)

    envelope_record = envelope.to_record()
    envelope_record["unexpected"] = True
    with pytest.raises(RecoveryContractDecodeError, match="unexpected"):
        RecoveryDeliveryEnvelopeIdentity.from_record(envelope_record)

    assert envelope.kind == RECOVERY_DELIVERY_EVENT_KIND
    assert envelope.source == RECOVERY_DELIVERY_EVENT_SOURCE


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    [
        ("kind", "OtherEvent"),
        ("source", "other_scanner"),
        ("ownership_generation", True),
    ],
)
def test_delivery_envelope_decoder_rejects_each_identity_field_tamper(
    field_name: str,
    replacement: object,
) -> None:
    _, envelope = _delivery_contract()
    record = envelope.to_record()
    record[field_name] = replacement

    with pytest.raises(RecoveryContractDecodeError):
        RecoveryDeliveryEnvelopeIdentity.from_record(record)


@pytest.mark.parametrize(
    "value",
    [
        {"bad": 1.25},
        {"nested": [0.0]},
        {"bad": object()},
        {1: "non-string-key"},
    ],
)
def test_canonical_contract_rejects_float_and_non_json_values(value: object) -> None:
    with pytest.raises(TypeError):
        canonical_recovery_json(value)


@pytest.mark.parametrize(
    "value",
    [
        {"bad": "\ud800"},
        {"\udfff": "bad-key"},
    ],
)
def test_canonical_contract_rejects_non_utf8_unicode(value: object) -> None:
    with pytest.raises(TypeError, match="valid UTF-8"):
        canonical_recovery_json(value)


def test_certificate_decoder_rejects_non_utf8_unicode() -> None:
    record = deepcopy(_certificate().to_record())
    record["nodes"][0]["facts"] = {"bad": "\ud800"}

    with pytest.raises(RecoveryContractDecodeError, match="valid UTF-8"):
        decode_recovery_certificate(record)


def test_certificate_decoder_rejects_excessive_json_depth() -> None:
    nested: object = 0
    for _index in range(130):
        nested = [nested]
    with pytest.raises(TypeError, match="maximum recovery JSON nesting depth"):
        canonical_recovery_json({"nested": nested})

    record = deepcopy(_certificate().to_record())
    record["nodes"][0]["facts"] = {"nested": nested}
    with pytest.raises(
        RecoveryContractDecodeError,
        match="maximum recovery JSON nesting depth",
    ):
        decode_recovery_certificate(record)


def test_canonical_contract_rejects_excessive_json_nodes_and_bytes() -> None:
    excessive_nodes = {"items": [0] * MAX_RECOVERY_JSON_NODES}
    with pytest.raises(TypeError, match="maximum recovery JSON node count"):
        canonical_recovery_json(excessive_nodes)

    excessive_bytes = {"items": ["x" * MAX_RECOVERY_TEXT_BYTES] * 256}
    with pytest.raises(TypeError, match="maximum canonical JSON byte size"):
        canonical_recovery_json(excessive_bytes)


def test_contract_rejects_oversized_text_before_certificate_construction() -> None:
    with pytest.raises(TypeError, match="maximum recovery text byte size"):
        RecoveryGraphNode(
            identity="effect:oversized",
            kind="effect",
            authority="agent_effect_outbox",
            status="pending",
            facts={"payload": "x" * (MAX_RECOVERY_TEXT_BYTES + 1)},
        )

    record = deepcopy(_certificate().to_record())
    record["nodes"][0]["facts"] = {
        "payload": "x" * (MAX_RECOVERY_TEXT_BYTES + 1)
    }
    with pytest.raises(RecoveryContractDecodeError, match="maximum recovery text"):
        decode_recovery_certificate(record)


def test_graph_and_decision_collections_are_bounded() -> None:
    nodes = tuple(
        RecoveryGraphNode(
            identity=f"node:{index}",
            kind="authority",
            authority="test",
            status="known",
        )
        for index in range(MAX_RECOVERY_GRAPH_NODES + 1)
    )
    with pytest.raises(ValueError, match="nodes exceeds"):
        build_recovery_certificate(
            subject=_subject(),
            aggregate_fence=_fence(),
            nodes=nodes,
            edges=(),
            invariants=(),
            decision=RecoveryDecision(kind=RecoveryDecisionKind.NO_RECOVERY),
        )

    operation = _operation_node()
    effect = _effect_node()
    edges = tuple(
        RecoveryGraphEdge(
            identity=f"edge:{index}",
            source=operation.identity,
            target=effect.identity,
            relation="produced",
        )
        for index in range(MAX_RECOVERY_GRAPH_EDGES + 1)
    )
    with pytest.raises(ValueError, match="edges exceeds"):
        build_recovery_certificate(
            subject=_subject(),
            aggregate_fence=_fence(),
            nodes=(operation, effect),
            edges=edges,
            invariants=(),
            decision=RecoveryDecision(kind=RecoveryDecisionKind.NO_RECOVERY),
        )

    invariants = tuple(
        RecoveryInvariant(
            identity=f"invariant:{index}",
            code="known",
            severity=RecoveryInvariantSeverity.INFO,
            authority="test",
            node_identity=operation.identity,
        )
        for index in range(MAX_RECOVERY_INVARIANTS + 1)
    )
    with pytest.raises(ValueError, match="invariants exceeds"):
        build_recovery_certificate(
            subject=_subject(),
            aggregate_fence=_fence(),
            nodes=(operation,),
            edges=(),
            invariants=invariants,
            decision=RecoveryDecision(kind=RecoveryDecisionKind.NO_RECOVERY),
        )

    with pytest.raises(ValueError, match="reason_codes exceeds"):
        RecoveryDecision(
            kind=RecoveryDecisionKind.NO_RECOVERY,
            reason_codes=tuple(
                f"reason-{index}"
                for index in range(MAX_RECOVERY_DECISION_REASON_CODES + 1)
            ),
        )
    with pytest.raises(ValueError, match="target_node_identities exceeds"):
        RecoveryDecision(
            kind=RecoveryDecisionKind.NO_RECOVERY,
            target_node_identities=tuple(
                f"node-{index}"
                for index in range(MAX_RECOVERY_DECISION_TARGETS + 1)
            ),
        )


def test_graph_rejects_duplicate_and_dangling_identity() -> None:
    duplicate = replace(_effect_node(), identity="operation:review-1")
    with pytest.raises(ValueError, match="duplicate recovery graph identity"):
        _certificate(nodes=(_operation_node(), duplicate))

    dangling = replace(_edge(), target="effect:missing")
    with pytest.raises(ValueError, match="references a missing node"):
        build_recovery_certificate(
            subject=_subject(),
            aggregate_fence=_fence(),
            nodes=(_operation_node(), _effect_node()),
            edges=(dangling,),
            invariants=(_invariant(),),
            decision=_decision(),
        )


def test_contract_rejects_float_inside_authority_record() -> None:
    with pytest.raises(TypeError, match="must not contain floats"):
        RecoveryGraphNode(
            identity="effect:bad",
            kind="effect",
            authority="agent_effect_outbox",
            status="pending",
            facts={"available_at": 1.0},
        )
