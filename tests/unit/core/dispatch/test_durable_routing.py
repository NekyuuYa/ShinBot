"""Tests for core-owned durable message routing contracts."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from shinbot.core.dispatch.durable_routing import (
    INGRESS_ROUTING_PAYLOAD_VERSION,
    MESSAGE_ROUTING_JOB_VERSION,
    IngressRoutingPayload,
    IngressRoutingPayloadError,
    MessageRoutingJobEnvelope,
)
from shinbot.schema.events import MessagePayload, UnifiedEvent
from shinbot.schema.resources import Channel, User


def _ingress_payload() -> IngressRoutingPayload:
    event = UnifiedEvent(
        type="message-created",
        self_id="bot-self",
        platform="mock",
        timestamp=100.0,
        user=User(id="user-a"),
        channel=Channel(id="room-a", type=0),
        message=MessagePayload(id="message-a", content="<at id=\"bot-self\"/>hello"),
    )
    return IngressRoutingPayload(
        event=event.model_dump(mode="json"),
        adapter_instance_id="instance-a",
        adapter_platform="mock",
        message_xml=event.message_content,
        trace_id="ingress:instance-a:message-a",
        observed_at=101.0,
        base_session_id="instance-a:group:room-a",
        bot_id="bot-a",
        bot_binding_id="binding-a",
        bot_session_id="bot-a:group:room-a",
        fresh_at_ingress=True,
    )


def test_routing_job_normalizes_correlation_to_trace() -> None:
    envelope = MessageRoutingJobEnvelope(
        job_id=" job-a ",
        idempotency_key=" ingress-a ",
        trace_id=" trace-a ",
        payload={"event": "message-created"},
    )

    assert envelope.job_id == "job-a"
    assert envelope.idempotency_key == "ingress-a"
    assert envelope.trace_id == "trace-a"
    assert envelope.correlation_id == "trace-a"
    assert envelope.version == MESSAGE_ROUTING_JOB_VERSION


def test_routing_job_rejects_unsupported_version() -> None:
    with pytest.raises(ValueError, match="unsupported message routing job version"):
        MessageRoutingJobEnvelope(
            job_id="job-a",
            idempotency_key="ingress-a",
            trace_id="trace-a",
            version=999,
        )


def test_routing_job_deep_freezes_canonical_payload() -> None:
    original = {"event": {"parts": [{"type": "text"}]}}
    envelope = MessageRoutingJobEnvelope(
        job_id="job-a",
        idempotency_key="ingress-a",
        trace_id="trace-a",
        payload=original,
    )

    original["event"]["parts"][0]["type"] = "mutated"

    assert envelope.payload["event"]["parts"][0]["type"] == "text"
    with pytest.raises(TypeError, match="payloads are immutable"):
        envelope.payload["event"]["parts"].append({"type": "image"})
    with pytest.raises(TypeError, match="payloads are immutable"):
        envelope.payload["event"]["new"] = True


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_routing_job_rejects_nonfinite_nested_payload(value: float) -> None:
    with pytest.raises(ValueError, match="payload numbers must be finite"):
        MessageRoutingJobEnvelope(
            job_id="job-a",
            idempotency_key="ingress-a",
            trace_id="trace-a",
            payload={"event": {"timestamp": value}},
        )


@pytest.mark.parametrize("field_name", ["job_id", "idempotency_key", "trace_id"])
def test_routing_job_rejects_missing_durable_identity(field_name: str) -> None:
    values = {
        "job_id": "job-a",
        "idempotency_key": "ingress-a",
        "trace_id": "trace-a",
    }
    values[field_name] = ""

    with pytest.raises(ValueError, match=field_name):
        MessageRoutingJobEnvelope(**values)


def test_importing_durable_routing_repository_does_not_load_agent_package() -> None:
    check = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import shinbot.persistence.repositories.durable_routing; "
                "assert not any(name == 'shinbot.agent' or "
                "name.startswith('shinbot.agent.') for name in sys.modules)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert check.returncode == 0, check.stderr


def test_ingress_payload_round_trip_preserves_signed_replay_boundary() -> None:
    payload = _ingress_payload()

    wire = json.loads(json.dumps(payload.to_payload()))
    restored = IngressRoutingPayload.from_payload(wire)

    assert restored == payload
    assert restored.version == INGRESS_ROUTING_PAYLOAD_VERSION
    assert restored.to_event().message_content == payload.message_xml
    assert restored.payload_digest == wire["payload_digest"]
    assert restored.routing_job_id == payload.routing_job_id


def test_ingress_logical_identity_is_stable_across_contract_and_trace_changes() -> None:
    payload = _ingress_payload()
    replay = _ingress_payload()
    routing_job_id = payload.routing_job_id
    object.__setattr__(replay, "version", replay.version + 1)
    object.__setattr__(replay, "trace_id", "replayed-with-new-trace")
    object.__setattr__(replay, "observed_at", 999.0)

    assert replay.routing_job_id == routing_job_id
    assert payload.has_same_ingress_identity(replay)


def test_ingress_job_envelope_carries_mutable_ownership_fence_outside_payload() -> None:
    payload = _ingress_payload()

    envelope = payload.to_job_envelope(ownership_generation=7)

    assert envelope.profile_id == "bot-a"
    assert envelope.session_id == "bot-a:group:room-a"
    assert envelope.ownership_generation == 7
    assert "ownership_generation" not in envelope.payload


def test_ingress_job_envelope_supports_reserved_admission_scope_outside_payload() -> None:
    payload = _ingress_payload()

    envelope = payload.to_job_envelope(
        ownership_generation=0,
        admission_fence_id="fence-a",
        admission_fence_generation=3,
    )

    assert envelope.profile_id == "bot-a"
    assert envelope.session_id == "bot-a:group:room-a"
    assert envelope.is_reserved_admission is True
    assert envelope.admission_fence_id == "fence-a"
    assert envelope.admission_fence_generation == 3
    assert "admission_fence_id" not in envelope.payload


@pytest.mark.parametrize(
    "changes",
    [
        {"profile_id": "bot-a"},
        {"session_id": "bot-a:group:room-a"},
        {"ownership_generation": 1},
        {
            "profile_id": "bot-a",
            "session_id": "bot-a:group:room-a",
            "ownership_generation": 0,
        },
    ],
)
def test_routing_job_rejects_partial_ownership_fence(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="provided together"):
        MessageRoutingJobEnvelope(
            job_id="job-a",
            idempotency_key="ingress-a",
            trace_id="trace-a",
            **changes,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {
            "profile_id": "bot-a",
            "session_id": "bot-a:group:room-a",
            "ownership_generation": 0,
            "admission_fence_id": "fence-a",
        },
        {
            "profile_id": "bot-a",
            "session_id": "bot-a:group:room-a",
            "ownership_generation": 0,
            "admission_fence_generation": 1,
        },
        {
            "admission_fence_id": "fence-a",
            "admission_fence_generation": 1,
        },
    ],
)
def test_routing_job_rejects_partial_admission_fence(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="provided together"):
        MessageRoutingJobEnvelope(
            job_id="job-a",
            idempotency_key="ingress-a",
            trace_id="trace-a",
            **changes,
        )


def test_ingress_payload_rejects_unknown_contract_fields() -> None:
    wire = _ingress_payload().to_payload()
    wire["untrusted_extension"] = True

    with pytest.raises(IngressRoutingPayloadError, match="fields differ"):
        IngressRoutingPayload.from_payload(wire)


def test_ingress_payload_rejects_digest_tampering() -> None:
    wire = _ingress_payload().to_payload()
    wire["fresh_at_ingress"] = False

    with pytest.raises(IngressRoutingPayloadError, match="payload_digest"):
        IngressRoutingPayload.from_payload(wire)


def test_ingress_payload_rejects_event_and_xml_disagreement() -> None:
    payload = _ingress_payload()

    with pytest.raises(IngressRoutingPayloadError, match="message_xml"):
        IngressRoutingPayload(
            event=payload.event,
            adapter_instance_id=payload.adapter_instance_id,
            adapter_platform=payload.adapter_platform,
            message_xml="different",
            trace_id=payload.trace_id,
            observed_at=payload.observed_at,
            base_session_id=payload.base_session_id,
            bot_id=payload.bot_id,
            bot_binding_id=payload.bot_binding_id,
            bot_session_id=payload.bot_session_id,
            fresh_at_ingress=payload.fresh_at_ingress,
        )


@pytest.mark.parametrize("observed_at", [float("inf"), float("-inf"), float("nan")])
def test_ingress_payload_rejects_nonfinite_observation_time(
    observed_at: float,
) -> None:
    payload = _ingress_payload()

    with pytest.raises(IngressRoutingPayloadError, match="observed_at"):
        IngressRoutingPayload(
            event=payload.event,
            adapter_instance_id=payload.adapter_instance_id,
            adapter_platform=payload.adapter_platform,
            message_xml=payload.message_xml,
            trace_id=payload.trace_id,
            observed_at=observed_at,
            base_session_id=payload.base_session_id,
            bot_id=payload.bot_id,
            bot_binding_id=payload.bot_binding_id,
            bot_session_id=payload.bot_session_id,
            fresh_at_ingress=payload.fresh_at_ingress,
        )
