"""Pure contracts for commit-time session-actor recovery.

This module deliberately has no SQLite, store, reducer, registry, or workflow
dependency. The reducer can create a compact intent here, while a separate
commit coordinator later re-reads raw durable authority before materializing a
normal session transition.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from shinbot.agent.runtime.session_actor.aggregate import AgentSessionAggregate
from shinbot.agent.runtime.session_actor.events import SessionTransition
from shinbot.agent.runtime.session_actor.recovery import (
    MAX_RECOVERY_TEXT_BYTES,
    RecoveryCertificate,
    RecoveryContractDecodeError,
    RecoveryDeliveryEnvelopeIdentity,
    RecoveryDeliveryPayload,
    freeze_recovery_json_object,
    thaw_recovery_json,
)


class RecoveryCommitIntentMismatch(RuntimeError):
    """Raised when re-read typed authority differs from a reducer intent."""

    def __init__(self, code: str) -> None:
        """Expose one stable stale-authority classification code."""

        self.code = _required_text(code, field_name="recovery intent mismatch code")
        super().__init__(self.code)


class RecoveryDeliveryClaimLost(RuntimeError):
    """Raised when a typed recovery delivery is no longer owned by its claim.

    This lives in the pure recovery commit contract rather than the SQLite
    reader so the actor can recognize the only failure mode that must never
    enter its generic release/dead-letter path.
    """

    def __init__(self, code: str) -> None:
        """Expose one stable claim-loss classification code."""

        self.code = _required_text(code, field_name="recovery delivery claim code")
        super().__init__(self.code)


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryCommitIntent:
    """Compact typed expectation that must be revalidated during commit.

    The intent never carries the full certificate or a provisional target
    aggregate. Those values remain untrusted until the coordinator reconstructs
    authority from the claimed durable mailbox row and graph reader.
    """

    envelope: RecoveryDeliveryEnvelopeIdentity
    case_id: str
    delivery_cycle: int
    certificate_digest: str

    def __post_init__(self) -> None:
        """Normalize the immutable expectation carried by the pure reducer."""

        if not isinstance(self.envelope, RecoveryDeliveryEnvelopeIdentity):
            raise TypeError("envelope must be a RecoveryDeliveryEnvelopeIdentity")
        object.__setattr__(
            self,
            "case_id",
            _required_text(self.case_id, field_name="case_id"),
        )
        if type(self.delivery_cycle) is not int or self.delivery_cycle < 0:
            raise ValueError("delivery_cycle must be a non-negative integer")
        object.__setattr__(
            self,
            "certificate_digest",
            _sha256_digest(self.certificate_digest, field_name="certificate_digest"),
        )
        _validate_delivery_identity(
            envelope=self.envelope,
            case_id=self.case_id,
            delivery_cycle=self.delivery_cycle,
        )

    @classmethod
    def from_delivery(
        cls,
        *,
        envelope: RecoveryDeliveryEnvelopeIdentity,
        payload: RecoveryDeliveryPayload,
    ) -> RecoveryCommitIntent:
        """Build one compact expectation after pure typed payload decoding."""

        if not isinstance(envelope, RecoveryDeliveryEnvelopeIdentity):
            raise TypeError("envelope must be a RecoveryDeliveryEnvelopeIdentity")
        if not isinstance(payload, RecoveryDeliveryPayload):
            raise TypeError("payload must be a RecoveryDeliveryPayload")
        payload.validate_envelope(envelope)
        return cls(
            envelope=envelope,
            case_id=payload.case_id,
            delivery_cycle=payload.delivery_cycle,
            certificate_digest=payload.certificate.certificate_digest,
        )

    def validate_delivery(self, payload: RecoveryDeliveryPayload) -> None:
        """Reject a durable typed delivery that no longer matches this intent."""

        if not isinstance(payload, RecoveryDeliveryPayload):
            raise TypeError("payload must be a RecoveryDeliveryPayload")
        if payload.case_id != self.case_id:
            raise RecoveryCommitIntentMismatch("recovery_delivery_case_changed")
        if payload.delivery_cycle != self.delivery_cycle:
            raise RecoveryCommitIntentMismatch("recovery_delivery_cycle_changed")
        if payload.certificate.certificate_digest != self.certificate_digest:
            raise RecoveryCommitIntentMismatch("recovery_delivery_certificate_changed")
        try:
            payload.validate_envelope(self.envelope)
        except RecoveryContractDecodeError as exc:
            raise RecoveryCommitIntentMismatch("recovery_delivery_envelope_changed") from exc


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryMaterializationBlocked:
    """A fail-closed materializer result with no inferred recovery transition."""

    code: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize the operator-visible block code and freeze its details."""

        object.__setattr__(self, "code", _required_text(self.code, field_name="code"))
        object.__setattr__(
            self,
            "details",
            freeze_recovery_json_object(self.details, field_name="details"),
        )

    def to_record(self) -> dict[str, object]:
        """Return a fresh JSON-compatible diagnostic record for persistence."""

        return {
            "code": self.code,
            "details": thaw_recovery_json(self.details),
        }


class RecoveryMaterializer(Protocol):
    """Pure state-specific materializer invoked only after commit-time proof."""

    def materialize(
        self,
        *,
        aggregate: AgentSessionAggregate,
        intent: RecoveryCommitIntent,
        certificate: RecoveryCertificate,
    ) -> SessionTransition | RecoveryMaterializationBlocked:
        """Build a transition from only revalidated, immutable authority."""


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized or normalized != value:
        raise ValueError(f"{field_name} must be non-empty canonical text")
    try:
        encoded = normalized.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field_name} must contain valid UTF-8 text") from exc
    if len(encoded) > MAX_RECOVERY_TEXT_BYTES:
        raise ValueError(
            f"{field_name} exceeds the maximum recovery text byte size "
            f"of {MAX_RECOVERY_TEXT_BYTES}"
        )
    return normalized


def _sha256_digest(value: object, *, field_name: str) -> str:
    normalized = _required_text(value, field_name=field_name)
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return normalized


def _validate_delivery_identity(
    *,
    envelope: RecoveryDeliveryEnvelopeIdentity,
    case_id: str,
    delivery_cycle: int,
) -> None:
    """Reject a directly constructed intent with inconsistent deterministic ids."""

    prefix = "recovery-case:v1:"
    if len(case_id) != len(prefix) + 64 or not case_id.startswith(prefix):
        raise ValueError("case_id must be a v1 recovery case id")
    case_digest = case_id[len(prefix) :]
    if any(character not in "0123456789abcdef" for character in case_digest):
        raise ValueError("case_id must be a v1 recovery case id")
    expected_event_id = f"recovery-requested:v1:{case_digest}:{delivery_cycle}"
    if envelope.event_id != expected_event_id:
        raise ValueError("recovery delivery event_id does not match case and cycle")


__all__ = [
    "RecoveryCommitIntent",
    "RecoveryCommitIntentMismatch",
    "RecoveryDeliveryClaimLost",
    "RecoveryMaterializationBlocked",
    "RecoveryMaterializer",
]
