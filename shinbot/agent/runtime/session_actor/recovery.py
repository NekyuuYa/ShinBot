"""Pure recovery graph and certificate contracts for durable session actors."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar

RECOVERY_CERTIFICATE_SCHEMA = "shinbot.agent.session.recovery-certificate"
RECOVERY_CERTIFICATE_VERSION = 1
RECOVERY_DELIVERY_SCHEMA = "shinbot.agent.session.recovery-delivery"
RECOVERY_DELIVERY_VERSION = 1
RECOVERY_DELIVERY_EVENT_KIND = "RecoveryRequested"
RECOVERY_DELIVERY_EVENT_SOURCE = "durable_session_recovery_scanner"

_MAX_RECOVERY_JSON_DEPTH = 128


class RecoveryContractDecodeError(ValueError):
    """Raised when persisted recovery authority is malformed or non-canonical."""


class UnsupportedRecoveryCertificateVersion(RecoveryContractDecodeError):
    """Raised when no certificate decoder is registered for a stored version."""


class UnsupportedRecoveryDeliveryVersion(RecoveryContractDecodeError):
    """Raised when no delivery decoder is registered for a stored version."""


class RecoveryInvariantSeverity(StrEnum):
    """Severity assigned by the versioned recovery graph policy."""

    INFO = "info"
    WARNING = "warning"
    BLOCKING = "blocking"


class RecoveryDecisionKind(StrEnum):
    """Exhaustive phase-one recovery policy outcomes."""

    NO_RECOVERY = "no_recovery"
    WAIT_FOR_PROGRESS = "wait_for_progress"
    RECORD_BLOCKER = "record_blocker"
    RECOVER_ORPHANED_WORK = "recover_orphaned_work"


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoverySubject:
    """Profile-scoped actor ownership identity covered by a certificate."""

    profile_id: str
    session_id: str
    ownership_generation: int

    def __post_init__(self) -> None:
        """Normalize the stable actor identity and validate its generation."""

        object.__setattr__(
            self,
            "profile_id",
            _required_text(self.profile_id, field_name="profile_id"),
        )
        object.__setattr__(
            self,
            "session_id",
            _required_text(self.session_id, field_name="session_id"),
        )
        _require_positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )

    def to_record(self) -> dict[str, object]:
        """Return the canonical JSON record for the actor subject."""

        return {
            "ownership_generation": self.ownership_generation,
            "profile_id": self.profile_id,
            "session_id": self.session_id,
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryAggregateFence:
    """Exact aggregate snapshot fenced by one recovery certificate."""

    state: str
    state_revision: int
    event_sequence: int
    activity_generation: int
    active_epoch: int
    current_plan_id: str = ""
    review_plan_revision: int = 0

    def __post_init__(self) -> None:
        """Validate the monotonic fields used for commit-time revalidation."""

        object.__setattr__(self, "state", _required_text(self.state, field_name="state"))
        object.__setattr__(
            self,
            "current_plan_id",
            _optional_text(self.current_plan_id, field_name="current_plan_id"),
        )
        for field_name in (
            "state_revision",
            "event_sequence",
            "activity_generation",
            "active_epoch",
            "review_plan_revision",
        ):
            _require_nonnegative_int(getattr(self, field_name), field_name=field_name)
        if bool(self.current_plan_id) != (self.review_plan_revision > 0):
            raise ValueError(
                "current_plan_id must be present exactly when review_plan_revision "
                "is positive"
            )

    def to_record(self) -> dict[str, object]:
        """Return the full aggregate fence included in certificate identity."""

        return {
            "active_epoch": self.active_epoch,
            "activity_generation": self.activity_generation,
            "current_plan_id": self.current_plan_id,
            "event_sequence": self.event_sequence,
            "review_plan_revision": self.review_plan_revision,
            "state": self.state,
            "state_revision": self.state_revision,
        }

    def to_work_graph_record(self) -> dict[str, object]:
        """Return the semantic fence, excluding mailbox delivery sequence."""

        record = self.to_record()
        record.pop("event_sequence")
        return record


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryGraphNode:
    """One normalized authority record represented in the recovery work graph."""

    identity: str
    kind: str
    authority: str
    status: str
    facts: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate node identity and freeze its normalized authority facts."""

        for field_name in ("identity", "kind", "authority", "status"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "facts",
            _freeze_json_object(self.facts, field_name="node.facts"),
        )

    def to_record(self) -> dict[str, object]:
        """Return a canonical node record."""

        return {
            "authority": self.authority,
            "facts": _thaw_json(self.facts),
            "identity": self.identity,
            "kind": self.kind,
            "status": self.status,
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryGraphEdge:
    """A deterministic dependency between two recovery graph nodes."""

    identity: str
    source: str
    target: str
    relation: str
    facts: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate edge identity and freeze its derived facts."""

        for field_name in ("identity", "source", "target", "relation"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        object.__setattr__(
            self,
            "facts",
            _freeze_json_object(self.facts, field_name="edge.facts"),
        )

    def to_record(self) -> dict[str, object]:
        """Return a canonical edge record."""

        return {
            "facts": _thaw_json(self.facts),
            "identity": self.identity,
            "relation": self.relation,
            "source": self.source,
            "target": self.target,
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryInvariant:
    """One deterministic graph consistency finding."""

    identity: str
    code: str
    severity: RecoveryInvariantSeverity
    authority: str
    node_identity: str = ""
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate invariant identity and freeze its diagnostics."""

        for field_name in ("identity", "code", "authority"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        if not isinstance(self.severity, RecoveryInvariantSeverity):
            object.__setattr__(self, "severity", RecoveryInvariantSeverity(self.severity))
        object.__setattr__(
            self,
            "node_identity",
            _optional_text(self.node_identity, field_name="node_identity"),
        )
        object.__setattr__(
            self,
            "details",
            _freeze_json_object(self.details, field_name="invariant.details"),
        )

    def to_record(self) -> dict[str, object]:
        """Return a canonical invariant record."""

        return {
            "authority": self.authority,
            "code": self.code,
            "details": _thaw_json(self.details),
            "identity": self.identity,
            "node_identity": self.node_identity,
            "severity": self.severity.value,
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryDecision:
    """Versioned policy decision derived solely from a normalized work graph."""

    kind: RecoveryDecisionKind
    reason_codes: tuple[str, ...] = ()
    target_node_identities: tuple[str, ...] = ()
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Normalize set-like fields and freeze decision diagnostics."""

        if not isinstance(self.kind, RecoveryDecisionKind):
            object.__setattr__(self, "kind", RecoveryDecisionKind(self.kind))
        object.__setattr__(
            self,
            "reason_codes",
            _normalized_text_set(self.reason_codes, field_name="reason_codes"),
        )
        object.__setattr__(
            self,
            "target_node_identities",
            _normalized_text_set(
                self.target_node_identities,
                field_name="target_node_identities",
            ),
        )
        object.__setattr__(
            self,
            "details",
            _freeze_json_object(self.details, field_name="decision.details"),
        )

    def to_record(self) -> dict[str, object]:
        """Return a canonical policy decision record."""

        return {
            "details": _thaw_json(self.details),
            "kind": self.kind.value,
            "reason_codes": list(self.reason_codes),
            "target_node_identities": list(self.target_node_identities),
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryCaseIdentity:
    """Stable identity shared by all deliveries for one semantic recovery case."""

    subject: RecoverySubject
    policy_version: int
    work_graph_digest: str
    certificate_version: int = RECOVERY_CERTIFICATE_VERSION
    case_id: str = field(init=False)

    def __post_init__(self) -> None:
        """Derive the case id from subject, policy, and semantic work graph."""

        if not isinstance(self.subject, RecoverySubject):
            raise TypeError("subject must be a RecoverySubject")
        _require_positive_int(self.policy_version, field_name="policy_version")
        _require_positive_int(
            self.certificate_version,
            field_name="certificate_version",
        )
        _require_sha256_digest(
            self.work_graph_digest,
            field_name="work_graph_digest",
        )
        digest = canonical_recovery_digest(
            {
                "policy_version": self.policy_version,
                "subject": self.subject.to_record(),
                "work_graph_digest": self.work_graph_digest,
            }
        )
        object.__setattr__(
            self,
            "case_id",
            f"recovery-case:v{self.certificate_version}:{digest}",
        )

    def to_record(self) -> dict[str, object]:
        """Return the persisted recovery-case identity."""

        return {
            "case_id": self.case_id,
            "certificate_version": self.certificate_version,
            "policy_version": self.policy_version,
            "subject": self.subject.to_record(),
            "work_graph_digest": self.work_graph_digest,
        }


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryCertificate:
    """Canonical, immutable proof of one session recovery decision."""

    schema: ClassVar[str] = RECOVERY_CERTIFICATE_SCHEMA
    version: ClassVar[int] = RECOVERY_CERTIFICATE_VERSION

    subject: RecoverySubject
    aggregate_fence: RecoveryAggregateFence
    nodes: tuple[RecoveryGraphNode, ...]
    edges: tuple[RecoveryGraphEdge, ...]
    invariants: tuple[RecoveryInvariant, ...]
    decision: RecoveryDecision
    policy_version: int = 1
    work_graph_digest: str = field(init=False)
    certificate_digest: str = field(init=False)
    case_identity: RecoveryCaseIdentity = field(init=False)

    def __post_init__(self) -> None:
        """Normalize graph order, validate identity, and derive all digests."""

        if not isinstance(self.subject, RecoverySubject):
            raise TypeError("subject must be a RecoverySubject")
        if not isinstance(self.aggregate_fence, RecoveryAggregateFence):
            raise TypeError("aggregate_fence must be a RecoveryAggregateFence")
        if not isinstance(self.decision, RecoveryDecision):
            raise TypeError("decision must be a RecoveryDecision")
        _require_positive_int(self.policy_version, field_name="policy_version")

        nodes = _normalized_contract_items(
            self.nodes,
            item_type=RecoveryGraphNode,
            field_name="nodes",
        )
        edges = _normalized_contract_items(
            self.edges,
            item_type=RecoveryGraphEdge,
            field_name="edges",
        )
        invariants = _normalized_contract_items(
            self.invariants,
            item_type=RecoveryInvariant,
            field_name="invariants",
        )
        _validate_graph_identity(nodes, edges, invariants, self.decision)
        object.__setattr__(self, "nodes", nodes)
        object.__setattr__(self, "edges", edges)
        object.__setattr__(self, "invariants", invariants)

        work_graph_digest = canonical_recovery_digest(self._work_graph_record())
        object.__setattr__(self, "work_graph_digest", work_graph_digest)
        case_identity = RecoveryCaseIdentity(
            subject=self.subject,
            policy_version=self.policy_version,
            work_graph_digest=work_graph_digest,
            certificate_version=self.version,
        )
        object.__setattr__(self, "case_identity", case_identity)
        object.__setattr__(
            self,
            "certificate_digest",
            canonical_recovery_digest(self._certificate_identity_record()),
        )

    def _work_graph_record(self) -> dict[str, object]:
        return {
            "aggregate_fence": self.aggregate_fence.to_work_graph_record(),
            "decision": self.decision.to_record(),
            "edges": [edge.to_record() for edge in self.edges],
            "invariants": [invariant.to_record() for invariant in self.invariants],
            "nodes": [node.to_record() for node in self.nodes],
            "schema": self.schema,
            "version": self.version,
        }

    def _certificate_identity_record(self) -> dict[str, object]:
        return {
            "aggregate_fence": self.aggregate_fence.to_record(),
            "case_id": self.case_identity.case_id,
            "decision": self.decision.to_record(),
            "edges": [edge.to_record() for edge in self.edges],
            "invariants": [invariant.to_record() for invariant in self.invariants],
            "nodes": [node.to_record() for node in self.nodes],
            "policy_version": self.policy_version,
            "schema": self.schema,
            "subject": self.subject.to_record(),
            "version": self.version,
            "work_graph_digest": self.work_graph_digest,
        }

    def to_record(self) -> dict[str, object]:
        """Return the complete certificate record suitable for mailbox payloads."""

        return {
            **self._certificate_identity_record(),
            "certificate_digest": self.certificate_digest,
        }

    @classmethod
    def from_record(cls, value: object) -> RecoveryCertificate:
        """Decode and verify one persisted, versioned certificate record."""

        certificate = decode_recovery_certificate(value)
        if not isinstance(certificate, cls):
            raise RecoveryContractDecodeError(
                "decoded recovery certificate has an unexpected contract type"
            )
        return certificate


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryDeliveryEnvelopeIdentity:
    """Mailbox identity that must match one typed recovery delivery payload."""

    event_id: str
    profile_id: str
    session_id: str
    ownership_generation: int
    kind: str = RECOVERY_DELIVERY_EVENT_KIND
    source: str = RECOVERY_DELIVERY_EVENT_SOURCE

    def __post_init__(self) -> None:
        """Normalize and validate the fixed recovery mailbox identity."""

        for field_name in ("event_id", "profile_id", "session_id", "kind", "source"):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name=field_name),
            )
        _require_positive_int(
            self.ownership_generation,
            field_name="ownership_generation",
        )
        if self.kind != RECOVERY_DELIVERY_EVENT_KIND:
            raise ValueError(f"recovery delivery kind must be {RECOVERY_DELIVERY_EVENT_KIND!r}")
        if self.source != RECOVERY_DELIVERY_EVENT_SOURCE:
            raise ValueError(
                "recovery delivery source must be "
                f"{RECOVERY_DELIVERY_EVENT_SOURCE!r}"
            )

    @property
    def subject(self) -> RecoverySubject:
        """Return the profile-scoped actor identity carried by the mailbox."""

        return RecoverySubject(
            profile_id=self.profile_id,
            session_id=self.session_id,
            ownership_generation=self.ownership_generation,
        )

    def to_record(self) -> dict[str, object]:
        """Return the strict mailbox identity projection used by the decoder."""

        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "ownership_generation": self.ownership_generation,
            "profile_id": self.profile_id,
            "session_id": self.session_id,
            "source": self.source,
        }

    @classmethod
    def from_record(cls, value: object) -> RecoveryDeliveryEnvelopeIdentity:
        """Decode an exact mailbox identity record."""

        record = _strict_record(
            value,
            field_name="recovery_delivery_envelope",
            expected_fields={
                "event_id",
                "kind",
                "ownership_generation",
                "profile_id",
                "session_id",
                "source",
            },
        )
        try:
            identity = cls(
                event_id=_strict_string(record["event_id"], field_name="event_id"),
                kind=_strict_string(record["kind"], field_name="kind"),
                ownership_generation=_strict_integer(
                    record["ownership_generation"],
                    field_name="ownership_generation",
                ),
                profile_id=_strict_string(
                    record["profile_id"],
                    field_name="profile_id",
                ),
                session_id=_strict_string(
                    record["session_id"],
                    field_name="session_id",
                ),
                source=_strict_string(record["source"], field_name="source"),
            )
        except (TypeError, ValueError) as exc:
            raise RecoveryContractDecodeError(str(exc)) from exc
        _require_canonical_record(
            record,
            identity.to_record(),
            field_name="recovery delivery envelope",
        )
        return identity


@dataclass(slots=True, frozen=True, kw_only=True)
class RecoveryDeliveryPayload:
    """Typed payload for one deterministic recovery mailbox delivery cycle."""

    schema: ClassVar[str] = RECOVERY_DELIVERY_SCHEMA
    version: ClassVar[int] = RECOVERY_DELIVERY_VERSION

    certificate: RecoveryCertificate
    delivery_cycle: int
    case_id: str = field(init=False)
    event_id: str = field(init=False)

    def __post_init__(self) -> None:
        """Derive the case and mailbox identities from immutable authority."""

        if not isinstance(self.certificate, RecoveryCertificate):
            raise TypeError("certificate must be a RecoveryCertificate")
        _require_nonnegative_int(self.delivery_cycle, field_name="delivery_cycle")
        object.__setattr__(self, "case_id", self.certificate.case_identity.case_id)
        object.__setattr__(
            self,
            "event_id",
            recovery_delivery_event_id(
                self.certificate.case_identity,
                delivery_cycle=self.delivery_cycle,
            ),
        )

    def to_record(self) -> dict[str, object]:
        """Return the canonical mailbox payload record."""

        return {
            "case_id": self.case_id,
            "certificate": self.certificate.to_record(),
            "delivery_cycle": self.delivery_cycle,
            "schema": self.schema,
            "version": self.version,
        }

    def validate_envelope(self, envelope: RecoveryDeliveryEnvelopeIdentity) -> None:
        """Validate the mailbox subject and event id against this payload."""

        if not isinstance(envelope, RecoveryDeliveryEnvelopeIdentity):
            raise TypeError("envelope must be a RecoveryDeliveryEnvelopeIdentity")
        if envelope.subject != self.certificate.subject:
            raise RecoveryContractDecodeError(
                "recovery delivery envelope subject does not match certificate"
            )
        _require_constant_time_match(
            envelope.event_id,
            self.event_id,
            field_name="recovery delivery event_id",
        )

    @classmethod
    def from_record(
        cls,
        value: object,
        *,
        envelope: RecoveryDeliveryEnvelopeIdentity,
    ) -> RecoveryDeliveryPayload:
        """Decode and verify a payload against its mailbox envelope identity."""

        payload = decode_recovery_delivery_payload(value, envelope=envelope)
        if not isinstance(payload, cls):
            raise RecoveryContractDecodeError(
                "decoded recovery delivery has an unexpected contract type"
            )
        return payload


def build_recovery_certificate(
    *,
    subject: RecoverySubject,
    aggregate_fence: RecoveryAggregateFence,
    nodes: Sequence[RecoveryGraphNode],
    edges: Sequence[RecoveryGraphEdge],
    invariants: Sequence[RecoveryInvariant],
    decision: RecoveryDecision,
    policy_version: int = 1,
) -> RecoveryCertificate:
    """Build a certificate from already-normalized authority records.

    This function performs no database reads and applies no recovery policy. A
    future persistence adapter is responsible for producing the typed authority
    records and a versioned policy is responsible for producing ``decision``.

    Args:
        subject: Exact profile-scoped ownership identity.
        aggregate_fence: Aggregate snapshot observed with the authority records.
        nodes: Normalized authority and derived work nodes.
        edges: Deterministic dependencies between nodes.
        invariants: Versioned graph consistency findings.
        decision: Recovery policy output for this exact graph.
        policy_version: Positive version of the graph policy.

    Returns:
        An immutable certificate with deterministic graph, case, and snapshot
        identities.
    """

    return RecoveryCertificate(
        subject=subject,
        aggregate_fence=aggregate_fence,
        nodes=tuple(nodes),
        edges=tuple(edges),
        invariants=tuple(invariants),
        decision=decision,
        policy_version=policy_version,
    )


def recovery_delivery_event_id(
    case_identity: RecoveryCaseIdentity,
    *,
    delivery_cycle: int,
) -> str:
    """Return a deterministic mailbox id for one recovery delivery cycle."""

    if not isinstance(case_identity, RecoveryCaseIdentity):
        raise TypeError("case_identity must be a RecoveryCaseIdentity")
    _require_nonnegative_int(delivery_cycle, field_name="delivery_cycle")
    case_digest = case_identity.case_id.rsplit(":", maxsplit=1)[-1]
    return (
        f"recovery-requested:v{case_identity.certificate_version}:"
        f"{case_digest}:{delivery_cycle}"
    )


def canonical_recovery_json(value: object) -> str:
    """Serialize bounded UTF-8 JSON authority data, rejecting all floats."""

    normalized = _thaw_json(_freeze_json(value, path="recovery", depth=0))
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_recovery_digest(value: object) -> str:
    """Return the lowercase SHA-256 digest of canonical recovery JSON."""

    canonical = canonical_recovery_json(value)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def decode_recovery_certificate(value: object) -> RecoveryCertificate:
    """Decode a certificate through the registry for its persisted version.

    Args:
        value: Raw JSON-compatible certificate record.

    Returns:
        The verified immutable certificate.

    Raises:
        RecoveryContractDecodeError: If the record is malformed or tampered.
        UnsupportedRecoveryCertificateVersion: If its version is unknown.
    """

    record = _strict_record(
        value,
        field_name="recovery_certificate",
        required_fields={"schema", "version"},
    )
    schema = _strict_string(record["schema"], field_name="certificate.schema")
    if schema != RECOVERY_CERTIFICATE_SCHEMA:
        raise RecoveryContractDecodeError(
            f"unsupported recovery certificate schema: {schema!r}"
        )
    version = _strict_integer(record["version"], field_name="certificate.version")
    decoder = _RECOVERY_CERTIFICATE_DECODERS.get(version)
    if decoder is None:
        raise UnsupportedRecoveryCertificateVersion(
            f"unsupported recovery certificate version: {version}"
        )
    return decoder(record)


def decode_recovery_delivery_payload(
    value: object,
    *,
    envelope: RecoveryDeliveryEnvelopeIdentity,
) -> RecoveryDeliveryPayload:
    """Decode a recovery payload and verify its mailbox envelope identity.

    Args:
        value: Raw JSON-compatible recovery delivery payload.
        envelope: Exact profile, generation, kind, source, and event id from the
            durable mailbox row.

    Returns:
        The verified typed delivery payload.

    Raises:
        RecoveryContractDecodeError: If payload or envelope authority conflicts.
        UnsupportedRecoveryDeliveryVersion: If its version is unknown.
    """

    if not isinstance(envelope, RecoveryDeliveryEnvelopeIdentity):
        raise TypeError("envelope must be a RecoveryDeliveryEnvelopeIdentity")
    record = _strict_record(
        value,
        field_name="recovery_delivery",
        required_fields={"schema", "version"},
    )
    schema = _strict_string(record["schema"], field_name="delivery.schema")
    if schema != RECOVERY_DELIVERY_SCHEMA:
        raise RecoveryContractDecodeError(
            f"unsupported recovery delivery schema: {schema!r}"
        )
    version = _strict_integer(record["version"], field_name="delivery.version")
    decoder = _RECOVERY_DELIVERY_DECODERS.get(version)
    if decoder is None:
        raise UnsupportedRecoveryDeliveryVersion(
            f"unsupported recovery delivery version: {version}"
        )
    return decoder(record, envelope)


def _decode_recovery_certificate_v1(
    record: Mapping[str, Any],
) -> RecoveryCertificate:
    record = _strict_record(
        record,
        field_name="recovery_certificate_v1",
        expected_fields={
            "aggregate_fence",
            "case_id",
            "certificate_digest",
            "decision",
            "edges",
            "invariants",
            "nodes",
            "policy_version",
            "schema",
            "subject",
            "version",
            "work_graph_digest",
        },
    )
    try:
        work_graph_digest = _strict_string(
            record["work_graph_digest"],
            field_name="work_graph_digest",
        )
        certificate_digest = _strict_string(
            record["certificate_digest"],
            field_name="certificate_digest",
        )
        _require_sha256_digest(
            work_graph_digest,
            field_name="work_graph_digest",
        )
        _require_sha256_digest(
            certificate_digest,
            field_name="certificate_digest",
        )
        certificate = RecoveryCertificate(
            subject=_decode_recovery_subject(record["subject"]),
            aggregate_fence=_decode_recovery_aggregate_fence(
                record["aggregate_fence"]
            ),
            nodes=tuple(
                _decode_recovery_graph_node(item)
                for item in _strict_array(record["nodes"], field_name="nodes")
            ),
            edges=tuple(
                _decode_recovery_graph_edge(item)
                for item in _strict_array(record["edges"], field_name="edges")
            ),
            invariants=tuple(
                _decode_recovery_invariant(item)
                for item in _strict_array(
                    record["invariants"],
                    field_name="invariants",
                )
            ),
            decision=_decode_recovery_decision(record["decision"]),
            policy_version=_strict_integer(
                record["policy_version"],
                field_name="policy_version",
            ),
        )
        case_id = _strict_string(record["case_id"], field_name="case_id")
        _require_constant_time_match(
            work_graph_digest,
            certificate.work_graph_digest,
            field_name="work_graph_digest",
        )
        _require_constant_time_match(
            case_id,
            certificate.case_identity.case_id,
            field_name="case_id",
        )
        _require_constant_time_match(
            certificate_digest,
            certificate.certificate_digest,
            field_name="certificate_digest",
        )
        _require_canonical_record(
            record,
            certificate.to_record(),
            field_name="recovery certificate",
        )
    except RecoveryContractDecodeError:
        raise
    except (TypeError, ValueError) as exc:
        raise RecoveryContractDecodeError(str(exc)) from exc
    return certificate


def _decode_recovery_delivery_v1(
    record: Mapping[str, Any],
    envelope: RecoveryDeliveryEnvelopeIdentity,
) -> RecoveryDeliveryPayload:
    record = _strict_record(
        record,
        field_name="recovery_delivery_v1",
        expected_fields={
            "case_id",
            "certificate",
            "delivery_cycle",
            "schema",
            "version",
        },
    )
    try:
        certificate = decode_recovery_certificate(record["certificate"])
        payload = RecoveryDeliveryPayload(
            certificate=certificate,
            delivery_cycle=_strict_integer(
                record["delivery_cycle"],
                field_name="delivery_cycle",
            ),
        )
        _require_constant_time_match(
            _strict_string(record["case_id"], field_name="case_id"),
            payload.case_id,
            field_name="delivery case_id",
        )
        _require_canonical_record(
            record,
            payload.to_record(),
            field_name="recovery delivery",
        )
        payload.validate_envelope(envelope)
    except RecoveryContractDecodeError:
        raise
    except (TypeError, ValueError) as exc:
        raise RecoveryContractDecodeError(str(exc)) from exc
    return payload


def _decode_recovery_subject(value: object) -> RecoverySubject:
    record = _strict_record(
        value,
        field_name="subject",
        expected_fields={"ownership_generation", "profile_id", "session_id"},
    )
    return RecoverySubject(
        profile_id=_strict_string(record["profile_id"], field_name="profile_id"),
        session_id=_strict_string(record["session_id"], field_name="session_id"),
        ownership_generation=_strict_integer(
            record["ownership_generation"],
            field_name="ownership_generation",
        ),
    )


def _decode_recovery_aggregate_fence(value: object) -> RecoveryAggregateFence:
    record = _strict_record(
        value,
        field_name="aggregate_fence",
        expected_fields={
            "active_epoch",
            "activity_generation",
            "current_plan_id",
            "event_sequence",
            "review_plan_revision",
            "state",
            "state_revision",
        },
    )
    return RecoveryAggregateFence(
        state=_strict_string(record["state"], field_name="state"),
        state_revision=_strict_integer(
            record["state_revision"],
            field_name="state_revision",
        ),
        event_sequence=_strict_integer(
            record["event_sequence"],
            field_name="event_sequence",
        ),
        activity_generation=_strict_integer(
            record["activity_generation"],
            field_name="activity_generation",
        ),
        active_epoch=_strict_integer(
            record["active_epoch"],
            field_name="active_epoch",
        ),
        current_plan_id=_strict_string(
            record["current_plan_id"],
            field_name="current_plan_id",
        ),
        review_plan_revision=_strict_integer(
            record["review_plan_revision"],
            field_name="review_plan_revision",
        ),
    )


def _decode_recovery_graph_node(value: object) -> RecoveryGraphNode:
    record = _strict_record(
        value,
        field_name="node",
        expected_fields={"authority", "facts", "identity", "kind", "status"},
    )
    return RecoveryGraphNode(
        identity=_strict_string(record["identity"], field_name="node.identity"),
        kind=_strict_string(record["kind"], field_name="node.kind"),
        authority=_strict_string(
            record["authority"],
            field_name="node.authority",
        ),
        status=_strict_string(record["status"], field_name="node.status"),
        facts=_strict_record(record["facts"], field_name="node.facts"),
    )


def _decode_recovery_graph_edge(value: object) -> RecoveryGraphEdge:
    record = _strict_record(
        value,
        field_name="edge",
        expected_fields={"facts", "identity", "relation", "source", "target"},
    )
    return RecoveryGraphEdge(
        identity=_strict_string(record["identity"], field_name="edge.identity"),
        source=_strict_string(record["source"], field_name="edge.source"),
        target=_strict_string(record["target"], field_name="edge.target"),
        relation=_strict_string(record["relation"], field_name="edge.relation"),
        facts=_strict_record(record["facts"], field_name="edge.facts"),
    )


def _decode_recovery_invariant(value: object) -> RecoveryInvariant:
    record = _strict_record(
        value,
        field_name="invariant",
        expected_fields={
            "authority",
            "code",
            "details",
            "identity",
            "node_identity",
            "severity",
        },
    )
    return RecoveryInvariant(
        identity=_strict_string(
            record["identity"],
            field_name="invariant.identity",
        ),
        code=_strict_string(record["code"], field_name="invariant.code"),
        severity=RecoveryInvariantSeverity(
            _strict_string(record["severity"], field_name="invariant.severity")
        ),
        authority=_strict_string(
            record["authority"],
            field_name="invariant.authority",
        ),
        node_identity=_strict_string(
            record["node_identity"],
            field_name="invariant.node_identity",
        ),
        details=_strict_record(record["details"], field_name="invariant.details"),
    )


def _decode_recovery_decision(value: object) -> RecoveryDecision:
    record = _strict_record(
        value,
        field_name="decision",
        expected_fields={
            "details",
            "kind",
            "reason_codes",
            "target_node_identities",
        },
    )
    return RecoveryDecision(
        kind=RecoveryDecisionKind(
            _strict_string(record["kind"], field_name="decision.kind")
        ),
        reason_codes=tuple(
            _strict_string(item, field_name="decision.reason_codes item")
            for item in _strict_array(
                record["reason_codes"],
                field_name="decision.reason_codes",
            )
        ),
        target_node_identities=tuple(
            _strict_string(item, field_name="decision.target_node_identities item")
            for item in _strict_array(
                record["target_node_identities"],
                field_name="decision.target_node_identities",
            )
        ),
        details=_strict_record(record["details"], field_name="decision.details"),
    )


_RecoveryCertificateDecoder = Callable[[Mapping[str, Any]], RecoveryCertificate]
_RecoveryDeliveryDecoder = Callable[
    [Mapping[str, Any], RecoveryDeliveryEnvelopeIdentity],
    RecoveryDeliveryPayload,
]

_RECOVERY_CERTIFICATE_DECODERS: dict[int, _RecoveryCertificateDecoder] = {
    RECOVERY_CERTIFICATE_VERSION: _decode_recovery_certificate_v1,
}
_RECOVERY_DELIVERY_DECODERS: dict[int, _RecoveryDeliveryDecoder] = {
    RECOVERY_DELIVERY_VERSION: _decode_recovery_delivery_v1,
}


def _validate_graph_identity(
    nodes: tuple[RecoveryGraphNode, ...],
    edges: tuple[RecoveryGraphEdge, ...],
    invariants: tuple[RecoveryInvariant, ...],
    decision: RecoveryDecision,
) -> None:
    identities: set[str] = set()
    for collection_name, items in (
        ("node", nodes),
        ("edge", edges),
        ("invariant", invariants),
    ):
        for item in items:
            if item.identity in identities:
                raise ValueError(
                    f"duplicate recovery graph identity: {item.identity!r} "
                    f"({collection_name})"
                )
            identities.add(item.identity)

    node_identities = {node.identity for node in nodes}
    for edge in edges:
        if edge.source not in node_identities or edge.target not in node_identities:
            raise ValueError(f"recovery edge {edge.identity!r} references a missing node")
    for invariant in invariants:
        if invariant.node_identity and invariant.node_identity not in node_identities:
            raise ValueError(
                f"recovery invariant {invariant.identity!r} references a missing node"
            )
    missing_targets = set(decision.target_node_identities) - node_identities
    if missing_targets:
        raise ValueError(
            "recovery decision references missing nodes: "
            + ", ".join(sorted(missing_targets))
        )


def _normalized_contract_items(
    values: Sequence[Any],
    *,
    item_type: type[Any],
    field_name: str,
) -> tuple[Any, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError(f"{field_name} must be a sequence")
    normalized = tuple(values)
    if any(not isinstance(value, item_type) for value in normalized):
        raise TypeError(f"{field_name} contains an invalid contract type")
    return tuple(sorted(normalized, key=lambda value: value.identity))


def _normalized_text_set(values: Sequence[str], *, field_name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError(f"{field_name} must be a sequence of strings")
    normalized = tuple(
        _required_text(value, field_name=f"{field_name} item") for value in values
    )
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} contains duplicate values")
    return tuple(sorted(normalized))


def _freeze_json_object(value: object, *, field_name: str) -> Mapping[str, Any]:
    frozen = _freeze_json(value, path=field_name, depth=0)
    if not isinstance(frozen, Mapping):
        raise TypeError(f"{field_name} must be a mapping")
    return frozen


def _freeze_json(value: object, *, path: str, depth: int) -> Any:
    if depth > _MAX_RECOVERY_JSON_DEPTH:
        raise TypeError(
            f"{path} exceeds the maximum recovery JSON nesting depth "
            f"of {_MAX_RECOVERY_JSON_DEPTH}"
        )
    if isinstance(value, Mapping):
        items: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings")
            if not _is_valid_utf8(key):
                raise TypeError(f"{path} keys must contain valid UTF-8 text")
            items[key] = _freeze_json(
                item,
                path=f"{path}.{key}",
                depth=depth + 1,
            )
        return MappingProxyType(items)
    if isinstance(value, (list, tuple)):
        return tuple(
            _freeze_json(
                item,
                path=f"{path}[{index}]",
                depth=depth + 1,
            )
            for index, item in enumerate(value)
        )
    if isinstance(value, float):
        raise TypeError(f"{path} must not contain floats")
    if isinstance(value, str):
        if not _is_valid_utf8(value):
            raise TypeError(f"{path} must contain valid UTF-8 text")
        return value
    if value is None or isinstance(value, (int, bool)):
        return value
    raise TypeError(f"{path} must contain only float-free JSON-compatible values")


def _thaw_json(value: object) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(item) for item in value]
    return value


def _strict_record(
    value: object,
    *,
    field_name: str,
    expected_fields: set[str] | None = None,
    required_fields: set[str] | None = None,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RecoveryContractDecodeError(f"{field_name} must be a JSON object")
    keys: set[str] = set()
    for key in value:
        if not isinstance(key, str):
            raise RecoveryContractDecodeError(f"{field_name} keys must be strings")
        keys.add(key)
    if expected_fields is not None and keys != expected_fields:
        missing = sorted(expected_fields - keys)
        unexpected = sorted(keys - expected_fields)
        raise RecoveryContractDecodeError(
            f"{field_name} fields do not match the contract; "
            f"missing={missing!r}, unexpected={unexpected!r}"
        )
    if required_fields is not None:
        missing = sorted(required_fields - keys)
        if missing:
            raise RecoveryContractDecodeError(
                f"{field_name} is missing required fields: {missing!r}"
            )
    return value


def _strict_array(value: object, *, field_name: str) -> list[Any]:
    if type(value) is not list:
        raise RecoveryContractDecodeError(f"{field_name} must be a JSON array")
    return value


def _strict_string(value: object, *, field_name: str) -> str:
    if type(value) is not str:
        raise RecoveryContractDecodeError(f"{field_name} must be a JSON string")
    if not _is_valid_utf8(value):
        raise RecoveryContractDecodeError(
            f"{field_name} must contain valid UTF-8 text"
        )
    return value


def _strict_integer(value: object, *, field_name: str) -> int:
    if type(value) is not int:
        raise RecoveryContractDecodeError(f"{field_name} must be a JSON integer")
    return value


def _require_constant_time_match(
    persisted: str,
    derived: str,
    *,
    field_name: str,
) -> None:
    if not hmac.compare_digest(persisted, derived):
        raise RecoveryContractDecodeError(
            f"{field_name} does not match canonical recovery authority"
        )


def _require_canonical_record(
    persisted: Mapping[str, Any],
    canonical: Mapping[str, Any],
    *,
    field_name: str,
) -> None:
    if canonical_recovery_json(persisted) != canonical_recovery_json(canonical):
        raise RecoveryContractDecodeError(f"{field_name} is not canonical")


def _required_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if not _is_valid_utf8(normalized):
        raise ValueError(f"{field_name} must contain valid UTF-8 text")
    return normalized


def _optional_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not _is_valid_utf8(normalized):
        raise ValueError(f"{field_name} must contain valid UTF-8 text")
    return normalized


def _is_valid_utf8(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _require_positive_int(value: object, *, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_nonnegative_int(value: object, *, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _require_sha256_digest(value: object, *, field_name: str) -> None:
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    if any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")


__all__ = [
    "RECOVERY_CERTIFICATE_SCHEMA",
    "RECOVERY_CERTIFICATE_VERSION",
    "RECOVERY_DELIVERY_EVENT_KIND",
    "RECOVERY_DELIVERY_EVENT_SOURCE",
    "RECOVERY_DELIVERY_SCHEMA",
    "RECOVERY_DELIVERY_VERSION",
    "RecoveryAggregateFence",
    "RecoveryCaseIdentity",
    "RecoveryCertificate",
    "RecoveryContractDecodeError",
    "RecoveryDecision",
    "RecoveryDecisionKind",
    "RecoveryDeliveryEnvelopeIdentity",
    "RecoveryDeliveryPayload",
    "RecoveryGraphEdge",
    "RecoveryGraphNode",
    "RecoveryInvariant",
    "RecoveryInvariantSeverity",
    "RecoverySubject",
    "UnsupportedRecoveryCertificateVersion",
    "UnsupportedRecoveryDeliveryVersion",
    "build_recovery_certificate",
    "canonical_recovery_digest",
    "canonical_recovery_json",
    "decode_recovery_certificate",
    "decode_recovery_delivery_payload",
    "recovery_delivery_event_id",
]
