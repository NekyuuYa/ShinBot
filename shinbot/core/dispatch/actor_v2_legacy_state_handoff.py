"""Versioned source-state handoff contracts for Actor v2 migration.

The legacy scheduler persists useful decisions in a schema that is not the
Actor v2 aggregate schema.  A live migration must therefore freeze the legacy
source first, capture every relevant source projection under one immutable
manifest, and let a versioned Actor-side materializer prepare a target staging
record.  These contracts deliberately do not activate an Actor or complete an
ownership transition.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from shinbot.core.dispatch.agent_identity import SessionKey

ACTOR_V2_LEGACY_STATE_HANDOFF_MANIFEST_VERSION = 1
"""The first durable source-payload shape retained for future migration."""

_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_SOURCE_PAYLOAD_KEYS = frozenset(
    {
        "schema_version",
        "scheduler_state",
        "unread_messages",
        "route_deliveries",
        "unread_ranges",
        "high_priority_events",
        "recent_mentions",
        "review_summaries",
        "summaries",
    }
)


class ActorV2LegacyStateHandoffError(RuntimeError):
    """Base error for a fail-closed legacy source-state handoff."""


class ActorV2LegacyStateHandoffConflict(ActorV2LegacyStateHandoffError):
    """Raised when a manifest or staging identity no longer matches its source."""


class ActorV2LegacyStateHandoffNotFound(ActorV2LegacyStateHandoffError):
    """Raised when a required immutable handoff record is absent."""


class ActorV2LegacyStateHandoffNotReady(ActorV2LegacyStateHandoffConflict):
    """Raised when the source boundary has not reached durable quiescence."""


class ActorV2LegacyStateHandoffScopeConflict(ActorV2LegacyStateHandoffConflict):
    """Raised when a v1 handoff cannot safely cover a shared base session."""

    def __init__(self, scope: ActorV2LegacyStateHandoffScope) -> None:
        """Expose the full profile/session scope without source payload content."""

        self.scope = scope
        members = ", ".join(
            f"{member.profile_id}:{member.session_id}" for member in scope.members
        )
        super().__init__(
            "legacy source-state handoff requires a resolved base-session scope: "
            + members
        )


class ActorV2LegacyStateHandoffSourceInvalid(ActorV2LegacyStateHandoffError):
    """Raised when legacy durable state cannot become canonical manifest data."""


@dataclass(slots=True, frozen=True)
class ActorV2LegacyStateHandoffScope:
    """The complete ownership scope sharing one legacy base-session projection."""

    legacy_session_id: str
    members: tuple[SessionKey, ...]

    def __post_init__(self) -> None:
        """Canonicalize the base session and its profile-scoped owners."""

        legacy_session_id = _identifier(self.legacy_session_id, "legacy_session_id")
        members = tuple(self.members)
        if not members or any(not isinstance(member, SessionKey) for member in members):
            raise ValueError("legacy handoff scope requires typed owner members")
        if len(set(members)) != len(members):
            raise ValueError("legacy handoff scope cannot repeat an owner member")
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "members", tuple(sorted(members)))

    @property
    def is_single_owner(self) -> bool:
        """Return whether the v1 one-owner materialization rule is satisfied."""

        return len(self.members) == 1

    def to_payload(self) -> dict[str, object]:
        """Return a canonical JSON-compatible scope representation."""

        return {
            "legacy_session_id": self.legacy_session_id,
            "members": [
                {
                    "profile_id": member.profile_id,
                    "session_id": member.session_id,
                }
                for member in self.members
            ],
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> ActorV2LegacyStateHandoffScope:
        """Decode one persisted canonical scope without accepting ambiguous keys."""

        if not isinstance(payload, Mapping):
            raise TypeError("legacy handoff scope payload must be an object")
        if set(payload) != {"legacy_session_id", "members"}:
            raise ValueError("legacy handoff scope payload has unexpected fields")
        members_value = payload.get("members")
        if isinstance(members_value, (str, bytes)) or not isinstance(
            members_value,
            Sequence,
        ):
            raise TypeError("legacy handoff scope members must be an array")
        members: list[SessionKey] = []
        for item in members_value:
            if not isinstance(item, Mapping) or set(item) != {"profile_id", "session_id"}:
                raise ValueError("legacy handoff scope member is malformed")
            members.append(
                SessionKey(
                    profile_id=_identifier(item["profile_id"], "profile_id"),
                    session_id=_identifier(item["session_id"], "session_id"),
                )
            )
        return cls(
            legacy_session_id=_identifier(
                payload.get("legacy_session_id"),
                "legacy_session_id",
            ),
            members=tuple(members),
        )


@dataclass(slots=True, frozen=True)
class ActorV2LegacyStateHandoffManifest:
    """Immutable, complete legacy source snapshot bound to one core drain."""

    manifest_id: str
    barrier_id: str
    core_ingress_drain_request_id: str
    key: SessionKey
    scope: ActorV2LegacyStateHandoffScope
    legacy_session_id: str
    source_generation: int
    migration_generation: int
    manifest_version: int
    source_payload: Mapping[str, object]
    core_ingress_digest: str
    legacy_quiescence_digest: str
    source_digest: str
    captured_at: float

    def __post_init__(self) -> None:
        """Validate source identity, canonical payload, and its exact digest."""

        if not isinstance(self.key, SessionKey):
            raise TypeError("legacy handoff manifest key must be a SessionKey")
        if not isinstance(self.scope, ActorV2LegacyStateHandoffScope):
            raise TypeError("legacy handoff manifest scope must be typed")
        manifest_id = _identifier(self.manifest_id, "manifest_id")
        barrier_id = _identifier(self.barrier_id, "barrier_id")
        request_id = _identifier(
            self.core_ingress_drain_request_id,
            "core_ingress_drain_request_id",
        )
        legacy_session_id = _identifier(self.legacy_session_id, "legacy_session_id")
        source_generation = _positive_integer(self.source_generation, "source_generation")
        migration_generation = _positive_integer(
            self.migration_generation,
            "migration_generation",
        )
        manifest_version = _positive_integer(self.manifest_version, "manifest_version")
        if migration_generation != source_generation + 1:
            raise ValueError("legacy handoff manifest must bind the next migration generation")
        if manifest_version != ACTOR_V2_LEGACY_STATE_HANDOFF_MANIFEST_VERSION:
            raise ValueError("unsupported legacy handoff manifest version")
        if self.scope.legacy_session_id != legacy_session_id:
            raise ValueError("legacy handoff scope belongs to another base session")
        if self.key not in self.scope.members:
            raise ValueError("legacy handoff scope must include the manifest key")
        source_payload = _freeze_source_payload(self.source_payload, manifest_version)
        core_ingress_digest = _digest(self.core_ingress_digest, "core_ingress_digest")
        legacy_quiescence_digest = _digest(
            self.legacy_quiescence_digest,
            "legacy_quiescence_digest",
        )
        source_digest = _digest(self.source_digest, "source_digest")
        captured_at = _finite_time(self.captured_at, "captured_at")
        expected_digest = self.compute_source_digest(
            barrier_id=barrier_id,
            core_ingress_drain_request_id=request_id,
            key=self.key,
            scope=self.scope,
            legacy_session_id=legacy_session_id,
            source_generation=source_generation,
            migration_generation=migration_generation,
            manifest_version=manifest_version,
            source_payload=source_payload,
            core_ingress_digest=core_ingress_digest,
            legacy_quiescence_digest=legacy_quiescence_digest,
        )
        if source_digest != expected_digest:
            raise ValueError("legacy handoff source digest does not match manifest content")
        object.__setattr__(self, "manifest_id", manifest_id)
        object.__setattr__(self, "barrier_id", barrier_id)
        object.__setattr__(self, "core_ingress_drain_request_id", request_id)
        object.__setattr__(self, "legacy_session_id", legacy_session_id)
        object.__setattr__(self, "source_generation", source_generation)
        object.__setattr__(self, "migration_generation", migration_generation)
        object.__setattr__(self, "manifest_version", manifest_version)
        object.__setattr__(self, "source_payload", source_payload)
        object.__setattr__(self, "core_ingress_digest", core_ingress_digest)
        object.__setattr__(self, "legacy_quiescence_digest", legacy_quiescence_digest)
        object.__setattr__(self, "source_digest", source_digest)
        object.__setattr__(self, "captured_at", captured_at)

    @classmethod
    def create(
        cls,
        *,
        manifest_id: str,
        barrier_id: str,
        core_ingress_drain_request_id: str,
        key: SessionKey,
        scope: ActorV2LegacyStateHandoffScope,
        legacy_session_id: str,
        source_generation: int,
        migration_generation: int,
        source_payload: Mapping[str, object],
        core_ingress_digest: str,
        legacy_quiescence_digest: str,
        captured_at: float,
    ) -> ActorV2LegacyStateHandoffManifest:
        """Create a v1 manifest and derive its canonical source digest."""

        manifest_version = ACTOR_V2_LEGACY_STATE_HANDOFF_MANIFEST_VERSION
        frozen_payload = _freeze_source_payload(source_payload, manifest_version)
        return cls(
            manifest_id=manifest_id,
            barrier_id=barrier_id,
            core_ingress_drain_request_id=core_ingress_drain_request_id,
            key=key,
            scope=scope,
            legacy_session_id=legacy_session_id,
            source_generation=source_generation,
            migration_generation=migration_generation,
            manifest_version=manifest_version,
            source_payload=frozen_payload,
            core_ingress_digest=core_ingress_digest,
            legacy_quiescence_digest=legacy_quiescence_digest,
            source_digest=cls.compute_source_digest(
                barrier_id=barrier_id,
                core_ingress_drain_request_id=core_ingress_drain_request_id,
                key=key,
                scope=scope,
                legacy_session_id=legacy_session_id,
                source_generation=source_generation,
                migration_generation=migration_generation,
                manifest_version=manifest_version,
                source_payload=frozen_payload,
                core_ingress_digest=core_ingress_digest,
                legacy_quiescence_digest=legacy_quiescence_digest,
            ),
            captured_at=captured_at,
        )

    @staticmethod
    def compute_source_digest(
        *,
        barrier_id: str,
        core_ingress_drain_request_id: str,
        key: SessionKey,
        scope: ActorV2LegacyStateHandoffScope,
        legacy_session_id: str,
        source_generation: int,
        migration_generation: int,
        manifest_version: int,
        source_payload: Mapping[str, object],
        core_ingress_digest: str,
        legacy_quiescence_digest: str,
    ) -> str:
        """Hash every source fact and boundary proof without capability tokens."""

        payload = {
            "barrier_id": _identifier(barrier_id, "barrier_id"),
            "core_ingress_digest": _digest(core_ingress_digest, "core_ingress_digest"),
            "core_ingress_drain_request_id": _identifier(
                core_ingress_drain_request_id,
                "core_ingress_drain_request_id",
            ),
            "legacy_quiescence_digest": _digest(
                legacy_quiescence_digest,
                "legacy_quiescence_digest",
            ),
            "legacy_session_id": _identifier(legacy_session_id, "legacy_session_id"),
            "manifest_version": _positive_integer(manifest_version, "manifest_version"),
            "migration_generation": _positive_integer(
                migration_generation,
                "migration_generation",
            ),
            "profile_id": key.profile_id,
            "scope": scope.to_payload(),
            "session_id": key.session_id,
            "source_generation": _positive_integer(source_generation, "source_generation"),
            "source_payload": _thaw_json(source_payload),
        }
        return _sha256(payload)

    def source_payload_as_dict(self) -> dict[str, object]:
        """Return a detached source payload for a pure materializer invocation."""

        value = _thaw_json(self.source_payload)
        assert isinstance(value, dict)
        return value


@dataclass(slots=True, frozen=True)
class ActorV2LegacyStateHandoffMaterialization:
    """Immutable staged Actor-side interpretation of one source manifest."""

    manifest_id: str
    materializer_id: str
    materializer_version: int
    target_schema_version: int
    source_digest: str
    target_payload: Mapping[str, object]
    target_digest: str
    materialized_at: float

    def __post_init__(self) -> None:
        """Validate a deterministic target staging record without activating it."""

        manifest_id = _identifier(self.manifest_id, "manifest_id")
        materializer_id = _identifier(self.materializer_id, "materializer_id")
        materializer_version = _positive_integer(
            self.materializer_version,
            "materializer_version",
        )
        target_schema_version = _positive_integer(
            self.target_schema_version,
            "target_schema_version",
        )
        source_digest = _digest(self.source_digest, "source_digest")
        target_payload = _freeze_json_object(self.target_payload, "target_payload")
        target_digest = _digest(self.target_digest, "target_digest")
        materialized_at = _finite_time(self.materialized_at, "materialized_at")
        expected_digest = self.compute_target_digest(
            manifest_id=manifest_id,
            materializer_id=materializer_id,
            materializer_version=materializer_version,
            target_schema_version=target_schema_version,
            source_digest=source_digest,
            target_payload=target_payload,
        )
        if target_digest != expected_digest:
            raise ValueError("legacy handoff target digest does not match staging content")
        object.__setattr__(self, "manifest_id", manifest_id)
        object.__setattr__(self, "materializer_id", materializer_id)
        object.__setattr__(self, "materializer_version", materializer_version)
        object.__setattr__(self, "target_schema_version", target_schema_version)
        object.__setattr__(self, "source_digest", source_digest)
        object.__setattr__(self, "target_payload", target_payload)
        object.__setattr__(self, "target_digest", target_digest)
        object.__setattr__(self, "materialized_at", materialized_at)

    @classmethod
    def create(
        cls,
        *,
        manifest_id: str,
        materializer_id: str,
        materializer_version: int,
        target_schema_version: int,
        source_digest: str,
        target_payload: Mapping[str, object],
        materialized_at: float,
    ) -> ActorV2LegacyStateHandoffMaterialization:
        """Create one immutable staging record from a pure materializer output."""

        frozen_payload = _freeze_json_object(target_payload, "target_payload")
        return cls(
            manifest_id=manifest_id,
            materializer_id=materializer_id,
            materializer_version=materializer_version,
            target_schema_version=target_schema_version,
            source_digest=source_digest,
            target_payload=frozen_payload,
            target_digest=cls.compute_target_digest(
                manifest_id=manifest_id,
                materializer_id=materializer_id,
                materializer_version=materializer_version,
                target_schema_version=target_schema_version,
                source_digest=source_digest,
                target_payload=frozen_payload,
            ),
            materialized_at=materialized_at,
        )

    @staticmethod
    def compute_target_digest(
        *,
        manifest_id: str,
        materializer_id: str,
        materializer_version: int,
        target_schema_version: int,
        source_digest: str,
        target_payload: Mapping[str, object],
    ) -> str:
        """Hash one target interpretation against its exact immutable source."""

        return _sha256(
            {
                "manifest_id": _identifier(manifest_id, "manifest_id"),
                "materializer_id": _identifier(materializer_id, "materializer_id"),
                "materializer_version": _positive_integer(
                    materializer_version,
                    "materializer_version",
                ),
                "source_digest": _digest(source_digest, "source_digest"),
                "target_payload": _thaw_json(target_payload),
                "target_schema_version": _positive_integer(
                    target_schema_version,
                    "target_schema_version",
                ),
            }
        )

    def target_payload_as_dict(self) -> dict[str, object]:
        """Return a detached target staging payload for a later activation controller."""

        value = _thaw_json(self.target_payload)
        assert isinstance(value, dict)
        return value


class ActorV2LegacyStateHandoffMaterializer(Protocol):
    """Pure Actor-side transformer from immutable source to staged target state."""

    @property
    def materializer_id(self) -> str:
        """Return a stable materializer implementation identity."""

        ...

    @property
    def materializer_version(self) -> int:
        """Return the positive semantic version of this transformation."""

        ...

    @property
    def target_schema_version(self) -> int:
        """Return the positive staged-target payload schema version."""

        ...

    def materialize(
        self,
        manifest: ActorV2LegacyStateHandoffManifest,
    ) -> Mapping[str, object]:
        """Return a deterministic staged target payload without side effects."""

        ...


def _freeze_source_payload(
    value: Mapping[str, object],
    manifest_version: int,
) -> Mapping[str, object]:
    """Validate the complete v1 source shape before it becomes immutable."""

    payload = _freeze_json_object(value, "source_payload")
    payload_keys = frozenset(payload)
    if payload_keys != _SOURCE_PAYLOAD_KEYS:
        raise ValueError("legacy handoff source payload has unexpected sections")
    if payload["schema_version"] != manifest_version:
        raise ValueError("legacy handoff source payload schema version does not match manifest")
    scheduler_state = payload["scheduler_state"]
    if scheduler_state is not None and not isinstance(scheduler_state, Mapping):
        raise TypeError("legacy handoff scheduler_state must be an object or null")
    for section in (
        "unread_messages",
        "route_deliveries",
        "unread_ranges",
        "high_priority_events",
        "recent_mentions",
        "review_summaries",
        "summaries",
    ):
        rows = payload[section]
        if not isinstance(rows, tuple) or any(not isinstance(row, Mapping) for row in rows):
            raise TypeError(f"legacy handoff {section} must be an array of objects")
    _validate_route_delivery_coverage(
        unread_messages=payload["unread_messages"],
        route_deliveries=payload["route_deliveries"],
    )
    return payload


def _validate_route_delivery_coverage(
    *,
    unread_messages: object,
    route_deliveries: object,
) -> None:
    """Require one explicit delivery-evidence outcome per legacy unread row."""

    assert isinstance(unread_messages, tuple)
    assert isinstance(route_deliveries, tuple)
    unread_ids: list[int] = []
    for row in unread_messages:
        assert isinstance(row, Mapping)
        unread_ids.append(_positive_integer(row.get("message_log_id"), "message_log_id"))
    if len(set(unread_ids)) != len(unread_ids):
        raise ValueError("legacy handoff unread messages cannot repeat a message_log_id")
    evidence_ids: list[int] = []
    for row in route_deliveries:
        assert isinstance(row, Mapping)
        message_log_id = _positive_integer(
            row.get("message_log_id"),
            "route_delivery.message_log_id",
        )
        status = _identifier(row.get("status"), "route_delivery.status")
        evidence_ids.append(message_log_id)
        if status == "verified":
            if set(row) != {"message_log_id", "status", "mailbox_payload"}:
                raise ValueError("verified route delivery evidence has unexpected fields")
            if not isinstance(row.get("mailbox_payload"), Mapping):
                raise TypeError("verified route delivery evidence requires a mailbox payload")
        elif status == "missing":
            if set(row) != {"message_log_id", "status"}:
                raise ValueError("missing route delivery evidence has unexpected fields")
        elif status == "ambiguous":
            event_ids = row.get("event_ids")
            if (
                set(row) != {"message_log_id", "status", "event_ids"}
                or not isinstance(event_ids, tuple)
                or not event_ids
                or any(not isinstance(event_id, str) or not event_id for event_id in event_ids)
                or len(set(event_ids)) != len(event_ids)
            ):
                raise ValueError("ambiguous route delivery evidence is malformed")
        else:
            raise ValueError("unsupported route delivery evidence status")
    if len(set(evidence_ids)) != len(evidence_ids) or tuple(evidence_ids) != tuple(unread_ids):
        raise ValueError("legacy handoff route delivery evidence must exactly cover unread messages")


def _freeze_json_object(value: Mapping[str, object], field_name: str) -> Mapping[str, object]:
    """Return one recursively immutable, canonicalizable JSON object."""

    if not isinstance(value, Mapping):
        raise TypeError(f"legacy handoff {field_name} must be an object")
    return _freeze_json(value, field_name=field_name, require_object=True)


def _freeze_json(
    value: object,
    *,
    field_name: str,
    require_object: bool = False,
) -> Any:
    """Normalize a bounded JSON tree into immutable mappings and tuples."""

    if isinstance(value, Mapping):
        items: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"legacy handoff {field_name} keys must be strings")
            items[key] = _freeze_json(item, field_name=field_name)
        frozen: Any = MappingProxyType(dict(sorted(items.items())))
    elif isinstance(value, (list, tuple)):
        frozen = tuple(_freeze_json(item, field_name=field_name) for item in value)
    elif value is None or isinstance(value, (str, int, bool)):
        frozen = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"legacy handoff {field_name} numbers must be finite")
        frozen = value
    else:
        raise TypeError(f"legacy handoff {field_name} must contain JSON-compatible values")
    if require_object and not isinstance(frozen, Mapping):
        raise TypeError(f"legacy handoff {field_name} must be an object")
    try:
        encoded = _canonical_json(_thaw_json(frozen))
    except (RecursionError, TypeError, ValueError, UnicodeEncodeError) as exc:
        raise ValueError(f"legacy handoff {field_name} is not canonical JSON") from exc
    if len(encoded.encode("utf-8")) > 1_048_576:
        raise ValueError(f"legacy handoff {field_name} exceeds the JSON size limit")
    return frozen


def _thaw_json(value: object) -> object:
    """Convert an immutable JSON tree into plain JSON-compatible containers."""

    if isinstance(value, Mapping):
        return {str(key): _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json(value: object) -> str:
    """Encode one deterministic JSON representation for hash computation."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _sha256(value: object) -> str:
    """Return the canonical SHA-256 hex digest for one durable payload."""

    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _identifier(value: object, field_name: str) -> str:
    """Normalize one required durable identity."""

    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"legacy handoff {field_name} must not be empty")
    return normalized


def _positive_integer(value: object, field_name: str) -> int:
    """Require one positive non-boolean version or ownership generation."""

    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"legacy handoff {field_name} must be positive")
    return value


def _finite_time(value: object, field_name: str) -> float:
    """Normalize one finite durable timestamp."""

    if isinstance(value, bool):
        raise ValueError(f"legacy handoff {field_name} must be finite")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"legacy handoff {field_name} must be finite")
    return normalized


def _digest(value: object, field_name: str) -> str:
    """Require a lowercase SHA-256 digest instead of raw local evidence."""

    normalized = str(value or "").strip().lower()
    if _DIGEST_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"legacy handoff {field_name} must be a SHA-256 digest")
    return normalized


__all__ = [
    "ACTOR_V2_LEGACY_STATE_HANDOFF_MANIFEST_VERSION",
    "ActorV2LegacyStateHandoffConflict",
    "ActorV2LegacyStateHandoffError",
    "ActorV2LegacyStateHandoffManifest",
    "ActorV2LegacyStateHandoffMaterialization",
    "ActorV2LegacyStateHandoffMaterializer",
    "ActorV2LegacyStateHandoffNotFound",
    "ActorV2LegacyStateHandoffNotReady",
    "ActorV2LegacyStateHandoffScope",
    "ActorV2LegacyStateHandoffScopeConflict",
    "ActorV2LegacyStateHandoffSourceInvalid",
]
