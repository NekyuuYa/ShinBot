"""Actor-side staging materializer for frozen legacy scheduler state.

This first materializer is intentionally lossless rather than operational.  It
turns a complete source manifest into a versioned Actor-owned staging payload,
but does not insert an aggregate, schedule an effect, or publish a wake target.
A later cutover finalizer must consume this staging record atomically with the
ownership refence it proves.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from shinbot.agent.runtime.session_actor.message_ledger import (
    append_message_ledger_entry_from_payload,
)
from shinbot.core.dispatch.actor_v2_legacy_state_handoff import (
    ActorV2LegacyStateHandoffManifest,
)


class ActorV2LegacyStateSnapshotStager:
    """Preserve every v1 legacy source section in a typed Actor-side staging form."""

    @property
    def materializer_id(self) -> str:
        """Return the stable identity of the lossless staging materializer."""

        return "actor_v2.legacy_state_snapshot_stager"

    @property
    def materializer_version(self) -> int:
        """Return the semantic version of the source-to-stage interpretation."""

        return 1

    @property
    def target_schema_version(self) -> int:
        """Return the version of the staging payload consumed by a later finalizer."""

        return 1

    def materialize(
        self,
        manifest: ActorV2LegacyStateHandoffManifest,
    ) -> Mapping[str, object]:
        """Return a complete detached target staging payload.

        The payload intentionally retains the legacy sections verbatim.  The
        v2 ledger needs route-delivery identity that old scheduler rows do not
        contain, so converting rows directly into active Actor tables here
        would be semantically unsafe.
        """

        if not isinstance(manifest, ActorV2LegacyStateHandoffManifest):
            raise TypeError("manifest must be an ActorV2LegacyStateHandoffManifest")
        return {
            "schema_version": self.target_schema_version,
            "kind": "actor_v2_legacy_state_stage",
            "manifest_id": manifest.manifest_id,
            "source_digest": manifest.source_digest,
            "session_key": {
                "profile_id": manifest.key.profile_id,
                "session_id": manifest.key.session_id,
            },
            "scope": manifest.scope.to_payload(),
            "legacy_source": manifest.source_payload_as_dict(),
        }


@dataclass(slots=True)
class ActorV2LegacyIdleStatePreparationBlocked(RuntimeError):
    """Stable reasons why a source cannot become an idle Actor target seed."""

    blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        """Require a non-empty canonical blocker set without payload leakage."""

        blockers = tuple(str(blocker or "").strip() for blocker in self.blockers)
        if not blockers or any(not blocker for blocker in blockers):
            raise ValueError("legacy idle target preparation requires blocker codes")
        if len(set(blockers)) != len(blockers):
            raise ValueError("legacy idle target preparation blockers must be unique")
        object.__setattr__(self, "blockers", tuple(sorted(blockers)))
        RuntimeError.__init__(
            self,
            "legacy source cannot prepare an idle Actor target: " + ", ".join(self.blockers),
        )


class ActorV2LegacyIdleStateTargetPreparer:
    """Prepare only fully evidenced idle legacy state for a later atomic finalizer.

    The class is a pure materializer. It deliberately does not claim a mailbox,
    create an aggregate, or translate active workflow state. A finalizer can
    consume its ledger and review-plan seeds only after it has atomically
    committed the matching Actor ownership generation.
    """

    @property
    def materializer_id(self) -> str:
        """Return the stable identity of the idle-target preparation contract."""

        return "actor_v2.legacy_idle_state_target_preparer"

    @property
    def materializer_version(self) -> int:
        """Return the semantic version of the supported legacy source subset."""

        return 1

    @property
    def target_schema_version(self) -> int:
        """Return the version of the prepared idle-target staging payload."""

        return 1

    def materialize(
        self,
        manifest: ActorV2LegacyStateHandoffManifest,
    ) -> Mapping[str, object]:
        """Return deterministic idle-target seeds or stable unsupported-state codes."""

        if not isinstance(manifest, ActorV2LegacyStateHandoffManifest):
            raise TypeError("manifest must be an ActorV2LegacyStateHandoffManifest")
        source = manifest.source_payload_as_dict()
        blockers = _idle_preparation_blockers(source)
        if blockers:
            raise ActorV2LegacyIdleStatePreparationBlocked(tuple(blockers))
        scheduler = source["scheduler_state"]
        assert scheduler is None or isinstance(scheduler, dict)
        ledger_seeds = _ledger_seeds(manifest, source)
        return {
            "schema_version": self.target_schema_version,
            "kind": "actor_v2_legacy_idle_target_preparation",
            "manifest_id": manifest.manifest_id,
            "source_digest": manifest.source_digest,
            "session_key": {
                "profile_id": manifest.key.profile_id,
                "session_id": manifest.key.session_id,
            },
            "scope": manifest.scope.to_payload(),
            "review_plan_seed": _review_plan_seed(scheduler),
            "ledger_seeds": ledger_seeds,
            "high_priority_events": source["high_priority_events"],
            "recent_mentions": source["recent_mentions"],
            "review_summaries": source["review_summaries"],
            "summaries": source["summaries"],
        }


def _idle_preparation_blockers(source: Mapping[str, object]) -> list[str]:
    """Return safe coarse reasons why the v1 idle target subset is unavailable."""

    blockers: list[str] = []
    scheduler = source.get("scheduler_state")
    if scheduler is not None:
        if not isinstance(scheduler, Mapping):
            return ["legacy_scheduler_state_invalid"]
        if scheduler.get("state") != "idle":
            blockers.append("legacy_scheduler_state_not_idle")
        if scheduler.get("active_chat_state") not in ({}, None):
            blockers.append("legacy_active_chat_state_present")
        if scheduler.get("state_resume") not in ({}, None):
            blockers.append("legacy_active_reply_resume_present")
    unread_messages = source.get("unread_messages")
    route_deliveries = source.get("route_deliveries")
    if not isinstance(unread_messages, list) or not isinstance(route_deliveries, list):
        return [*blockers, "legacy_route_delivery_evidence_invalid"]
    evidence_by_message_id: dict[int, Mapping[str, object]] = {}
    for evidence in route_deliveries:
        if not isinstance(evidence, Mapping):
            blockers.append("legacy_route_delivery_evidence_invalid")
            continue
        message_log_id = evidence.get("message_log_id")
        if isinstance(message_log_id, bool) or not isinstance(message_log_id, int):
            blockers.append("legacy_route_delivery_evidence_invalid")
            continue
        if message_log_id in evidence_by_message_id:
            blockers.append("legacy_route_delivery_evidence_invalid")
            continue
        evidence_by_message_id[message_log_id] = evidence
    for unread in unread_messages:
        if not isinstance(unread, Mapping):
            blockers.append("legacy_unread_message_invalid")
            continue
        message_log_id = unread.get("message_log_id")
        if isinstance(message_log_id, bool) or not isinstance(message_log_id, int):
            blockers.append("legacy_unread_message_invalid")
            continue
        evidence = evidence_by_message_id.get(message_log_id)
        if evidence is None:
            blockers.append("legacy_route_delivery_evidence_invalid")
            continue
        status = evidence.get("status")
        if status == "missing":
            blockers.append("legacy_route_delivery_evidence_missing")
        elif status == "ambiguous":
            blockers.append("legacy_route_delivery_evidence_ambiguous")
        elif status != "verified" or not isinstance(evidence.get("mailbox_payload"), Mapping):
            blockers.append("legacy_route_delivery_evidence_invalid")
    unread_ids = {
        item.get("message_log_id")
        for item in unread_messages
        if isinstance(item, Mapping) and isinstance(item.get("message_log_id"), int)
    }
    for event in source.get("high_priority_events", []):
        if (
            isinstance(event, Mapping)
            and event.get("handled") is False
            and event.get("message_log_id") not in unread_ids
        ):
            blockers.append("legacy_high_priority_event_without_unread")
    return list(dict.fromkeys(blockers))


def _ledger_seeds(
    manifest: ActorV2LegacyStateHandoffManifest,
    source: Mapping[str, object],
) -> list[dict[str, object]]:
    """Build ownership-unbound ledger seeds from verified route deliveries."""

    unread_messages = source["unread_messages"]
    route_deliveries = source["route_deliveries"]
    assert isinstance(unread_messages, list)
    assert isinstance(route_deliveries, list)
    evidence_by_message_id = {
        int(evidence["message_log_id"]): evidence
        for evidence in route_deliveries
        if isinstance(evidence, Mapping)
    }
    seeds: list[dict[str, object]] = []
    for unread in unread_messages:
        assert isinstance(unread, Mapping)
        message_log_id = int(unread["message_log_id"])
        evidence = evidence_by_message_id[message_log_id]
        mailbox_payload = evidence["mailbox_payload"]
        assert isinstance(mailbox_payload, Mapping)
        response_profile = unread.get("response_profile", "")
        if not isinstance(response_profile, str):
            raise ActorV2LegacyIdleStatePreparationBlocked(
                ("legacy_unread_message_invalid",)
            )
        # Reuse the Actor ledger's payload validation while keeping ownership
        # generation out of this pre-ownership staging record.
        try:
            append = append_message_ledger_entry_from_payload(
                mailbox_payload,
                key=manifest.key,
                ownership_generation=1,
                source_event_id=str(mailbox_payload["event_id"]),
                event_source="legacy_state_handoff",
                occurred_at=float(mailbox_payload["observed_at"]),
                event_created_at=float(mailbox_payload["observed_at"]),
                causation_id="legacy-state-handoff:" + manifest.manifest_id,
                correlation_id="legacy-state-handoff:" + manifest.source_digest,
                trace_id=str(mailbox_payload["trace_id"]),
                response_profile=response_profile,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ActorV2LegacyIdleStatePreparationBlocked(
                ("legacy_route_delivery_payload_invalid",)
            ) from exc
        seed = append.to_record()
        seed.pop("ownership_generation")
        seeds.append(
            {
                "append": seed,
                "legacy_chat_consumed": _bool(unread.get("chat_consumed")),
                "legacy_review_consumed": _bool(unread.get("review_consumed")),
            }
        )
    return seeds


def _review_plan_seed(scheduler: dict[str, object] | None) -> dict[str, object] | None:
    """Project the legacy idle review plan without allocating an Actor plan id."""

    if scheduler is None or scheduler.get("next_review_at") is None:
        return None
    return {
        "next_review_at": scheduler["next_review_at"],
        "reason": scheduler["review_reason"],
        "mention_sensitivity": scheduler["mention_sensitivity"],
        "active_reply_threshold": scheduler["active_reply_threshold"],
        "updated_at": scheduler["updated_at"],
    }


def _bool(value: object) -> bool:
    """Return one already-validated frozen legacy flag without coercing truthiness."""

    if not isinstance(value, bool):
        raise ActorV2LegacyIdleStatePreparationBlocked(("legacy_unread_message_invalid",))
    return value


__all__ = [
    "ActorV2LegacyIdleStatePreparationBlocked",
    "ActorV2LegacyIdleStateTargetPreparer",
    "ActorV2LegacyStateSnapshotStager",
]
