"""Read-only durable context projection for Actor v2 review effects.

The legacy review coordinator builds prompt windows from mutable scheduler
state and broad message-store queries. Actor v2 must instead derive every
model-visible message from the exact unread ledger snapshot captured by the
claimed effect. This projector provides that narrow, fail-closed boundary.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from shinbot.agent.coordinators.review.stores import MessageLogPayload
from shinbot.agent.runtime.session_actor.message_ledger import MessageLedgerEntry
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ReviewWorkflowRequest,
)
from shinbot.agent.services.context.review_context_builder import (
    ReviewContextBuilder,
    ReviewContextBuilderAdapter,
    ReviewContextBuildOptions,
    ReviewStageInput,
)


class ActorReviewWorkflowContextError(RuntimeError):
    """Raised when an Actor v2 review context cannot be projected safely."""


class ActorReviewWorkflowMessageStore(Protocol):
    """Load immutable logs only after the actor has authorized their IDs."""

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[MessageLogPayload]:
        """Return existing message logs for the supplied identifiers."""


class ActorReviewWorkflowContextProjector:
    """Build one reply-decision input from an effect's captured ledger entries.

    The projector intentionally receives no scheduler, ledger-store, timeline,
    or summary dependency. The effect adapter already captured the immutable
    unread rows; allowing a second query surface here would let a model observe
    messages that the actor did not authorize for this operation.
    """

    def __init__(
        self,
        *,
        message_store: ActorReviewWorkflowMessageStore,
        context_builder: ReviewContextBuilder | None = None,
    ) -> None:
        self._message_store = message_store
        self._context_builder = context_builder or ReviewContextBuilderAdapter()

    async def build_review_stage_input(
        self,
        request: ReviewWorkflowRequest,
    ) -> ReviewStageInput:
        """Project the exact durable input for one Actor v2 reply decision."""

        if not isinstance(request, ReviewWorkflowRequest):
            raise TypeError("request must be ReviewWorkflowRequest")
        entries = self._validated_entries(request)
        messages = self._load_messages(entries)
        effect = request.effect
        message_log_ids = tuple(entry.message_log_id for entry in entries)
        options = ReviewContextBuildOptions(
            instance_id=effect.instance_id,
            metadata={
                "actor_v2": True,
                "operation_id": effect.operation_id,
                "effect_id": effect.effect_id,
                "plan_id": request.plan_id,
                "plan_revision": request.plan_revision,
                "ownership_generation": effect.ownership_generation,
                "input_watermark": effect.input_watermark,
                "input_ledger_sequence": effect.input_ledger_sequence,
                "target_session_id": effect.target_session_id,
                "ledger_message_log_ids": list(message_log_ids),
                "candidate_message_ids": list(message_log_ids),
            },
        )
        return self._context_builder.build_for_messages(
            session_id=effect.key.session_id,
            messages=messages,
            purpose="reply_decision",
            options=options,
        )

    @staticmethod
    def _validated_entries(
        request: ReviewWorkflowRequest,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Revalidate every model-visible row before reading message content."""

        effect = request.effect
        entries = tuple(effect.ledger_entries)
        previous_sequence = 0
        message_ids: set[int] = set()
        for entry in entries:
            if not isinstance(entry, MessageLedgerEntry):
                raise ActorReviewWorkflowContextError(
                    "actor review effect contains an invalid ledger entry"
                )
            if entry.key != effect.key:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry belongs to another session"
                )
            if entry.message.ownership_generation != effect.ownership_generation:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry belongs to another ownership generation"
                )
            if entry.ledger_sequence <= previous_sequence:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entries are not strictly ordered"
                )
            previous_sequence = entry.ledger_sequence
            if entry.ledger_sequence > effect.input_ledger_sequence:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry exceeds the sequence fence"
                )
            if entry.message_log_id > effect.input_watermark:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry exceeds the watermark fence"
                )
            if entry.message_log_id in message_ids:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger contains a duplicate message log id"
                )
            message_ids.add(entry.message_log_id)
            if not entry.is_unread:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry is no longer unread"
                )
            if (
                not entry.message.eligible_for_work
                or entry.message.suppression_reason
            ):
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry is suppressed"
                )
            if entry.message.instance_id != effect.instance_id:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry belongs to another instance"
                )
            if entry.message.base_session_id != effect.target_session_id:
                raise ActorReviewWorkflowContextError(
                    "actor review ledger entry has a different transport session"
                )
        return entries

    def _load_messages(
        self,
        entries: tuple[MessageLedgerEntry, ...],
    ) -> list[MessageLogPayload]:
        """Reload exact message-log records and restore durable ledger order."""

        message_log_ids = tuple(entry.message_log_id for entry in entries)
        if not message_log_ids:
            return []
        expected_session_ids = {
            entry.message_log_id: entry.message.base_session_id for entry in entries
        }
        payloads = self._message_store.list_by_ids(message_log_ids)
        by_id: dict[int, MessageLogPayload] = {}
        expected_ids = set(message_log_ids)
        for payload in payloads:
            if not isinstance(payload, Mapping):
                raise ActorReviewWorkflowContextError(
                    "actor review message store returned an invalid payload"
                )
            message_log_id = payload.get("id")
            if (
                isinstance(message_log_id, bool)
                or not isinstance(message_log_id, int)
                or message_log_id not in expected_ids
            ):
                raise ActorReviewWorkflowContextError(
                    "actor review message store returned an unexpected message"
                )
            if message_log_id in by_id:
                raise ActorReviewWorkflowContextError(
                    "actor review message store returned a duplicate message"
                )
            session_id = payload.get("session_id")
            if (
                not isinstance(session_id, str)
                or session_id != expected_session_ids[message_log_id]
            ):
                raise ActorReviewWorkflowContextError(
                    "actor review message log session mismatch: "
                    + str(message_log_id)
                )
            by_id[message_log_id] = dict(payload)
        missing = [
            message_log_id
            for message_log_id in message_log_ids
            if message_log_id not in by_id
        ]
        if missing:
            raise ActorReviewWorkflowContextError(
                "actor review message logs disappeared: "
                + ", ".join(str(message_log_id) for message_log_id in missing)
            )
        return [by_id[message_log_id] for message_log_id in message_log_ids]


__all__ = [
    "ActorReviewWorkflowContextError",
    "ActorReviewWorkflowContextProjector",
    "ActorReviewWorkflowMessageStore",
]
