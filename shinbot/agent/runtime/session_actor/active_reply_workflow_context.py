"""Read-only durable context projection for Actor v2 active-reply effects.

Active replies may be triggered by an individual high-priority message while
the claimed operation snapshot contains several unread ledger rows.  This
projector therefore treats the request's selected IDs as an additional
authorization fence: no unselected captured row can become model-visible.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

from shinbot.agent.coordinators.review.stores import MessageLogPayload
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.message_ledger import MessageLedgerEntry
from shinbot.agent.runtime.session_actor.workflow_adapters import (
    ActiveReplyWorkflowRequest,
    ActorWorkflowEffectInput,
)
from shinbot.agent.services.context.review_context_builder import (
    ReviewContextBuilder,
    ReviewContextBuilderAdapter,
    ReviewContextBuildOptions,
    ReviewStageInput,
)


class ActorActiveReplyWorkflowContextError(RuntimeError):
    """Raised when an Actor v2 active-reply context cannot be projected safely."""


class ActorActiveReplyWorkflowMessageStore(Protocol):
    """Load immutable logs only after an active-reply request authorizes them."""

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[MessageLogPayload]:
        """Return existing message logs for the supplied identifiers."""


class ActorActiveReplyWorkflowContextProjector:
    """Build one reply-decision input from an exact active-reply selection.

    The effect adapter captures immutable ledger rows before constructing the
    request.  This projector intentionally has no scheduler, ledger-store,
    timeline, or summary dependency.  Its only message-store read is by the
    exact IDs jointly authorized by the captured rows and request selection.
    """

    def __init__(
        self,
        *,
        message_store: ActorActiveReplyWorkflowMessageStore,
        context_builder: ReviewContextBuilder | None = None,
    ) -> None:
        self._message_store = message_store
        self._context_builder = context_builder or ReviewContextBuilderAdapter()

    async def build_active_reply_stage_input(
        self,
        request: ActiveReplyWorkflowRequest,
    ) -> ReviewStageInput:
        """Project the exact durable input for one Actor v2 active reply."""

        if not isinstance(request, ActiveReplyWorkflowRequest):
            raise TypeError("request must be ActiveReplyWorkflowRequest")
        effect = self._validated_effect(request)
        entries = self._validated_selected_entries(request, effect=effect)
        messages = self._load_messages(entries)
        message_log_ids = tuple(entry.message_log_id for entry in entries)
        options = ReviewContextBuildOptions(
            instance_id=effect.instance_id,
            metadata={
                "actor_v2": True,
                "operation_id": effect.operation_id,
                "effect_id": effect.effect_id,
                "ownership_generation": effect.ownership_generation,
                "input_watermark": effect.input_watermark,
                "input_ledger_sequence": effect.input_ledger_sequence,
                "target_session_id": effect.target_session_id,
                "ledger_message_log_ids": list(message_log_ids),
                "candidate_message_ids": list(message_log_ids),
                "response_profile": request.response_profile,
                "sender_id": request.sender_id,
            },
        )
        return self._context_builder.build_for_messages(
            session_id=effect.key.session_id,
            messages=messages,
            purpose="reply_decision",
            options=options,
        )

    @staticmethod
    def _validated_effect(
        request: ActiveReplyWorkflowRequest,
    ) -> ActorWorkflowEffectInput:
        """Revalidate mutable-boundary fields before projecting any input."""

        effect = request.effect
        if not isinstance(effect, ActorWorkflowEffectInput):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply request contains an invalid effect"
            )
        if not isinstance(effect.key, SessionKey):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply effect contains an invalid session key"
            )
        if (
            isinstance(effect.ownership_generation, bool)
            or not isinstance(effect.ownership_generation, int)
            or effect.ownership_generation < 1
        ):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply effect contains an invalid ownership generation"
            )
        for field_name in ("input_watermark", "input_ledger_sequence"):
            value = getattr(effect, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply effect contains an invalid " + field_name
                )
        if not isinstance(effect.instance_id, str) or not effect.instance_id:
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply effect contains an invalid instance id"
            )
        if (
            not isinstance(effect.target_session_id, str)
            or not effect.target_session_id
            or not effect.target_session_id.startswith(f"{effect.instance_id}:")
        ):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply effect contains an invalid transport session"
            )
        return effect

    @staticmethod
    def _validated_selected_entries(
        request: ActiveReplyWorkflowRequest,
        *,
        effect: ActorWorkflowEffectInput,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Revalidate the request-selected ledger rows before reading content."""

        requested_message_log_ids = _validated_requested_message_log_ids(request)
        if not isinstance(effect.ledger_entries, tuple):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply effect contains an invalid ledger snapshot"
            )
        entries = effect.ledger_entries
        previous_sequence = 0
        entry_message_log_ids: list[int] = []
        seen_message_log_ids: set[int] = set()
        for entry in entries:
            if not isinstance(entry, MessageLedgerEntry):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply effect contains an invalid ledger entry"
                )
            if entry.key != effect.key:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry belongs to another session"
                )
            if entry.message.ownership_generation != effect.ownership_generation:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry belongs to another ownership generation"
                )
            if (
                isinstance(entry.ledger_sequence, bool)
                or not isinstance(entry.ledger_sequence, int)
                or entry.ledger_sequence < 1
            ):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry has an invalid sequence"
                )
            if entry.ledger_sequence <= previous_sequence:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entries are not strictly ordered"
                )
            previous_sequence = entry.ledger_sequence
            if entry.ledger_sequence > effect.input_ledger_sequence:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry exceeds the sequence fence"
                )
            if (
                isinstance(entry.message_log_id, bool)
                or not isinstance(entry.message_log_id, int)
                or entry.message_log_id < 1
            ):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry has an invalid message log id"
                )
            if entry.message_log_id > effect.input_watermark:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry exceeds the watermark fence"
                )
            if entry.message_log_id in seen_message_log_ids:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger contains a duplicate message log id"
                )
            seen_message_log_ids.add(entry.message_log_id)
            entry_message_log_ids.append(entry.message_log_id)
            if (
                not entry.message.eligible_for_work
                or entry.message.suppression_reason
            ):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry is suppressed"
                )
            if not entry.is_unread:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry is no longer unread"
                )
            if entry.message.instance_id != effect.instance_id:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry belongs to another instance"
                )
            if (
                not entry.message.base_session_id
                or entry.message.base_session_id != effect.target_session_id
            ):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply ledger entry has a different transport session"
                )
        if set(requested_message_log_ids) != seen_message_log_ids or len(
            requested_message_log_ids
        ) != len(entry_message_log_ids):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply request message ids do not match the selected "
                "ledger entries"
            )
        return entries

    def _load_messages(
        self,
        entries: tuple[MessageLedgerEntry, ...],
    ) -> list[MessageLogPayload]:
        """Reload exact message logs and restore durable ledger order."""

        message_log_ids = tuple(entry.message_log_id for entry in entries)
        if not message_log_ids:
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply requires at least one selected message"
            )
        expected_session_ids = {
            entry.message_log_id: entry.message.base_session_id for entry in entries
        }
        payloads = self._message_store.list_by_ids(message_log_ids)
        if not isinstance(payloads, list):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply message store returned an invalid payload list"
            )
        by_id: dict[int, MessageLogPayload] = {}
        expected_ids = set(message_log_ids)
        for payload in payloads:
            if not isinstance(payload, Mapping):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply message store returned an invalid payload"
                )
            message_log_id = payload.get("id")
            if (
                isinstance(message_log_id, bool)
                or not isinstance(message_log_id, int)
                or message_log_id not in expected_ids
            ):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply message store returned an unexpected message"
                )
            if message_log_id in by_id:
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply message store returned a duplicate message"
                )
            session_id = payload.get("session_id")
            if (
                not isinstance(session_id, str)
                or session_id != expected_session_ids[message_log_id]
            ):
                raise ActorActiveReplyWorkflowContextError(
                    "actor active reply message log session mismatch: "
                    + str(message_log_id)
                )
            by_id[message_log_id] = dict(payload)
        missing = [
            message_log_id
            for message_log_id in message_log_ids
            if message_log_id not in by_id
        ]
        if missing:
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply message logs disappeared: "
                + ", ".join(str(message_log_id) for message_log_id in missing)
            )
        return [by_id[message_log_id] for message_log_id in message_log_ids]


def _validated_requested_message_log_ids(
    request: ActiveReplyWorkflowRequest,
) -> tuple[int, ...]:
    """Return a non-empty duplicate-free request selection or fail closed."""

    message_log_ids = request.message_log_ids
    if not isinstance(message_log_ids, tuple):
        raise ActorActiveReplyWorkflowContextError(
            "actor active reply request message ids must be a tuple"
        )
    if not message_log_ids:
        raise ActorActiveReplyWorkflowContextError(
            "actor active reply request must select at least one message"
        )
    seen_message_log_ids: set[int] = set()
    for message_log_id in message_log_ids:
        if (
            isinstance(message_log_id, bool)
            or not isinstance(message_log_id, int)
            or message_log_id < 1
        ):
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply request contains an invalid message log id"
            )
        if message_log_id in seen_message_log_ids:
            raise ActorActiveReplyWorkflowContextError(
                "actor active reply request contains a duplicate message log id"
            )
        seen_message_log_ids.add(message_log_id)
    if not isinstance(request.response_profile, str) or not isinstance(
        request.sender_id,
        str,
    ):
        raise ActorActiveReplyWorkflowContextError(
            "actor active reply request contains invalid response metadata"
        )
    return message_log_ids


__all__ = [
    "ActorActiveReplyWorkflowContextError",
    "ActorActiveReplyWorkflowContextProjector",
    "ActorActiveReplyWorkflowMessageStore",
]
