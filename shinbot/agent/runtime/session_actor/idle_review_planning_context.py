"""Read-only durable context projection for Actor v2 idle-review planning."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from shinbot.agent.coordinators.review.stores import MessageLogPayload
from shinbot.agent.runtime.session_actor.aggregate import SessionKey
from shinbot.agent.runtime.session_actor.idle_review_planning_adapter import (
    IdleReviewPlanningWorkflowRequest,
)
from shinbot.agent.runtime.session_actor.message_ledger import MessageLedgerEntry
from shinbot.agent.services.context.review_context_builder import (
    ReviewContextBuilder,
    ReviewContextBuilderAdapter,
    ReviewContextBuildOptions,
    ReviewStageInput,
)


class IdleReviewPlanningContextError(RuntimeError):
    """Raised when durable planner context cannot be projected coherently."""


class ActorPlanningLedgerPort(Protocol):
    """Read actor-owned ledger entries without touching scheduler state."""

    async def list_message_ledger(
        self,
        key: SessionKey,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Return one actor ledger in durable sequence order."""


class ActorPlanningMessageStore(Protocol):
    """Load immutable message-log payloads for already-authorized ledger IDs."""

    def list_by_ids(self, message_log_ids: Sequence[int]) -> list[MessageLogPayload]:
        """Return existing message logs for the supplied identifiers."""


@dataclass(slots=True, frozen=True)
class IdleReviewPlanningContextConfig:
    """Bounded context settings for one Actor v2 planner projection."""

    max_messages: int = 64

    def __post_init__(self) -> None:
        """Reject nonsensical prompt-context bounds."""

        if (
            isinstance(self.max_messages, bool)
            or not isinstance(self.max_messages, int)
            or self.max_messages < 1
        ):
            raise ValueError("max_messages must be a positive integer")


class ActorIdleReviewPlanningContextProjector:
    """Build a planner stage input from a fenced actor ledger projection.

    The projection intentionally never reads the legacy scheduler or in-memory
    active-chat coordinator. A message that appears after the effect's captured
    watermark is excluded; any later state change will independently fence the
    planner completion in the reducer.
    """

    def __init__(
        self,
        *,
        ledger: ActorPlanningLedgerPort,
        message_store: ActorPlanningMessageStore,
        context_builder: ReviewContextBuilder | None = None,
        config: IdleReviewPlanningContextConfig | None = None,
    ) -> None:
        self._ledger = ledger
        self._message_store = message_store
        self._context_builder = context_builder or ReviewContextBuilderAdapter()
        self._config = config or IdleReviewPlanningContextConfig()

    async def build_idle_review_planning_stage_input(
        self,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> ReviewStageInput:
        """Project one exact actor-owned prompt boundary for the planner."""

        effect = request.effect
        entries = await self._ledger.list_message_ledger(effect.key)
        selected = self._select_entries(entries, request=request)
        messages = self._load_messages(selected)
        message_log_ids = tuple(entry.message_log_id for entry in selected)
        options = ReviewContextBuildOptions(
            metadata={
                "actor_v2": True,
                "operation_id": effect.operation_id,
                "plan_id": effect.plan_id,
                "ownership_generation": effect.ownership_generation,
                "input_watermark": effect.input_watermark,
                "input_ledger_sequence": effect.input_ledger_sequence,
                "ledger_message_log_ids": list(message_log_ids),
                "planning_input": effect.planning_input.to_payload(),
            }
        )
        return self._context_builder.build_for_messages(
            session_id=effect.key.session_id,
            messages=messages,
            purpose="idle_review_planning",
            options=options,
        )

    def _select_entries(
        self,
        entries: Sequence[MessageLedgerEntry],
        *,
        request: IdleReviewPlanningWorkflowRequest,
    ) -> tuple[MessageLedgerEntry, ...]:
        """Select the latest ledger rows inside the effect's immutable fence."""

        effect = request.effect
        selected: list[MessageLedgerEntry] = []
        seen_message_ids: set[int] = set()
        previous_sequence = 0
        for entry in entries:
            if not isinstance(entry, MessageLedgerEntry):
                raise IdleReviewPlanningContextError(
                    "actor planner ledger returned an invalid entry"
                )
            if entry.key != effect.key:
                raise IdleReviewPlanningContextError(
                    "actor planner ledger returned another session"
                )
            if entry.ledger_sequence <= previous_sequence:
                raise IdleReviewPlanningContextError(
                    "actor planner ledger order is not strictly increasing"
                )
            previous_sequence = entry.ledger_sequence
            if entry.message.ownership_generation != effect.ownership_generation:
                continue
            if not entry.message.eligible_for_work:
                continue
            if entry.message_log_id > effect.input_watermark:
                continue
            if (
                effect.input_ledger_sequence is not None
                and entry.ledger_sequence > effect.input_ledger_sequence
            ):
                continue
            if entry.message_log_id in seen_message_ids:
                raise IdleReviewPlanningContextError(
                    "actor planner ledger contains a duplicate message_log_id"
                )
            seen_message_ids.add(entry.message_log_id)
            selected.append(entry)
        return tuple(selected[-self._config.max_messages :])

    def _load_messages(
        self,
        entries: tuple[MessageLedgerEntry, ...],
    ) -> list[MessageLogPayload]:
        """Reload selected logs after checking their ingress session identity."""

        message_log_ids = tuple(entry.message_log_id for entry in entries)
        if not message_log_ids:
            return []
        expected_session_ids = {
            entry.message_log_id: entry.message.base_session_id for entry in entries
        }
        missing_session_identity = next(
            (
                message_log_id
                for message_log_id, session_id in expected_session_ids.items()
                if not session_id
            ),
            None,
        )
        if missing_session_identity is not None:
            raise IdleReviewPlanningContextError(
                "actor planner ledger omitted base session identity: "
                + str(missing_session_identity)
            )
        payloads = self._message_store.list_by_ids(message_log_ids)
        by_id: dict[int, MessageLogPayload] = {}
        expected = set(message_log_ids)
        for payload in payloads:
            message_log_id = payload.get("id")
            if (
                isinstance(message_log_id, bool)
                or not isinstance(message_log_id, int)
                or message_log_id not in expected
            ):
                raise IdleReviewPlanningContextError(
                    "actor planner message store returned an unexpected message"
                )
            if message_log_id in by_id:
                raise IdleReviewPlanningContextError(
                    "actor planner message store returned a duplicate message"
                )
            expected_session_id = expected_session_ids[message_log_id]
            actual_session_id = payload.get("session_id")
            if (
                not isinstance(actual_session_id, str)
                or actual_session_id != expected_session_id
            ):
                raise IdleReviewPlanningContextError(
                    "actor planner message log session mismatch: "
                    + str(message_log_id)
                )
            by_id[message_log_id] = dict(payload)
        missing = [message_log_id for message_log_id in message_log_ids if message_log_id not in by_id]
        if missing:
            rendered = ", ".join(str(message_log_id) for message_log_id in missing)
            raise IdleReviewPlanningContextError(
                "actor planner message logs disappeared: " + rendered
            )
        return [by_id[message_log_id] for message_log_id in message_log_ids]


__all__ = [
    "ActorIdleReviewPlanningContextProjector",
    "ActorPlanningLedgerPort",
    "ActorPlanningMessageStore",
    "IdleReviewPlanningContextConfig",
    "IdleReviewPlanningContextError",
]
