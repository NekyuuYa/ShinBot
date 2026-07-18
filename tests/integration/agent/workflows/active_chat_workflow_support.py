from __future__ import annotations

import asyncio

import pytest

from shinbot.agent.coordinators.active_chat import ActiveChatCoordinator
from shinbot.agent.coordinators.active_chat.actions import interest_effect_for_round
from shinbot.agent.coordinators.active_chat.attention import (
    ActiveChatAttention,
    ActiveChatAttentionConfig,
)
from shinbot.agent.coordinators.active_chat.models import (
    ActiveChatActionKind,
    ActiveChatBatch,
    ActiveChatMessageSignal,
    ActiveChatNoReplyIntensity,
    ActiveChatReplyIntensity,
    ActiveChatRoundResult,
)
from shinbot.agent.scheduler import ActiveChatState
from shinbot.agent.scheduler.models import AgentState, ReviewPlan


class RecordingScheduler:
    def __init__(self) -> None:
        self.consumed: list[tuple[str, list[int]]] = []
        self.adjustments: list[dict[str, object]] = []
        self.adjustment_active_epochs: list[int | None] = []
        self.planned_review: ReviewPlan | None = None

    def mark_active_chat_consumed(
        self,
        session_id: str,
        message_log_ids: list[int],
    ) -> list[object]:
        self.consumed.append((session_id, list(message_log_ids)))
        return []

    def adjust_active_chat_interest(
        self,
        session_id: str,
        *,
        delta: float = 0.0,
        force_exit: bool = False,
        active_epoch: int | None = None,
        reason: str = "",
        next_review_plan: ReviewPlan | None = None,
    ) -> object:
        self.adjustment_active_epochs.append(active_epoch)
        self.adjustments.append(
            {
                "session_id": session_id,
                "delta": delta,
                "force_exit": force_exit,
                "reason": reason,
                "next_review_plan": next_review_plan,
            }
        )
        return object()

    def preview_active_chat_interest_adjustment(
        self,
        session_id: str,
        *,
        delta: float = 0.0,
        force_exit: bool = False,
        active_epoch: int | None = None,
    ) -> object:
        return type(
            "Preview",
            (),
            {
                "session_id": session_id,
                "state": AgentState.ACTIVE_CHAT,
                "delta": delta,
                "force_exit": force_exit,
                "will_return_idle": force_exit,
            },
        )()

    async def plan_idle_review_after_active_chat(
        self,
        session_id: str,
        *,
        request: object | None = None,
    ) -> ReviewPlan | None:
        del request
        return self.planned_review


def make_active_state(
    *,
    interest_value: float = 30.0,
    active_epoch: int = 0,
) -> ActiveChatState:
    return ActiveChatState(
        session_id="bot:group:room",
        interest_value=interest_value,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=active_epoch,
    )


def make_signal(**kwargs) -> ActiveChatMessageSignal:
    values = {
        "session_id": "bot:group:room",
        "message_log_id": 1,
        "sender_id": "user-1",
        "response_profile": "balanced",
        "self_platform_id": "bot-self",
    }
    values.update(kwargs)
    return ActiveChatMessageSignal(**values)


async def start_workflow(
    workflow: ActiveChatCoordinator,
    *,
    active_state: ActiveChatState | None = None,
    review_result_summary: object | None = None,
) -> ActiveChatState:
    state = active_state or make_active_state()
    await workflow.start_active_chat(
        session_id="bot:group:room",
        active_chat_state=state,
        review_result_summary=review_result_summary,
    )
    return state


__all__ = [
    "ActiveChatActionKind",
    "ActiveChatAttention",
    "ActiveChatAttentionConfig",
    "ActiveChatBatch",
    "ActiveChatCoordinator",
    "ActiveChatMessageSignal",
    "ActiveChatNoReplyIntensity",
    "ActiveChatReplyIntensity",
    "ActiveChatRoundResult",
    "ActiveChatState",
    "RecordingScheduler",
    "ReviewPlan",
    "annotations",
    "asyncio",
    "interest_effect_for_round",
    "make_active_state",
    "make_signal",
    "pytest",
    "start_workflow",
]
