from __future__ import annotations

import pytest

from shinbot.agent.review import (
    ActiveChatBootstrapStageOutput,
    DatabaseReviewMessageStore,
    ReplyDecisionStageOutput,
    ReviewContextBuilderAdapter,
    ReviewScanStageOutput,
    ReviewWorkflow,
    ReviewWorkflowConfig,
)
from shinbot.agent.scheduler import AgentScheduler, AgentState, AttentionActiveReplyDispatcher
from shinbot.agent.scheduler.models import (
    ReviewCompletionDecision,
    ReviewPlan,
    UnreadMessage,
    UnreadRange,
)
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import MessageLogRecord


class FixedReviewPolicy:
    def initial_plan(self, *, session_id: str, now: float) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now,
            reason="fixed_due_review",
            updated_at=now,
        )

    def plan_after_review(
        self,
        *,
        session_id: str,
        now: float,
        previous_plan: ReviewPlan | None = None,
    ) -> ReviewPlan:
        return ReviewPlan(
            session_id=session_id,
            next_review_at=now + 60.0,
            reason="fixed_after_review",
            updated_at=now,
        )


class FakeReviewScheduler:
    def __init__(self) -> None:
        self.complete_review_calls: list[dict[str, object]] = []

    def unread_ranges(self, session_id: str, *, limit: int = 50) -> list[UnreadRange]:
        return [
            UnreadRange(
                id=1,
                session_id=session_id,
                start_msg_log_id=1,
                end_msg_log_id=2,
                start_at=10.0,
                end_at=11.0,
                message_count=2,
            ),
            UnreadRange(
                id=2,
                session_id=session_id,
                start_msg_log_id=3,
                end_msg_log_id=5,
                start_at=12.0,
                end_at=14.0,
                message_count=3,
            ),
        ][:limit]

    def count_unread_messages(self, session_id: str) -> int:
        return 5

    def complete_review(
        self,
        session_id: str,
        *,
        enter_active_chat: bool = False,
        active_chat_initial_interest: float | None = None,
        active_chat_decay_half_life_seconds: float | None = None,
        next_review_plan: ReviewPlan | None = None,
        now: float | None = None,
    ) -> ReviewCompletionDecision:
        self.complete_review_calls.append(
            {
                "session_id": session_id,
                "enter_active_chat": enter_active_chat,
                "active_chat_initial_interest": active_chat_initial_interest,
                "active_chat_decay_half_life_seconds": active_chat_decay_half_life_seconds,
                "next_review_plan": next_review_plan,
                "now": now,
            }
        )
        return ReviewCompletionDecision(
            session_id=session_id,
            state=AgentState.ACTIVE_CHAT,
            active_chat_started=True,
        )


class RecordingReviewContextBuilder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def build_for_messages(
        self,
        *,
        session_id: str,
        messages: list[dict],
        purpose: str,
        options=None,
    ):
        self.calls.append(
            {
                "session_id": session_id,
                "message_ids": [message["id"] for message in messages],
                "purpose": purpose,
                "metadata": dict(options.metadata) if options is not None else {},
            }
        )
        return None


class SelectingReviewScanRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ReviewScanStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return ReviewScanStageOutput(
            candidate_message_ids=[message_ids[-1], message_ids[-1]] if message_ids else [],
            reason=f"selected_from_{len(message_ids)}",
        )


class RecordingReplyDecisionRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ReplyDecisionStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        candidate_id = stage_input.metadata["candidate_message_id"]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "candidate_id": candidate_id,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return ReplyDecisionStageOutput(
            replied=False,
            target_message_ids=[candidate_id],
            reason=f"checked_{candidate_id}",
        )


class RecordingActiveChatBootstrapRunner:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def run(self, stage_input) -> ActiveChatBootstrapStageOutput:
        message_ids = [message["id"] for message in stage_input.source_messages]
        self.calls.append(
            {
                "purpose": stage_input.purpose,
                "message_ids": message_ids,
                "metadata": dict(stage_input.metadata),
            }
        )
        return ActiveChatBootstrapStageOutput(
            initial_interest=0.7,
            decay_half_life_seconds=30.0,
            reason="bootstrap_selected_interest",
        )


class FakeContextManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def build_instruction_stage_content(
        self,
        session_id: str,
        unread_records: list[dict],
        *,
        previous_summary: str = "",
        self_platform_id: str = "",
        now_ms: int | None = None,
    ) -> list[dict]:
        self.calls.append(
            {
                "session_id": session_id,
                "message_ids": [record["id"] for record in unread_records],
                "previous_summary": previous_summary,
                "self_platform_id": self_platform_id,
                "now_ms": now_ms,
            }
        )
        return [{"type": "text", "text": f"{len(unread_records)} messages"}]


def _insert_message(
    db: DatabaseManager,
    *,
    session_id: str = "bot:group:room",
    raw_text: str,
    created_at: float,
) -> int:
    return db.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id=f"msg-{raw_text}",
            sender_id="user-1",
            sender_name="User",
            raw_text=raw_text,
            content_json="[]",
            role="user",
            created_at=created_at,
        )
    )


def test_database_review_message_store_reads_review_windows(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    store = DatabaseReviewMessageStore(db)
    unread_range = UnreadRange(
        id=1,
        session_id="bot:group:room",
        start_msg_log_id=message_ids[1],
        end_msg_log_id=message_ids[3],
        start_at=2000.0,
        end_at=4000.0,
        message_count=3,
    )

    range_rows = store.list_for_unread_range(unread_range, limit=2, offset=1)
    around_rows = store.list_around_message(
        session_id="bot:group:room",
        message_log_id=message_ids[2],
        before=1,
        after=2,
    )
    time_rows = store.list_by_time(
        session_id="bot:group:room",
        start_at=2000.0,
        end_at=5000.0,
        limit=10,
    )

    assert [row["raw_text"] for row in range_rows] == ["m3", "m4"]
    assert [row["raw_text"] for row in around_rows] == ["m2", "m3", "m4", "m5"]
    assert [row["raw_text"] for row in time_rows] == ["m2", "m3", "m4", "m5"]


def test_review_context_builder_adapter_uses_context_manager() -> None:
    context_manager = FakeContextManager()
    adapter = ReviewContextBuilderAdapter(context_manager)

    stage_input = adapter.build_for_messages(
        session_id="bot:group:room",
        messages=[{"id": 1, "raw_text": "hello"}],
        purpose="review_scan",
        options=None,
    )

    assert stage_input.session_id == "bot:group:room"
    assert stage_input.purpose == "review_scan"
    assert stage_input.source_messages == [{"id": 1, "raw_text": "hello"}]
    assert stage_input.instruction_content == [{"type": "text", "text": "1 messages"}]
    assert stage_input.metadata == {"purpose": "review_scan"}
    assert context_manager.calls[0]["message_ids"] == [1]


@pytest.mark.asyncio
async def test_review_workflow_records_overflow_plan_and_enters_active_chat() -> None:
    scheduler = FakeReviewScheduler()
    workflow = ReviewWorkflow(
        ReviewWorkflowConfig(
            review_scan_batch_size=2,
            overflow_threshold_messages=3,
            fallback_active_chat_interest=0.05,
        ),
        now=lambda: 100.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=ReviewPlan(
            session_id="bot:group:room",
            next_review_at=100.0,
            reason="test_review",
        ),
        unread_messages=[],
    )

    assert result.failed is False
    assert result.scan.scanned_message_count == 3
    assert result.scan.loaded_message_count == 0
    assert result.scan.batch_count == 2
    assert len(result.scan.compressed_ranges) == 1
    assert result.scan.compressed_ranges[0].start_msg_log_id == 1
    assert result.scan.compressed_ranges[0].end_msg_log_id == 2
    assert result.scan.compressed_ranges[0].message_count == 2
    assert result.reply.target_message_ids == []
    assert result.bootstrap.initial_interest == 0.05
    assert result.bootstrap.tail_history_start_at == -80_000.0
    assert result.bootstrap.tail_history_end_at == 100_000.0
    assert result.consumed_range_ids == []
    assert scheduler.complete_review_calls == [
        {
            "session_id": "bot:group:room",
            "enter_active_chat": True,
            "active_chat_initial_interest": 0.05,
            "active_chat_decay_half_life_seconds": None,
            "next_review_plan": None,
            "now": None,
        }
    ]


@pytest.mark.asyncio
async def test_review_workflow_uses_message_store_for_scan_and_tail_history(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewWorkflow(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.scanned_message_count == 5
    assert result.scan.loaded_message_count == 5
    assert result.scan.stage_input_count == 3
    assert result.scan.batch_count == 3
    assert result.bootstrap.tail_history_message_count == 5
    assert result.bootstrap.stage_input_built is True
    assert result.consumed_range_ids == [1]
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (message_ids[0], message_ids[-1], True)
    ]
    assert scheduler.unread_messages("bot:group:room") == []
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_scan",
        "review_scan",
        "active_chat_bootstrap",
    ]


@pytest.mark.asyncio
async def test_review_scan_runner_selects_candidate_message_ids(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 5)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    scan_runner = SelectingReviewScanRunner()
    workflow = ReviewWorkflow(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=scan_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.candidate_message_ids == [message_ids[1], message_ids[3]]
    assert result.scan.scan_reason == "selected_from_2"
    assert result.reply.target_message_ids == [message_ids[1], message_ids[3]]
    assert [call["purpose"] for call in scan_runner.calls] == ["review_scan", "review_scan"]
    assert [call["message_ids"] for call in scan_runner.calls] == [
        message_ids[:2],
        message_ids[2:],
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_reads_candidate_local_context(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    scan_runner = SelectingReviewScanRunner()
    reply_runner = RecordingReplyDecisionRunner()
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewWorkflow(
        ReviewWorkflowConfig(
            review_scan_batch_size=5,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        scan_runner=scan_runner,
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.candidate_message_ids == [message_ids[-1]]
    assert result.reply.target_message_ids == [message_ids[-1]]
    assert result.reply.loaded_message_count == 2
    assert result.reply.stage_input_count == 1
    assert result.reply.reply_reason == f"checked_{message_ids[-1]}"
    assert reply_runner.calls == [
        {
            "purpose": "reply_decision",
            "candidate_id": message_ids[-1],
            "message_ids": message_ids[-2:],
            "metadata": {
                "purpose": "reply_decision",
                "candidate_message_id": message_ids[-1],
                "before_messages": 1,
                "after_messages": 1,
            },
        }
    ]
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "reply_decision",
        "active_chat_bootstrap",
    ]


@pytest.mark.asyncio
async def test_active_chat_bootstrap_runner_receives_tail_history_and_reply_facts(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 5)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    bootstrap_runner = RecordingActiveChatBootstrapRunner()
    workflow = ReviewWorkflow(
        ReviewWorkflowConfig(
            review_scan_batch_size=4,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
            tail_history_before_seconds=10.0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=SelectingReviewScanRunner(),
        reply_runner=RecordingReplyDecisionRunner(),
        bootstrap_runner=bootstrap_runner,
        now=lambda: 4.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.bootstrap.initial_interest == 0.7
    assert result.bootstrap.decay_half_life_seconds == 30.0
    assert result.bootstrap.reason == "bootstrap_selected_interest"
    assert result.completion.active_chat_state.interest_value == 0.7
    assert result.completion.active_chat_state.decay_half_life_seconds == 30.0
    assert bootstrap_runner.calls == [
        {
            "purpose": "active_chat_bootstrap",
            "message_ids": message_ids,
            "metadata": {
                "purpose": "active_chat_bootstrap",
                "tail_history_start_at": -6000.0,
                "tail_history_end_at": 4000.0,
                "reply_replied": False,
                "reply_message_id": None,
                "reply_target_message_ids": [message_ids[-1]],
                "reply_reason": f"checked_{message_ids[-1]}",
            },
        }
    ]


@pytest.mark.asyncio
async def test_attention_dispatcher_can_run_review_workflow() -> None:
    workflow = ReviewWorkflow(now=lambda: 100.0)
    dispatcher = AttentionActiveReplyDispatcher(None, review_workflow=workflow)
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    await scheduler.accept_signal(
        AgentEntrySignal(
            session_id="bot:group:room",
            message_log_id=1,
            event_type="message-created",
            sender_id="user-1",
            instance_id="bot",
            platform="mock",
            self_id="bot-self",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    decision = await scheduler.run_due_review("bot:group:room", now=10.0)

    assert decision.review_started is True
    assert decision.review_workflow_started is True
    assert decision.state == AgentState.ACTIVE_CHAT
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    active_chat_state = scheduler.active_chat_state_for("bot:group:room")
    assert active_chat_state is not None
    assert active_chat_state.interest_value == 0.05


@pytest.mark.asyncio
async def test_review_workflow_splits_partially_consumed_overflow_range(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 6)
    ]
    for message_id in message_ids:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
            )
        )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    workflow = ReviewWorkflow(
        ReviewWorkflowConfig(
            review_scan_batch_size=10,
            overflow_threshold_messages=3,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.scanned_message_count == 3
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (message_ids[2], message_ids[-1], False)
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        message_ids[0],
        message_ids[1],
    ]
    assert [
        (item.start_msg_log_id, item.end_msg_log_id, item.message_count)
        for item in scheduler.unread_ranges("bot:group:room")
    ] == [(message_ids[0], message_ids[1], 2)]
