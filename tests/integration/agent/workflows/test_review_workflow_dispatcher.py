from __future__ import annotations

from review_workflow_support import (
    ActiveChatCoordinator,
    ActiveChatDisposition,
    ActiveReplyDispatcher,
    AgentScheduler,
    AgentState,
    Any,
    DatabaseManager,
    DatabaseReviewMessageStore,
    DatabaseReviewSummaryStore,
    FixedReviewPolicy,
    RecordingOverflowCompressionRunner,
    RecordingReviewContextBuilder,
    ReviewCoordinator,
    ReviewWorkflowConfig,
    UnreadMessage,
    _insert_message,
    _strip_run_id_from_calls,
    build_review_workflow_explanation,
    make_agent_signal,
    pytest,
)


@pytest.mark.asyncio
async def test_attention_dispatcher_can_run_review_workflow() -> None:
    workflow = ReviewCoordinator(now=lambda: 100.0)
    active_chat_workflow = ActiveChatCoordinator(now=lambda: 100.0)
    dispatcher = ActiveReplyDispatcher(
        review_coordinator=workflow,
        active_chat_workflow=active_chat_workflow,
    )
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        now=lambda: 10.0,
    )

    await scheduler.accept_signal(make_agent_signal(message_log_id=1))

    decision = await scheduler.run_due_review("bot:group:room", now=10.0)

    assert decision.review_started is True
    assert decision.review_workflow_started is True
    assert decision.state == AgentState.ACTIVE_CHAT
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    active_chat_state = scheduler.active_chat_state_for("bot:group:room")
    assert active_chat_state is not None
    assert active_chat_state.interest_value == 15.0
    assert dispatcher.last_review_result is not None
    assert dispatcher.last_review_explanation is not None
    assert dispatcher.last_review_explanation.active_chat_initial_interest is None
    assert dispatcher.last_review_explanation.replied is False
    active_attention_state = active_chat_workflow.attention_state_for("bot:group:room")
    assert active_attention_state is not None
    from shinbot.agent.services.summaries import ReviewHandoffContext

    assert isinstance(active_attention_state.review_result_summary, ReviewHandoffContext)
    assert active_attention_state.review_result_summary.explanation == dispatcher.last_review_explanation


@pytest.mark.asyncio
async def test_attention_dispatcher_feeds_review_added_unread_to_active_chat(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    before_review_id = _insert_message(db, raw_text="before review", created_at=1000.0)
    during_review_id = _insert_message(db, raw_text="during review", created_at=2000.0)
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)

    active_chat_workflow = ActiveChatCoordinator(now=lambda: 100.0)
    dispatcher = ActiveReplyDispatcher(
        review_coordinator=ReviewCoordinator(
            ReviewWorkflowConfig(review_scan_batch_size=10),
            message_store=DatabaseReviewMessageStore(db),
            context_builder=RecordingReviewContextBuilder(),
            now=lambda: 100.0,
        ),
        active_chat_workflow=active_chat_workflow,
    )
    now = 10.0
    scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=lambda signal: (
            f"profile-{signal.message.message_log_id}" if signal.message else "profile-missing"
        ),
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: now,
    )
    await scheduler.accept_signal(make_agent_signal(message_log_id=before_review_id))
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    frozen_unread = scheduler.unread_messages("bot:group:room")

    now = 11.0
    await scheduler.accept_signal(
        make_agent_signal(
            message_log_id=during_review_id,
            sender_id="user-2",
            is_mention_to_other=True,
        )
    )

    await dispatcher.run_review(
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=frozen_unread,
    )

    active_attention_state = active_chat_workflow.attention_state_for("bot:group:room")
    assert active_attention_state is not None
    assert [
        message.message_log_id
        for message in active_attention_state.pending_buffer
    ] == [during_review_id]
    seeded_signal = active_attention_state.pending_buffer[0]
    assert seeded_signal.response_profile == f"profile-{during_review_id}"
    assert seeded_signal.is_mention_to_other is True
    assert active_attention_state.accumulated == 0.5
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        during_review_id
    ]
    assert dispatcher.last_review_result is not None
    assert dispatcher.last_review_result.consumed_ranges[0].start_msg_log_id == before_review_id
    await active_chat_workflow.shutdown()


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
    workflow = ReviewCoordinator(
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


@pytest.mark.asyncio
async def test_overflow_compression_runner_summarizes_old_unread_prefix(tmp_path) -> None:
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
    compression_runner = RecordingOverflowCompressionRunner()
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=10,
            overflow_threshold_messages=3,
        ),
        message_store=DatabaseReviewMessageStore(db),
        summary_store=DatabaseReviewSummaryStore(db),
        context_builder=context_builder,
        compression_runner=compression_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert len(result.scan.compressed_ranges) == 1
    compressed = result.scan.compressed_ranges[0]
    assert compressed.summary == "older messages summarized"
    assert compressed.candidate_message_ids == [message_ids[0]]
    assert compressed.reason == "compressed_old_messages"
    assert result.scan.candidate_message_ids == [message_ids[0]]
    assert result.reply.target_message_ids == [message_ids[0]]
    persisted_summaries = DatabaseReviewSummaryStore(db).list_summaries("bot:group:room")
    assert len(persisted_summaries) == 1
    assert persisted_summaries[0].summary == "older messages summarized"
    assert persisted_summaries[0].candidate_message_ids == [message_ids[0]]
    assert persisted_summaries[0].reason == "compressed_old_messages"
    assert persisted_summaries[0].start_msg_log_id == message_ids[0]
    assert persisted_summaries[0].end_msg_log_id == message_ids[1]
    assert _strip_run_id_from_calls(compression_runner.calls) == [
        {
            "purpose": "overflow_compression",
            "message_ids": message_ids[:2],
            "metadata": {
                "purpose": "overflow_compression",
                "start_msg_log_id": message_ids[0],
                "end_msg_log_id": message_ids[1],
                "message_count": 2,
                "reason": "overflow_pending_compression",
            },
        }
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        message_ids[0],
        message_ids[1],
    ]
    assert [call["purpose"] for call in context_builder.calls][:2] == [
        "overflow_compression",
        "review_scan",
    ]
    review_scan_call = next(
        call for call in context_builder.calls if call["purpose"] == "review_scan"
    )
    assert "older messages summarized" in review_scan_call["previous_summary"]
    assert review_scan_call["metadata"]["overflow_summaries"][0]["summary"] == (
        "older messages summarized"
    )
    reply_call = next(
        call for call in context_builder.calls if call["purpose"] == "reply_decision"
    )
    await workflow.wait_pending_bootstraps()
    bootstrap_call = next(
        call for call in context_builder.calls if call["purpose"] == "active_chat_bootstrap"
    )
    assert "older messages summarized" in reply_call["previous_summary"]
    assert "older messages summarized" in bootstrap_call["previous_summary"]
    assert [trace.purpose for trace in result.stage_traces] == [
        "overflow_compression",
        "review_scan",
        "reply_decision",
    ]
    assert result.stage_traces[0].reason == "compressed_old_messages"
    assert result.stage_traces[0].candidate_message_ids == [message_ids[0]]
    assert result.stage_traces[1].metadata["overflow_summaries"][0]["summary"] == (
        "older messages summarized"
    )
    assert "older messages summarized" in result.stage_traces[1].previous_summary


def test_review_workflow_explanation_summarizes_result() -> None:
    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ConsumedUnreadRange,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewStageTrace,
        ReviewWorkflowResult,
        UnreadRangeSummaryRecord,
    )

    result = ReviewWorkflowResult(
        review_run_id="test_run_id",
        scan=ReviewScanResult(
            candidate_message_ids=[3],
            scanned_message_count=5,
            loaded_message_count=3,
            batch_count=2,
            compressed_ranges=[
                UnreadRangeSummaryRecord(
                    session_id="bot:group:room",
                    start_msg_log_id=1,
                    end_msg_log_id=2,
                    start_at=1.0,
                    end_at=2.0,
                    message_count=2,
                    summary="older context",
                )
            ],
        ),
        reply=ReplyDecisionResult(
            replied=True,
            reply_message_id=10,
            reply_message_ids=[10],
            target_message_ids=[3],
            reply_reason="answered",
        ),
        bootstrap=ActiveChatBootstrapResult(
            disposition=ActiveChatDisposition.CASUAL,
            bootstrap_applied=True,
            active_chat_interest_value=40.0,
            active_chat_decay_half_life_seconds=30.0,
            reason="keep_chatting",
        ),
        review_started_at=100.0,
        consumed_range_ids=[7],
        consumed_ranges=[
            ConsumedUnreadRange(
                range_id=7,
                session_id="bot:group:room",
                start_msg_log_id=3,
                end_msg_log_id=5,
                message_count=3,
                full_range=True,
            )
        ],
        stage_traces=[
            ReviewStageTrace(
                purpose="reply_decision",
                message_ids=[2, 3, 4],
                reason="answered",
                target_message_ids=[3],
                replied=True,
                reply_message_id=10,
                reply_message_ids=[10],
            ),
            ReviewStageTrace(
                purpose="active_chat_bootstrap",
                message_ids=[3, 4, 5],
                reason="keep_chatting",
                active_chat_disposition=ActiveChatDisposition.CASUAL,
                active_chat_bootstrap_applied=True,
                active_chat_interest_value=40.0,
                active_chat_decay_half_life_seconds=30.0,
            ),
        ],
    )

    explanation = build_review_workflow_explanation(result)

    assert explanation.review_started_at == 100.0
    assert explanation.scanned_message_count == 5
    assert explanation.loaded_message_count == 3
    assert explanation.reviewed_batch_count == 2
    assert explanation.candidate_message_ids == [3]
    assert explanation.reply_target_message_ids == [3]
    assert explanation.replied is True
    assert explanation.reply_message_id == 10
    assert explanation.reply_message_ids == [10]
    assert explanation.overflow_summary_count == 1
    assert explanation.overflow_summary_message_count == 2
    assert explanation.consumed_range_ids == [7]
    assert explanation.consumed_message_count == 3
    assert explanation.active_chat_initial_interest == 40.0
    assert explanation.active_chat_decay_half_life_seconds == 30.0
    assert explanation.active_chat_disposition == ActiveChatDisposition.CASUAL
    assert explanation.active_chat_bootstrap_applied is True
    assert explanation.active_chat_reason == "keep_chatting"
    assert [stage.purpose for stage in explanation.stages] == [
        "reply_decision",
        "active_chat_bootstrap",
    ]
    assert explanation.stages[0].input_message_count == 3
    assert explanation.stages[0].target_message_ids == [3]
    assert explanation.stages[0].replied is True
    assert explanation.stages[0].reply_message_ids == [10]
    assert explanation.stages[1].active_chat_interest_value == 40.0
    assert explanation.stages[1].active_chat_disposition == ActiveChatDisposition.CASUAL


@pytest.mark.asyncio
async def test_review_workflow_uses_actual_message_bounds_for_interleaved_sessions(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text="room-1", created_at=1000.0),
        _insert_message(db, session_id="bot:group:other", raw_text="other-1", created_at=1500.0),
        _insert_message(db, raw_text="room-2", created_at=2000.0),
        _insert_message(db, session_id="bot:group:other", raw_text="other-2", created_at=2500.0),
        _insert_message(db, raw_text="room-3", created_at=3000.0),
    ]
    room_message_ids = [message_ids[0], message_ids[2], message_ids[4]]
    for message_id in room_message_ids:
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
    compression_runner = RecordingOverflowCompressionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=10,
            overflow_threshold_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        summary_store=DatabaseReviewSummaryStore(db),
        context_builder=RecordingReviewContextBuilder(),
        compression_runner=compression_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert [call["message_ids"] for call in compression_runner.calls] == [
        room_message_ids[:2]
    ]
    assert result.scan.compressed_ranges[0].start_msg_log_id == room_message_ids[0]
    assert result.scan.compressed_ranges[0].end_msg_log_id == room_message_ids[1]
    assert [(item.start_msg_log_id, item.end_msg_log_id) for item in result.consumed_ranges] == [
        (room_message_ids[2], room_message_ids[2])
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        room_message_ids[0],
        room_message_ids[1],
    ]


class _HandoffFakeSummaryRecord:
    def __init__(
        self,
        *,
        content: str,
        block_index: int | None = None,
        msg_log_start: int | None = None,
        msg_log_end: int | None = None,
        msg_count: int = 0,
        created_at: float = 0.0,
    ) -> None:
        self.content = content
        self.block_index = block_index
        self.msg_log_start = msg_log_start
        self.msg_log_end = msg_log_end
        self.msg_count = msg_count
        self.created_at = created_at


class _HandoffFakeSummaryService:
    def __init__(
        self,
        *,
        overflow_records: list[Any] | None = None,
        digest_records: list[Any] | None = None,
        active_record: Any | None = None,
    ) -> None:
        self._overflow_records = overflow_records or []
        self._digest_records = digest_records or []
        self._active_record = active_record
        self.list_by_run_id_calls: list[tuple[str, Any]] = []
        self.get_latest_calls: list[tuple[str, Any]] = []

    def list_by_run_id(
        self, run_id: str, *, summary_type: Any = None,
    ) -> list[Any]:
        self.list_by_run_id_calls.append((run_id, summary_type))
        from shinbot.agent.services.summaries import SummaryType

        if summary_type == SummaryType.OVERFLOW_COMPRESSION:
            return self._overflow_records
        if summary_type == SummaryType.BLOCK_DIGEST:
            return self._digest_records
        return []

    def get_latest_by_session(
        self, session_id: str, *, summary_type: Any = None,
    ) -> Any | None:
        self.get_latest_calls.append((session_id, summary_type))
        return self._active_record


@pytest.mark.asyncio
async def test_dispatcher_build_handoff_context_with_summaries() -> None:
    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewWorkflowConfig,
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )
    from shinbot.agent.services.summaries import (
        ReviewHandoffContext,
        SummaryHandoffEntry,
        SummaryType,
    )

    explanation = ReviewWorkflowExplanation(
        review_run_id="run_abc",
        review_started_at=100.0,
    )
    result = ReviewWorkflowResult(
        review_run_id="run_abc",
        scan=ReviewScanResult(),
        reply=ReplyDecisionResult(),
        bootstrap=ActiveChatBootstrapResult(),
        review_started_at=100.0,
    )

    fake_summary_service = _HandoffFakeSummaryService(
        overflow_records=[
            _HandoffFakeSummaryRecord(
                content="Old overflow summary.",
                msg_log_start=1,
                msg_log_end=10,
                msg_count=10,
            ),
        ],
        digest_records=[
            _HandoffFakeSummaryRecord(
                content="Block 0 digest.",
                block_index=0,
                msg_log_start=11,
                msg_log_end=20,
                msg_count=10,
            ),
            _HandoffFakeSummaryRecord(
                content="Block 1 digest.",
                block_index=1,
                msg_log_start=21,
                msg_log_end=30,
                msg_count=10,
            ),
        ],
        active_record=_HandoffFakeSummaryRecord(
            content="Previous active chat context.",
            created_at=9999999999.0,
        ),
    )
    dispatcher = ActiveReplyDispatcher(
        summary_service=fake_summary_service,
        review_config=ReviewWorkflowConfig(active_chat_summary_max_age_seconds=1800.0),
    )

    handoff = await dispatcher._build_handoff_context(
        session_id="bot:group:room",
        result=result,
        explanation=explanation,
    )

    assert isinstance(handoff, ReviewHandoffContext)
    assert handoff.review_run_id == "run_abc"
    assert handoff.explanation is explanation
    assert handoff.overflow_summaries == [
        SummaryHandoffEntry(
            content="Old overflow summary.",
            msg_log_start=1,
            msg_log_end=10,
            msg_count=10,
        )
    ]
    assert handoff.block_digests == [
        SummaryHandoffEntry(
            content="Block 0 digest.",
            block_index=0,
            msg_log_start=11,
            msg_log_end=20,
            msg_count=10,
        ),
        SummaryHandoffEntry(
            content="Block 1 digest.",
            block_index=1,
            msg_log_start=21,
            msg_log_end=30,
            msg_count=10,
        ),
    ]
    assert handoff.recent_active_chat_summary == "Previous active chat context."

    assert fake_summary_service.list_by_run_id_calls[0] == (
        "run_abc", SummaryType.OVERFLOW_COMPRESSION,
    )
    assert fake_summary_service.list_by_run_id_calls[1] == (
        "run_abc", SummaryType.BLOCK_DIGEST,
    )
    assert fake_summary_service.get_latest_calls[0] == (
        "bot:group:room", SummaryType.ACTIVE_CHAT,
    )


@pytest.mark.asyncio
async def test_dispatcher_build_handoff_context_without_summary_service() -> None:
    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )
    from shinbot.agent.services.summaries import ReviewHandoffContext

    explanation = ReviewWorkflowExplanation(
        review_run_id="run_abc",
        review_started_at=100.0,
    )
    result = ReviewWorkflowResult(
        review_run_id="run_abc",
        scan=ReviewScanResult(),
        reply=ReplyDecisionResult(),
        bootstrap=ActiveChatBootstrapResult(),
        review_started_at=100.0,
    )
    dispatcher = ActiveReplyDispatcher()

    handoff = await dispatcher._build_handoff_context(
        session_id="bot:group:room",
        result=result,
        explanation=explanation,
    )

    assert isinstance(handoff, ReviewHandoffContext)
    assert handoff.overflow_summaries == []
    assert handoff.block_digests == []
    assert handoff.recent_active_chat_summary is None


@pytest.mark.asyncio
async def test_dispatcher_build_handoff_context_filters_stale_active_summary() -> None:
    import time

    from shinbot.agent.coordinators.review.models import (
        ActiveChatBootstrapResult,
        ReplyDecisionResult,
        ReviewScanResult,
        ReviewWorkflowConfig,
        ReviewWorkflowExplanation,
        ReviewWorkflowResult,
    )

    explanation = ReviewWorkflowExplanation(
        review_run_id="run_abc",
        review_started_at=100.0,
    )
    result = ReviewWorkflowResult(
        review_run_id="run_abc",
        scan=ReviewScanResult(),
        reply=ReplyDecisionResult(),
        bootstrap=ActiveChatBootstrapResult(),
        review_started_at=100.0,
    )

    stale_record = _HandoffFakeSummaryRecord(
        content="Stale summary.",
        created_at=time.time() - 3600,
    )
    fake_summary_service = _HandoffFakeSummaryService(active_record=stale_record)
    dispatcher = ActiveReplyDispatcher(
        summary_service=fake_summary_service,
        review_config=ReviewWorkflowConfig(active_chat_summary_max_age_seconds=1800.0),
    )

    handoff = await dispatcher._build_handoff_context(
        session_id="bot:group:room",
        result=result,
        explanation=explanation,
    )

    assert handoff.recent_active_chat_summary is None
