from __future__ import annotations

from review_workflow_support import (
    ActiveChatDisposition,
    AgentScheduler,
    AgentState,
    DatabaseManager,
    DatabaseReviewMessageStore,
    FailingBlockDigestRunner,
    FakeReviewScheduler,
    FakeSummaryService,
    FixedCandidateScanRunner,
    FixedReviewPolicy,
    RecordingActiveChatBootstrapRunner,
    RecordingBlockDigestRunner,
    RecordingReplyDecisionRunner,
    RecordingReviewContextBuilder,
    ReviewCoordinator,
    ReviewPlan,
    ReviewWorkflowConfig,
    SelectingReviewScanRunner,
    SlowBlockDigestRunner,
    UnreadMessage,
    YieldingReviewScanRunner,
    _insert_message,
    _strip_run_id_from_calls,
    asyncio,
    make_agent_signal,
    pytest,
)

from shinbot.agent.runners.review_models import ReviewScanStageOutput
from shinbot.agent.runtime.task_manager import AgentTaskManager


class _InterruptingReviewScanRunner:
    """Scan runner that fails on the Nth call to simulate a forced shutdown."""

    def __init__(self, *, fail_on_call: int) -> None:
        self.calls = 0
        self._fail_on_call = fail_on_call

    async def run(self, stage_input) -> ReviewScanStageOutput:
        self.calls += 1
        if self.calls >= self._fail_on_call:
            raise RuntimeError("simulated forced shutdown mid-scan")
        message_ids = [message["id"] for message in stage_input.source_messages]
        return ReviewScanStageOutput(
            candidate_message_ids=[],
            reason=f"scanned_{len(message_ids)}",
        )


@pytest.mark.asyncio
async def test_review_workflow_records_overflow_plan_and_enters_active_chat() -> None:
    scheduler = FakeReviewScheduler()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=2,
            overflow_threshold_messages=3,
            provisional_active_chat_interest=15.0,
            provisional_active_chat_half_life_seconds=20.0,
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
    assert result.bootstrap.disposition is None
    assert result.bootstrap.reason == "active_chat_bootstrap_scheduled"
    assert result.bootstrap.tail_history_start_at == -80_000.0
    assert result.bootstrap.tail_history_end_at is None
    assert result.consumed_range_ids == []
    await workflow.wait_pending_bootstraps()
    completed_bootstrap = workflow.last_bootstrap_result("bot:group:room")
    assert completed_bootstrap is not None
    assert completed_bootstrap.reason == "active_chat_bootstrap_skipped_no_message_store"
    assert completed_bootstrap.tail_history_end_at == 100_000.0
    assert scheduler.complete_review_calls == [
        {
            "session_id": "bot:group:room",
            "enter_active_chat": True,
            "active_chat_initial_interest": 15.0,
            "active_chat_decay_half_life_seconds": 20.0,
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
                self_platform_id="bot-self",
                trace_id=f"ingress:bot:{message_id}",
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
    unread_snapshot = [
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=message_id,
            sender_id="user-1",
            created_at=float(message_id),
            self_platform_id="bot-self",
            trace_id=f"ingress:bot:{message_id}",
        )
        for message_id in message_ids
    ]
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=unread_snapshot,
    )

    assert result.scan.scanned_message_count == 5
    assert result.scan.loaded_message_count == 5
    assert result.scan.stage_input_count == 3
    assert result.scan.batch_count == 3
    assert result.bootstrap.reason == "active_chat_bootstrap_scheduled"
    assert result.consumed_range_ids == [1]
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (message_ids[0], message_ids[-1], True)
    ]
    assert [trace.purpose for trace in result.stage_traces] == [
        "review_scan",
        "review_scan",
        "review_scan",
    ]
    assert result.stage_traces[0].message_ids == message_ids[:2]
    assert scheduler.unread_messages("bot:group:room") == []
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT
    await workflow.wait_pending_bootstraps()
    completed_bootstrap = workflow.last_bootstrap_result("bot:group:room")
    assert completed_bootstrap is not None
    assert completed_bootstrap.tail_history_message_count == 5
    assert completed_bootstrap.stage_input_built is True
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_block_digest",
        "review_scan",
        "review_block_digest",
        "review_scan",
        "review_block_digest",
        "active_chat_bootstrap",
    ]
    first_scan_metadata = context_builder.calls[0]["metadata"]
    assert first_scan_metadata["self_platform_id"] == "bot-self"
    assert first_scan_metadata["trace_id"] == f"ingress:bot:{message_ids[0]}"
    assert first_scan_metadata["trace_ids"] == [
        f"ingress:bot:{message_ids[0]}",
        f"ingress:bot:{message_ids[1]}",
    ]


@pytest.mark.asyncio
async def test_review_workflow_freezes_unread_snapshot_at_entry(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    first_message_id = _insert_message(db, raw_text="before review", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=first_message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id="bot:group:room", now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    now = 10.0
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: now,
    )
    scheduler.prepare_due_review("bot:group:room", now=10.0)
    frozen_unread = scheduler.unread_messages("bot:group:room")

    second_message_id = _insert_message(db, raw_text="during review", created_at=2000.0)
    now = 11.0
    await scheduler.accept_signal(
        make_agent_signal(message_log_id=second_message_id, sender_id="user-2")
    )

    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=10),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        now=lambda: 12.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=frozen_unread,
    )

    assert result.scan.scanned_message_count == 1
    assert [trace.message_ids for trace in result.stage_traces if trace.purpose == "review_scan"] == [
        [first_message_id]
    ]
    assert [(item.start_msg_log_id, item.end_msg_log_id, item.full_range) for item in result.consumed_ranges] == [
        (first_message_id, first_message_id, False)
    ]
    assert [message.message_log_id for message in scheduler.unread_messages("bot:group:room")] == [
        second_message_id
    ]
    assert scheduler.state_for("bot:group:room") == AgentState.ACTIVE_CHAT


@pytest.mark.asyncio
async def test_concurrent_review_runs_keep_distinct_review_run_ids(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    sessions = ["bot:group:room-a", "bot:group:room-b"]
    message_ids_by_session: dict[str, list[int]] = {}
    for session_index, session_id in enumerate(sessions):
        message_ids = [
            _insert_message(
                db,
                session_id=session_id,
                raw_text=f"{session_id}-m{index}",
                created_at=float((session_index + 1) * 10_000 + index * 1000),
            )
            for index in range(1, 3)
        ]
        message_ids_by_session[session_id] = message_ids
        for message_id in message_ids:
            db.agent_scheduler.add_unread(
                UnreadMessage(
                    session_id=session_id,
                    message_log_id=message_id,
                    sender_id="user-1",
                    created_at=float(message_id),
                )
            )
        db.agent_scheduler.set_review_plan(
            FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
        )
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    review_inputs = []
    for session_id in sessions:
        decision = scheduler.prepare_due_review(session_id, now=10.0)
        assert decision.review_plan is not None
        review_inputs.append((
            session_id,
            decision.review_plan,
            scheduler.unread_messages(session_id),
        ))
    context_builder = RecordingReviewContextBuilder()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=context_builder,
        scan_runner=YieldingReviewScanRunner(),
        reply_runner=RecordingReplyDecisionRunner(),
        now=lambda: 5.0,
    )

    results = await asyncio.gather(*[
        workflow.run(
            scheduler=scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=unread_messages,
        )
        for session_id, review_plan, unread_messages in review_inputs
    ])
    await workflow.wait_pending_bootstraps()

    run_id_by_session = {result.completion.session_id: result.review_run_id for result in results if result.completion is not None}
    assert len(set(run_id_by_session.values())) == 2
    for call in context_builder.calls:
        assert call["metadata"]["review_run_id"] == run_id_by_session[call["session_id"]]


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
    workflow = ReviewCoordinator(
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
    workflow = ReviewCoordinator(
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
    assert _strip_run_id_from_calls(reply_runner.calls) == [
        {
            "purpose": "reply_decision",
            "candidate_id": message_ids[-1],
            "message_ids": message_ids[-2:],
            "metadata": {
                "purpose": "reply_decision",
                "candidate_message_id": message_ids[-1],
                "candidate_message_ids": [message_ids[-1]],
                "before_messages": 1,
                "after_messages": 1,
            },
        }
    ]
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_block_digest",
        "reply_decision",
    ]
    await workflow.wait_pending_bootstraps()
    assert [call["purpose"] for call in context_builder.calls] == [
        "review_scan",
        "review_block_digest",
        "reply_decision",
        "active_chat_bootstrap",
    ]


@pytest.mark.asyncio
async def test_reply_decision_groups_overlapping_candidate_contexts(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 8)
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
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=7,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_ids[2], message_ids[3]]),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.candidate_message_ids == [message_ids[2], message_ids[3]]
    assert result.reply.target_message_ids == [message_ids[2], message_ids[3]]
    assert result.reply.stage_input_count == 1
    assert result.reply.loaded_message_count == 4
    assert _strip_run_id_from_calls(reply_runner.calls) == [
        {
            "purpose": "reply_decision",
            "candidate_id": message_ids[2],
            "message_ids": message_ids[1:5],
            "metadata": {
                "purpose": "reply_decision",
                "candidate_message_id": message_ids[2],
                "candidate_message_ids": [message_ids[2], message_ids[3]],
                "before_messages": 1,
                "after_messages": 1,
            },
        }
    ]
    assert result.stage_traces[1].metadata["candidate_message_ids"] == [
        message_ids[2],
        message_ids[3],
    ]


@pytest.mark.asyncio
async def test_reply_decision_marks_other_target_only_mention_candidates(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    before_id = _insert_message(db, raw_text=">?", created_at=1000.0)
    candidate_id = _insert_message(
        db,
        raw_text="@月梅塩絮 风子",
        content_json=(
            '[{"type":"quote","attrs":{"id":"357983158"}},'
            '{"type":"at","attrs":{"id":"2898394893","name":"月梅塩絮"}},'
            '{"type":"text","attrs":{"content":" 风子"}}]'
        ),
        sender_id="2085430718",
        sender_name="襁褓而从心所欲",
        created_at=2000.0,
    )
    after_id = _insert_message(db, raw_text="?", created_at=3000.0)
    for message_id in [before_id, candidate_id, after_id]:
        db.agent_scheduler.add_unread(
            UnreadMessage(
                session_id="bot:group:room",
                message_log_id=message_id,
                sender_id="user-1",
                created_at=float(message_id),
                self_platform_id="bot-self",
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
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=3,
            reply_context_before_messages=1,
            reply_context_after_messages=1,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([candidate_id]),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    metadata = reply_runner.calls[0]["metadata"]
    assert metadata["self_platform_id"] == "bot-self"
    assert metadata["other_target_only_candidate_message_ids"] == [candidate_id]
    assert metadata["candidate_has_other_target_only"] is True
    assert metadata["candidate_target_facts"] == [
        {
            "message_id": candidate_id,
            "sender_id": "2085430718",
            "mentions_bot": False,
            "mentions_other": True,
            "poke_to_bot": False,
            "poke_to_other": False,
            "targeted_to_bot": False,
            "targeted_to_other_only": True,
            "other_target_ids": ["2898394893"],
            "text_without_target_markers": "风子",
        }
    ]


@pytest.mark.asyncio
async def test_reply_decision_receives_target_and_adjacent_block_digests(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 7)
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
    reply_runner = RecordingReplyDecisionRunner()
    block_digest_runner = RecordingBlockDigestRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=2,
            reply_context_before_messages=0,
            reply_context_after_messages=0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_ids[-1]]),
        block_digest_runner=block_digest_runner,
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert [call["message_ids"] for call in block_digest_runner.calls] == [
        message_ids[:2],
        message_ids[2:4],
        message_ids[4:],
    ]
    block_digests = reply_runner.calls[0]["metadata"]["block_digests"]
    assert [item["block_index"] for item in block_digests] == [1, 2]
    assert [item["msg_log_start"] for item in block_digests] == [
        message_ids[2],
        message_ids[4],
    ]
    assert "digest_0" not in reply_runner.calls[0]["metadata"]["previous_summary"]
    assert "digest_1" in reply_runner.calls[0]["metadata"]["previous_summary"]
    assert "digest_2" in reply_runner.calls[0]["metadata"]["previous_summary"]


@pytest.mark.asyncio
async def test_block_digest_runner_concurrency_is_bounded(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 7)
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
    block_digest_runner = SlowBlockDigestRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            review_block_digest_concurrency=2,
        ),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([]),
        block_digest_runner=block_digest_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert block_digest_runner.max_active_count <= 2


@pytest.mark.asyncio
async def test_block_digest_failure_does_not_block_scan_or_reply(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 4)
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
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=3),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_ids[-1]]),
        block_digest_runner=FailingBlockDigestRunner(),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.failed is False
    assert result.scan.candidate_message_ids == [message_ids[-1]]
    assert result.reply.target_message_ids == [message_ids[-1]]
    assert "block_digests" not in reply_runner.calls[0]["metadata"]


@pytest.mark.asyncio
async def test_block_digest_tasks_register_in_task_scope(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_ids = [
        _insert_message(db, raw_text=f"m{index}", created_at=float(index * 1000))
        for index in range(1, 4)
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
    task_manager = AgentTaskManager()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        context_builder=RecordingReviewContextBuilder(),
        now=lambda: 5.0,
        block_digest_task_scope=task_manager.scope("agent:test:review_block_digest"),
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    assert result.scan.scanned_message_count == 3
    assert task_manager.tasks(prefix="agent:test:review_block_digest") == []


@pytest.mark.asyncio
async def test_reply_decision_uses_latest_active_chat_summary(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    message_id = _insert_message(db, raw_text="m1", created_at=1000.0)
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
    summary_service = FakeSummaryService()
    summary_service.session_summaries = [
        type("Summary", (), {"created_at": 1.0, "content": "old active summary"})(),
        type("Summary", (), {"created_at": 4.0, "content": "new active summary"})(),
    ]
    reply_runner = RecordingReplyDecisionRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            active_chat_summary_max_age_seconds=10,
        ),
        message_store=DatabaseReviewMessageStore(db),
        summary_service=summary_service,
        context_builder=RecordingReviewContextBuilder(),
        scan_runner=FixedCandidateScanRunner([message_id]),
        reply_runner=reply_runner,
        now=lambda: 5.0,
    )

    await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages("bot:group:room"),
    )

    metadata = reply_runner.calls[0]["metadata"]
    assert metadata["active_chat_summary"] == "new active summary"
    assert "new active summary" in metadata["previous_summary"]
    assert "old active summary" not in metadata["previous_summary"]


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
    workflow = ReviewCoordinator(
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

    assert result.bootstrap.reason == "active_chat_bootstrap_scheduled"
    assert result.completion.active_chat_state.interest_value == 15.0
    assert result.completion.active_chat_state.decay_half_life_seconds == 20.0
    await workflow.wait_pending_bootstraps()
    completed_bootstrap = workflow.last_bootstrap_result("bot:group:room")
    assert completed_bootstrap is not None
    assert completed_bootstrap.disposition == ActiveChatDisposition.ENGAGED
    assert completed_bootstrap.bootstrap_applied is True
    assert completed_bootstrap.active_chat_interest_value == 40.0
    assert completed_bootstrap.active_chat_decay_half_life_seconds == 35.0
    assert completed_bootstrap.reason == "bootstrap_selected_interest"
    assert _strip_run_id_from_calls(bootstrap_runner.calls) == [
        {
            "purpose": "active_chat_bootstrap",
            "message_ids": message_ids,
            "metadata": {
                "purpose": "active_chat_bootstrap",
                "tail_history_start_at": -6000.0,
                "tail_history_end_at": 4000.0,
                "reply_replied": False,
                "reply_message_id": None,
                "reply_message_ids": [],
                "reply_target_message_ids": [message_ids[-1]],
                "reply_reason": f"checked_{message_ids[-1]}",
            },
        }
    ]


@pytest.mark.asyncio
async def test_review_scan_persists_progress_per_batch_on_interrupt(tmp_path) -> None:
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
                self_platform_id="bot-self",
                trace_id=f"ingress:bot:{message_id}",
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
    unread_snapshot = [
        UnreadMessage(
            session_id="bot:group:room",
            message_log_id=message_id,
            sender_id="user-1",
            created_at=float(message_id),
            self_platform_id="bot-self",
            trace_id=f"ingress:bot:{message_id}",
        )
        for message_id in message_ids
    ]

    # Scan runner interrupts after the first batch (batch size 2 over 5
    # messages → batches [m1,m2], [m3,m4], [m5]; it fails on the 2nd batch).
    failing_scan_runner = _InterruptingReviewScanRunner(fail_on_call=2)
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=2),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=failing_scan_runner,
        context_builder=RecordingReviewContextBuilder(),
        now=lambda: 5.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id="bot:group:room",
        review_plan=review_plan,
        unread_messages=unread_snapshot,
    )

    # The review run failed mid-scan, but the first batch (m1, m2) was already
    # consumed durably, so only the unscanned remainder stays unread: a restart
    # resumes from the checkpoint instead of re-scanning the whole backlog.
    assert result.failed is True
    assert failing_scan_runner.calls == 2
    remaining = [
        message.message_log_id
        for message in scheduler.unread_messages("bot:group:room")
    ]
    assert remaining == message_ids[2:]
