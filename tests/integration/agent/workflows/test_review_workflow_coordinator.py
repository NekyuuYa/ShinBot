from __future__ import annotations

from review_workflow_support import (
    ActiveChatDisposition,
    AgentScheduler,
    AgentState,
    DatabaseManager,
    DatabaseReviewMessageStore,
    FailingBlockDigestRunner,
    FakeModelRuntime,
    FakeReviewScheduler,
    FakeReviewToolManager,
    FakeSummaryService,
    FixedCandidateScanRunner,
    FixedReviewPolicy,
    LLMReplyDecisionStageRunner,
    RecordingActiveChatBootstrapRunner,
    RecordingBlockDigestRunner,
    RecordingReplyDecisionRunner,
    RecordingReviewContextBuilder,
    ReviewCoordinator,
    ReviewLLMRunnerConfig,
    ReviewPlan,
    ReviewWorkflowConfig,
    SelectingReviewScanRunner,
    SlowBlockDigestRunner,
    UnreadMessage,
    YieldingReviewScanRunner,
    _insert_message,
    _make_prompt_registry,
    _strip_run_id_from_calls,
    asyncio,
    make_agent_signal,
    pytest,
)

from shinbot.agent.runners.review_models import ReplyDecisionStageOutput, ReviewScanStageOutput
from shinbot.agent.runtime.task_manager import (
    AgentTaskManager,
    AgentTaskQuiescenceStatus,
)
from shinbot.agent.workflows.chat_actions.tool_registration import SendReplyIdempotencyStore


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


class _BlockingReviewScanRunner:
    """Keep a scan stage active until the parent review is cancelled."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def run(self, _stage_input) -> ReviewScanStageOutput:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _BlockingBlockDigestRunner:
    """Expose whether cancellation reaches a parallel digest child task."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def run(self, _stage_input):
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class _CommitBlockingReplyRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = asyncio.Event()

    async def run(self, stage_input) -> ReplyDecisionStageOutput:
        self.started.set()
        await self.release.wait()
        self.completed.set()
        return ReplyDecisionStageOutput(
            replied=True,
            target_message_ids=list(stage_input.metadata["candidate_message_ids"]),
            reason="reply_side_effect_committed",
        )


class _CancellationResistantReplyRunner:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = asyncio.Event()

    async def run(self, stage_input) -> ReplyDecisionStageOutput:
        self.started.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
        self.completed.set()
        return ReplyDecisionStageOutput(
            replied=True,
            target_message_ids=list(stage_input.metadata["candidate_message_ids"]),
            reason="late_reply_side_effect",
        )


class _DeferredConsumptionReplyRunner:
    async def run(self, stage_input) -> ReplyDecisionStageOutput:
        return ReplyDecisionStageOutput(
            target_message_ids=list(stage_input.metadata["candidate_message_ids"]),
            reason="send_reply_tool_pending:in_flight",
            consumption_deferred=True,
        )


class _DeferredConsumptionScanRunner:
    async def run(self, _stage_input) -> ReviewScanStageOutput:
        return ReviewScanStageOutput(
            reason="llm_review_scan_failed",
            consumption_deferred=True,
        )


class _CommittedThenFailingReviewToolManager(FakeReviewToolManager):
    def __init__(self) -> None:
        super().__init__()
        self.store = SendReplyIdempotencyStore()
        self.committed = asyncio.Event()
        self.release_failure = asyncio.Event()
        self.send_count = 0

    async def execute(self, call):
        self.execute_calls.append(call)
        if call.tool_name != "send_reply":
            return _review_tool_result(output={"sent": True})
        key = str(call.arguments["idempotency_key"])
        claim = self.store.begin(key)
        if not claim.accepted:
            return _review_tool_result(
                output={
                    "sent": False,
                    "deduplicated": True,
                    "deduplicated_reason": claim.deduplicated_reason,
                }
            )
        self.send_count += 1
        self.store.finish(key)
        self.committed.set()
        await self.release_failure.wait()
        raise RuntimeError("reply task failed after send committed")


def _review_tool_result(*, output: dict[str, object]):
    return type(
        "FakeToolCallResult",
        (),
        {
            "success": True,
            "output": output,
            "error_code": "",
            "error_message": "",
        },
    )()


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
async def test_review_cancellation_commits_consumption_after_reply_stage(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "bot:group:room"
    message_id = _insert_message(db, raw_text="reply once", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id=session_id,
            message_log_id=message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review(session_id, now=10.0)
    reply_runner = _CommitBlockingReplyRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=1),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=FixedCandidateScanRunner([message_id]),
        reply_runner=reply_runner,
        now=lambda: 10.0,
    )
    review_task = asyncio.create_task(
        workflow.run(
            scheduler=scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=scheduler.unread_messages(session_id),
        )
    )
    await reply_runner.started.wait()

    review_task.cancel()
    await asyncio.sleep(0)
    assert review_task.done() is False
    reply_runner.release.set()

    with pytest.raises(asyncio.CancelledError):
        await review_task
    assert reply_runner.completed.is_set()
    assert scheduler.unread_messages(session_id) == []
    assert scheduler.state_for(session_id) == AgentState.REVIEW


@pytest.mark.asyncio
async def test_review_cancellation_detaches_stuck_reply_tail_without_consuming(
    tmp_path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "bot:group:room"
    message_id = _insert_message(db, raw_text="stuck reply", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id=session_id,
            message_log_id=message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review(session_id, now=10.0)
    reply_runner = _CancellationResistantReplyRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            reply_commit_timeout_seconds=0.01,
        ),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=FixedCandidateScanRunner([message_id]),
        reply_runner=reply_runner,
        now=lambda: 10.0,
    )
    review_task = asyncio.create_task(
        workflow.run(
            scheduler=scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=scheduler.unread_messages(session_id),
        )
    )
    await reply_runner.started.wait()

    started_at = asyncio.get_running_loop().time()
    review_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(review_task, timeout=0.5)

    assert asyncio.get_running_loop().time() - started_at < 0.4
    assert reply_runner.cancelled.is_set()
    assert scheduler.unread_messages(session_id) != []
    assert [task.get_name() for task in workflow.pending_reply_commit_tasks()] == [
        f"review-reply-commit:{session_id}"
    ]

    quiescence = await workflow.quiesce_session_tasks(
        session_id,
        timeout_seconds=0.0,
    )
    assert quiescence.status is AgentTaskQuiescenceStatus.TIMED_OUT
    assert quiescence.locally_confirmed_quiescent is False
    assert quiescence.remaining_task_names == (
        f"review-reply-commit:{session_id}",
    )

    shutdown_started_at = asyncio.get_running_loop().time()
    await workflow.shutdown()
    assert asyncio.get_running_loop().time() - shutdown_started_at < 0.4
    assert workflow.pending_reply_commit_tasks() != []

    reply_runner.release.set()
    await asyncio.wait_for(reply_runner.completed.wait(), timeout=0.5)
    await asyncio.sleep(0)
    assert workflow.pending_reply_commit_tasks() == []
    assert scheduler.unread_messages(session_id) != []


@pytest.mark.asyncio
async def test_review_cancellation_drains_parallel_block_digest_tasks(tmp_path) -> None:
    """Cancelling a scan cannot leave its digest model work behind."""

    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "bot:group:room"
    message_id = _insert_message(db, raw_text="pending digest", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id=session_id,
            message_log_id=message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review(session_id, now=10.0)
    scan_runner = _BlockingReviewScanRunner()
    digest_runner = _BlockingBlockDigestRunner()
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(review_scan_batch_size=1),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=scan_runner,
        block_digest_runner=digest_runner,
        now=lambda: 10.0,
    )
    review_task = asyncio.create_task(
        workflow.run(
            scheduler=scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=scheduler.unread_messages(session_id),
        )
    )
    await scan_runner.started.wait()
    await digest_runner.started.wait()

    review_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(review_task, timeout=0.5)

    assert scan_runner.cancelled.is_set()
    assert digest_runner.cancelled.is_set()


@pytest.mark.asyncio
async def test_review_defers_candidate_consumption_while_reply_is_in_flight(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "bot:group:room"
    message_id = _insert_message(db, raw_text="pending reply", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id=session_id,
            message_log_id=message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review(session_id, now=10.0)
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            deferred_consumption_retry_after_seconds=5.0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=FixedCandidateScanRunner([message_id]),
        reply_runner=_DeferredConsumptionReplyRunner(),
        now=lambda: 10.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id=session_id,
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages(session_id),
    )

    assert result.reply.consumption_deferred is True
    assert result.consumed_ranges == []
    assert result.completion is not None
    assert result.completion.state == AgentState.IDLE
    assert result.bootstrap.reason == "review_deferred_consumption_retry"
    next_plan = scheduler.review_plan_for(session_id)
    assert next_plan is not None
    assert next_plan.next_review_at == 15.0
    assert next_plan.reason == "review_deferred_consumption_retry"
    assert [item.message_log_id for item in scheduler.unread_messages(session_id)] == [
        message_id
    ]


@pytest.mark.asyncio
async def test_review_defers_unread_consumption_when_scan_model_fails(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "bot:group:room"
    message_id = _insert_message(db, raw_text="scan must be retried", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id=session_id,
            message_log_id=message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review(session_id, now=10.0)
    workflow = ReviewCoordinator(
        ReviewWorkflowConfig(
            review_scan_batch_size=1,
            deferred_consumption_retry_after_seconds=5.0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=_DeferredConsumptionScanRunner(),
        now=lambda: 10.0,
    )

    result = await workflow.run(
        scheduler=scheduler,
        session_id=session_id,
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages(session_id),
    )

    assert result.scan.consumption_deferred is True
    assert result.consumed_ranges == []
    assert result.completion is not None
    assert result.completion.state == AgentState.IDLE
    assert result.bootstrap.reason == "review_deferred_consumption_retry"
    next_plan = scheduler.review_plan_for(session_id)
    assert next_plan is not None
    assert next_plan.next_review_at == 15.0
    assert next_plan.reason == "review_deferred_consumption_retry"
    assert [item.message_log_id for item in scheduler.unread_messages(session_id)] == [
        message_id
    ]


@pytest.mark.asyncio
async def test_cancelled_review_failure_is_resumed_without_duplicate_reply(tmp_path) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    session_id = "bot:group:room"
    message_id = _insert_message(db, raw_text="reply once", created_at=1000.0)
    db.agent_scheduler.add_unread(
        UnreadMessage(
            session_id=session_id,
            message_log_id=message_id,
            sender_id="user-1",
            created_at=1.0,
        )
    )
    review_plan = FixedReviewPolicy().initial_plan(session_id=session_id, now=10.0)
    db.agent_scheduler.set_review_plan(review_plan)
    scheduler = AgentScheduler(
        response_profile_resolver=lambda _signal: "balanced",
        review_policy=FixedReviewPolicy(),
        inbox=db.agent_scheduler,
        state_store=db.agent_scheduler,
        now=lambda: 10.0,
    )
    scheduler.prepare_due_review(session_id, now=10.0)
    tool_manager = _CommittedThenFailingReviewToolManager()
    tool_call = {
        "id": "reply-1",
        "function": {
            "name": "send_reply",
            "arguments": f'{{"text": "hello", "quote_message_log_id": {message_id}}}',
        },
    }
    reply_runner = LLMReplyDecisionStageRunner(
        FakeModelRuntime(
            [
                {"execution_id": "exec-old", "tool_calls": [tool_call]},
                {"execution_id": "exec-replacement", "tool_calls": [tool_call]},
            ]
        ),
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    def make_workflow() -> ReviewCoordinator:
        return ReviewCoordinator(
            ReviewWorkflowConfig(review_scan_batch_size=1),
            message_store=DatabaseReviewMessageStore(db),
            scan_runner=FixedCandidateScanRunner([message_id]),
            reply_runner=reply_runner,
            now=lambda: 10.0,
        )

    old_workflow = make_workflow()
    frozen_unread = scheduler.unread_messages(session_id)
    old_review_task = asyncio.create_task(
        old_workflow.run(
            scheduler=scheduler,
            session_id=session_id,
            review_plan=review_plan,
            unread_messages=frozen_unread,
        )
    )
    await tool_manager.committed.wait()

    old_review_task.cancel()
    await asyncio.sleep(0)
    assert old_review_task.done() is False
    assert scheduler.unread_messages(session_id) == frozen_unread

    replacement_workflow = make_workflow()
    replacement_result = await replacement_workflow.run(
        scheduler=scheduler,
        session_id=session_id,
        review_plan=review_plan,
        unread_messages=scheduler.unread_messages(session_id),
    )

    assert replacement_result.failed is False
    assert tool_manager.send_count == 1
    assert [
        call.arguments["idempotency_key"] for call in tool_manager.execute_calls
    ] == [
        f"review:{session_id}:{message_id}:send_reply:0",
        f"review:{session_id}:{message_id}:send_reply:0",
    ]
    assert scheduler.unread_messages(session_id) == []

    tool_manager.release_failure.set()
    with pytest.raises(asyncio.CancelledError):
        await old_review_task
    assert scheduler.unread_messages(session_id) == []
    await replacement_workflow.wait_pending_bootstraps()


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
        ReviewWorkflowConfig(
            review_scan_batch_size=2,
            deferred_consumption_retry_after_seconds=5.0,
        ),
        message_store=DatabaseReviewMessageStore(db),
        scan_runner=failing_scan_runner,
        context_builder=RecordingReviewContextBuilder(),
        now=lambda: 10.0,
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
    assert result.completion is not None
    assert result.completion.state == AgentState.IDLE
    assert result.bootstrap.reason == "review_failed_retry_scheduled"
    next_plan = scheduler.review_plan_for("bot:group:room")
    assert next_plan is not None
    assert next_plan.next_review_at == 15.0
    assert next_plan.reason == "review_deferred_consumption_retry"
    remaining = [
        message.message_log_id
        for message in scheduler.unread_messages("bot:group:room")
    ]
    assert remaining == message_ids[2:]
