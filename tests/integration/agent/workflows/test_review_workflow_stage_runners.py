from __future__ import annotations

from review_workflow_support import (
    ActiveChatDisposition,
    DatabaseManager,
    DatabaseReviewMessageStore,
    FakeContextManager,
    FakeModelRuntime,
    FakeSummaryService,
    LLMActiveChatBootstrapStageRunner,
    LLMOverflowCompressionStageRunner,
    LLMReplyDecisionStageRunner,
    LLMReviewBlockDigestStageRunner,
    LLMReviewScanStageRunner,
    ReviewBlockDigestStageOutput,
    ReviewContextBuilderAdapter,
    ReviewLLMRunnerConfig,
    ReviewStageInput,
    UnreadRange,
    _insert_message,
    _make_prompt_registry,
    parse_json_object,
    pytest,
)


def test_database_review_store_reads_review_windows(tmp_path) -> None:
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


def test_review_context_builder_adapter_keeps_source_messages_structured() -> None:
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
    assert stage_input.instruction_content == []
    assert stage_input.metadata == {"purpose": "review_scan"}
    assert context_manager.calls == []


def test_review_llm_json_parser_accepts_fenced_object() -> None:
    payload = parse_json_object('```json\n{"candidate_message_ids": [1], "reason": "ok"}\n```')

    assert payload == {"candidate_message_ids": [1], "reason": "ok"}


@pytest.mark.asyncio
async def test_review_llm_stage_runners_parse_structured_outputs() -> None:
    model_runtime = FakeModelRuntime(
        [
            '{"summary": "old context", "candidate_message_ids": [1, "2"], "reason": "compressed"}',
            '{"candidate_message_ids": [3, 3], "reason": "selected"}',
            '{"replied": true, "reply_message_id": 10, "target_message_ids": [3], "reason": "reply"}',
            '{"disposition": "engaged", "reason": "chat"}',
        ]
    )
    config = ReviewLLMRunnerConfig(
        route_id="route-a",
        model_id="model-a",
        caller="test.review",
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 3, "raw_text": "hello"}],
        metadata={"candidate_message_id": 3},
    )
    prompt_registry = _make_prompt_registry()

    compression = await LLMOverflowCompressionStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)
    scan = await LLMReviewScanStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)
    reply = await LLMReplyDecisionStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)
    bootstrap = await LLMActiveChatBootstrapStageRunner(
        model_runtime,
        config=config,
        prompt_registry=prompt_registry,
    ).run(stage_input)

    assert compression.summary == "old context"
    assert compression.candidate_message_ids == [1, 2]
    assert compression.reason == "compressed"
    assert scan.candidate_message_ids == [3, 3]
    assert scan.reason == "selected"
    assert reply.replied is True
    assert reply.reply_message_id == 10
    assert reply.reply_message_ids == [10]
    assert reply.target_message_ids == [3]
    assert bootstrap.disposition == ActiveChatDisposition.ENGAGED
    assert model_runtime.calls[0].route_id == "route-a"
    assert model_runtime.calls[0].model_id == "model-a"
    assert model_runtime.calls[0].caller == "test.review"
    assert model_runtime.calls[0].instance_id == "bot"
    assert model_runtime.calls[0].response_format["type"] == "json_schema"
    assert model_runtime.calls[0].metadata["candidate_message_id"] == 3


@pytest.mark.asyncio
async def test_review_compression_runner_saves_summary() -> None:
    summary_service = FakeSummaryService()
    model_runtime = FakeModelRuntime(
        ['{"summary": "older context", "candidate_message_ids": [2], "reason": "compressed"}']
    )
    runner = LLMOverflowCompressionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        summary_service=summary_service,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="overflow_compression",
            source_messages=[{"id": 1, "raw_text": "old"}, {"id": 2, "raw_text": "older"}],
            metadata={
                "review_run_id": "review-1",
                "start_msg_log_id": 1,
                "end_msg_log_id": 2,
                "message_count": 2,
            },
        )
    )

    assert result.summary == "older context"
    assert result.candidate_message_ids == [2]
    saved = summary_service.overflow_compressions[0]
    assert saved["args"] == ()
    assert saved["kwargs"]["session_id"] == "bot:group:room"
    assert saved["kwargs"]["source_run_id"] == "review-1"
    assert saved["kwargs"]["content"] == "older context"
    assert saved["kwargs"]["msg_log_start"] == 1
    assert saved["kwargs"]["msg_log_end"] == 2
    assert saved["kwargs"]["msg_count"] == 2
    assert saved["kwargs"]["metadata"]["candidate_message_ids"] == [2]


@pytest.mark.asyncio
async def test_review_block_digest_runner_saves_block_digest() -> None:
    summary_service = FakeSummaryService()
    model_runtime = FakeModelRuntime(
        ['{"summary": "block context", "reason": "digest"}']
    )
    runner = LLMReviewBlockDigestStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        summary_service=summary_service,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="review_block_digest",
            source_messages=[{"id": 10, "raw_text": "hello"}],
            metadata={
                "review_run_id": "review-1",
                "block_index": 3,
                "start_msg_log_id": 10,
                "end_msg_log_id": 19,
                "message_count": 10,
            },
        )
    )

    assert isinstance(result, ReviewBlockDigestStageOutput)
    assert result.summary == "block context"
    saved = summary_service.block_digests[0]
    assert saved["args"] == ()
    assert saved["kwargs"]["session_id"] == "bot:group:room"
    assert saved["kwargs"]["source_run_id"] == "review-1"
    assert saved["kwargs"]["block_index"] == 3
    assert saved["kwargs"]["content"] == "block context"
    assert saved["kwargs"]["msg_log_start"] == 10
    assert saved["kwargs"]["msg_log_end"] == 19
    assert saved["kwargs"]["msg_count"] == 10


@pytest.mark.asyncio
async def test_review_llm_runner_uses_prompt_registry_when_available() -> None:
    model_runtime = FakeModelRuntime(['{"candidate_message_ids": [7], "reason": "selected"}'])
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 7, "raw_text": "hello"}],
        instruction_content=[{"type": "text", "text": "rendered context"}],
        metadata={"batch": 1},
    )

    result = await runner.run(stage_input)

    assert result.candidate_message_ids == [7]
    call = model_runtime.calls[0]
    system_text = " ".join(block["text"] for block in call.messages[0]["content"])
    user_text = " ".join(block["text"] for block in call.messages[-1]["content"])
    assert "ShinBot Agent 审查流程的内部阶段" in system_text
    assert "评估提供的未读消息" in user_text
    assert "rendered context" in user_text
    assert call.tools == []
    assert call.metadata["workflow_id"] == "review"
    assert call.metadata["stage_id"] == "review_scan"
    assert call.metadata["review_stage"] == "review_scan"
    assert call.metadata["batch"] == 1
    assert "review.review_scan.instruction" in call.metadata["prompt_component_ids"]


@pytest.mark.asyncio
async def test_review_scan_runner_accepts_observed_candidate_id_aliases() -> None:
    model_runtime = FakeModelRuntime(
        [
            '{"candidate_msg_log_ids": [7], "reason": "selected"}',
            '{"selected_msg_log_ids": [7], "reason": "selected"}',
            '{"selected_message_log_ids": [8], "reason": "selected"}',
            '{"message_log_ids": [8], "reason": "selected"}',
        ]
    )
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 8, "raw_text": "hi"}],
    )

    candidate_msg_log_result = await runner.run(stage_input)
    selected_result = await runner.run(stage_input)
    selected_message_log_result = await runner.run(stage_input)
    message_log_result = await runner.run(stage_input)

    assert candidate_msg_log_result.candidate_message_ids == [7]
    assert selected_result.candidate_message_ids == [7]
    assert selected_message_log_result.candidate_message_ids == [8]
    assert message_log_result.candidate_message_ids == [8]


@pytest.mark.asyncio
async def test_review_scan_runner_filters_other_target_and_low_signal_image_candidates() -> None:
    model_runtime = FakeModelRuntime(
        ['{"candidate_message_ids": [7, 8, 9, 10, 11, 12], "reason": "selected"}']
    )
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=_make_prompt_registry(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[
            {
                "id": 7,
                "raw_text": "",
                "content_json": '[{"type":"at","attrs":{"id":"user-2","name":"Bob"}}]',
            },
            {
                "id": 8,
                "raw_text": "",
                "content_json": '[{"type":"sb:poke","attrs":{"sender_id":"user-1","target":"user-2"}}]',
            },
            {
                "id": 9,
                "raw_text": "",
                "content_json": '[{"type":"img","attrs":{"src":"missing.jpg"}}]',
            },
            {
                "id": 10,
                "raw_text": "",
                "content_json": '[{"type":"at","attrs":{"id":"bot-self","name":"Bot"}}]',
            },
            {
                "id": 11,
                "raw_text": "[@用户 user-2] 风子",
                "content_json": (
                    '[{"type":"quote","attrs":{"id":"357983158"}},'
                    '{"type":"at","attrs":{"id":"user-2","name":"Bob"}},'
                    '{"type":"text","attrs":{"content":" 风子"}}]'
                ),
            },
            {
                "id": 12,
                "raw_text": "@Bob 你刚才那张图里报错是连接超时，Shinku 你怎么看",
                "content_json": (
                    '[{"type":"at","attrs":{"id":"user-2","name":"Bob"}},'
                    '{"type":"text","attrs":{"content":" '
                    '你刚才那张图里报错是连接超时，Shinku 你怎么看"}}]'
                ),
            },
        ],
        metadata={"self_id": "bot-self"},
    )

    result = await runner.run(stage_input)

    assert result.candidate_message_ids == [10, 12]


@pytest.mark.asyncio
async def test_review_scan_runner_keeps_image_candidate_with_semantic_hint() -> None:
    class FakeMessageFormatter:
        def format_text(self, _records, _config=None) -> str:
            return "Alice: [图片: a screenshot of an error]"

    model_runtime = FakeModelRuntime(
        ['{"candidate_message_ids": [9], "reason": "selected"}']
    )
    runner = LLMReviewScanStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(),
        prompt_registry=_make_prompt_registry(),
        message_formatter=FakeMessageFormatter(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="review_scan",
        source_messages=[
            {
                "id": 9,
                "raw_text": "",
                "content_json": '[{"type":"img","attrs":{"src":"missing.jpg"}}]',
            },
        ],
        metadata={"self_id": "bot-self"},
    )

    result = await runner.run(stage_input)

    assert result.candidate_message_ids == [9]
