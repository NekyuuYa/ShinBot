from __future__ import annotations

import asyncio
import json

from review_workflow_support import (
    FakeModelRuntime,
    FakeReviewToolManager,
    LLMReplyDecisionStageRunner,
    PromptComponent,
    PromptComponentKind,
    PromptStage,
    ReviewLLMRunnerConfig,
    ReviewStageInput,
    StageToolConfig,
    _make_prompt_registry,
    pytest,
)

from shinbot.agent.services.tools import ToolManager, ToolRegistry
from shinbot.agent.workflows.chat_actions.tool_registration import (
    SendReplyIdempotencyStore,
    register_chat_action_tools,
)


class _SendHandle:
    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class _ReviewActionAdapter:
    instance_id = "bot"
    platform = "test"

    def __init__(
        self,
        *,
        block_on_attempt: int | None = None,
        fail_on_attempts: set[int] | None = None,
    ) -> None:
        self.block_on_attempt = block_on_attempt
        self.fail_on_attempts = set(fail_on_attempts or set())
        self.send_started = asyncio.Event()
        self.release_send = asyncio.Event()
        self.attempts: list[tuple[str, list[object]]] = []
        self.sent: list[tuple[str, list[object]]] = []
        self.api_calls: list[tuple[str, dict[str, object]]] = []

    async def send(self, session_id: str, elements: list[object]) -> _SendHandle:
        self.attempts.append((session_id, list(elements)))
        attempt = len(self.attempts)
        if attempt == self.block_on_attempt:
            self.send_started.set()
            await self.release_send.wait()
        if attempt in self.fail_on_attempts:
            raise RuntimeError(f"send attempt {attempt} failed")
        self.sent.append((session_id, list(elements)))
        return _SendHandle(f"platform-{len(self.sent)}")

    async def call_api(self, method: str, params: dict[str, object]) -> dict[str, object]:
        self.api_calls.append((method, dict(params)))
        return {"ok": True, "method": method, "params": params}


class _ReviewActionAdapterManager:
    def __init__(self, adapter: _ReviewActionAdapter) -> None:
        self.adapter = adapter

    def get_instance(self, instance_id: str) -> _ReviewActionAdapter | None:
        return self.adapter if instance_id == self.adapter.instance_id else None

    def is_connected(self, instance_id: str) -> bool:
        return instance_id == self.adapter.instance_id


def _real_review_tool_manager(
    adapter: _ReviewActionAdapter,
    *,
    store: SendReplyIdempotencyStore,
) -> ToolManager:
    registry = ToolRegistry()
    register_chat_action_tools(
        registry,
        adapter_manager=_ReviewActionAdapterManager(adapter),  # type: ignore[arg-type]
        send_reply_idempotency_store=store,
    )
    return ToolManager(registry)


def _reply_tool_call(
    tool_id: str,
    *,
    text: str,
    quote_message_log_id: int | None = None,
) -> dict[str, object]:
    arguments: dict[str, object] = {"text": text}
    if quote_message_log_id is not None:
        arguments.update(
            {
                "quote_message_log_id": quote_message_log_id,
                "quote_message_id": f"platform-{quote_message_log_id}",
            }
        )
    return {
        "id": tool_id,
        "function": {
            "name": "send_reply",
            "arguments": json.dumps(arguments),
        },
    }


@pytest.mark.asyncio
async def test_reply_decision_runner_exports_and_executes_terminal_tools() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    call = model_runtime.calls[0]
    tool_names = [tool["function"]["name"] for tool in call.tools]
    assert tool_names == ["no_reply", "send_reply", "send_poke", "send_reaction"]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    send_reply_tool = call.tools[1]
    send_poke_tool = call.tools[2]
    send_reaction_tool = call.tools[3]
    assert "quote_message_log_id" not in send_reply_tool["function"]["parameters"]["required"]
    assert "first send_reply" in send_reply_tool["function"]["description"]
    assert "only takes effect after at least one send_reply" in send_poke_tool["function"][
        "description"
    ]
    assert "standalone lightweight visible response" in send_reaction_tool["function"][
        "description"
    ]
    assert call.response_format is None
    assert result.replied is True
    assert result.reply_message_id == 42
    assert result.reply_message_ids == [42]
    assert result.target_message_ids == [7]
    assert result.reason == "send_reply_tool"
    assert tool_manager.execute_calls[0].tool_name == "send_reply"
    assert tool_manager.execute_calls[0].caller == "test.review"


@pytest.mark.asyncio
async def test_reply_decision_runner_adds_configured_extra_tools() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {"name": "no_reply", "arguments": "{}"},
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            caller="test.review",
            tool_config=StageToolConfig(
                extra_names=("attention.inspect_state",),
                extra_tags=("knowledge",),
            ),
        ),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    call = model_runtime.calls[0]
    tool_names = [tool["function"]["name"] for tool in call.tools]
    assert tool_names == [
        "no_reply",
        "send_reply",
        "send_poke",
        "send_reaction",
        "attention.inspect_state",
    ]
    assert tool_manager.build_request_tool_calls[0]["tags"] == {"chat_action"}
    assert "tags" not in tool_manager.build_request_tool_calls[1]
    assert tool_manager.export_model_tool_calls[-1]["tags"] == {"knowledge"}
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_executes_multiple_replies_in_order() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "first", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "second"}',
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 8, "raw_text": "world"}],
            metadata={"candidate_message_ids": [7, 8]},
        )
    )

    assert result.replied is True
    assert result.reply_message_id == 42
    assert result.reply_message_ids == [42, 43]
    assert result.target_message_ids == [7, 8]
    assert result.reason == "send_reply_tool:2"
    assert [call.tool_name for call in tool_manager.execute_calls] == [
        "send_reply",
        "send_reply",
    ]
    assert [call.arguments["text"] for call in tool_manager.execute_calls] == [
        "first",
        "second",
    ]
    assert tool_manager.execute_calls[0].arguments["quote_message_log_id"] == 7
    assert "quote_message_log_id" not in tool_manager.execute_calls[1].arguments
    assert [call.arguments["idempotency_key"] for call in tool_manager.execute_calls] == [
        "review:bot:group:room:7,8:send_reply:0",
        "review:bot:group:room:7,8:send_reply:1",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_uses_stable_reply_slots_across_model_runs() -> None:
    tool_manager = FakeReviewToolManager()
    tool_calls = [
        {
            "id": "tool-poke",
            "function": {
                "name": "send_poke",
                "arguments": '{"user_id": "user-1"}',
            },
        },
        {
            "id": "tool-reply-1",
            "function": {
                "name": "send_reply",
                "arguments": (
                    '{"text": "first", "quote_message_log_id": 8, '
                    '"idempotency_key": "model-chosen-random-key"}'
                ),
            },
        },
        {
            "id": "tool-reaction",
            "function": {
                "name": "send_reaction",
                "arguments": '{"message_log_id": 7, "emoji_id": "128077"}',
            },
        },
        {
            "id": "tool-reply-2",
            "function": {
                "name": "send_reply",
                "arguments": '{"text": "second"}',
            },
        },
    ]
    model_runtime = FakeModelRuntime(
        [
            {"execution_id": "exec-old", "tool_calls": tool_calls},
            {"execution_id": "exec-replacement", "tool_calls": tool_calls},
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="reply_decision",
        source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 8, "raw_text": "world"}],
        metadata={"candidate_message_ids": [8, 7, 8]},
    )

    await runner.run(stage_input)
    first_run_keys = [
        call.arguments["idempotency_key"]
        for call in tool_manager.execute_calls
        if call.tool_name == "send_reply"
    ]
    await runner.run(stage_input)
    second_run_keys = [
        call.arguments["idempotency_key"]
        for call in tool_manager.execute_calls
        if call.tool_name == "send_reply"
    ][2:]

    expected_keys = [
        "review:bot:group:room:7,8:send_reply:0",
        "review:bot:group:room:7,8:send_reply:1",
    ]
    assert first_run_keys == expected_keys
    assert second_run_keys == expected_keys
    assert [
        call.run_id
        for call in tool_manager.execute_calls
        if call.tool_name == "send_reply"
    ] == ["exec-old", "exec-old", "exec-replacement", "exec-replacement"]


@pytest.mark.asyncio
async def test_reply_decision_runner_defers_in_flight_slot_until_failed_send_releases() -> None:
    adapter = _ReviewActionAdapter(block_on_attempt=1, fail_on_attempts={1})
    tool_manager = _real_review_tool_manager(
        adapter,
        store=SendReplyIdempotencyStore(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="reply_decision",
        source_messages=[{"id": 7, "raw_text": "hello"}],
        metadata={"candidate_message_ids": [7]},
    )
    def make_runner(execution_id: str, text: str) -> LLMReplyDecisionStageRunner:
        return LLMReplyDecisionStageRunner(
            FakeModelRuntime(
                [
                    {
                        "execution_id": execution_id,
                        "tool_calls": [
                            _reply_tool_call(
                                f"reply-{execution_id}",
                                text=text,
                                quote_message_log_id=7,
                            )
                        ],
                    }
                ]
            ),
            config=ReviewLLMRunnerConfig(caller="test.review"),
            prompt_registry=_make_prompt_registry(),
            tool_manager=tool_manager,
        )

    old_task = asyncio.create_task(make_runner("old", "old reply").run(stage_input))
    await adapter.send_started.wait()
    replacement_result = await make_runner("replacement", "replacement reply").run(
        stage_input
    )

    assert replacement_result.replied is False
    assert replacement_result.consumption_deferred is True
    assert replacement_result.reason == "send_reply_tool_pending:in_flight"
    assert len(adapter.attempts) == 1
    assert adapter.sent == []

    adapter.release_send.set()
    old_result = await old_task
    assert old_result.replied is False
    assert old_result.reason == "reply_tool_failed:tool_execution_failed"

    retried_result = await make_runner("retry", "retry reply").run(stage_input)

    assert retried_result.replied is True
    assert retried_result.consumption_deferred is False
    assert len(adapter.attempts) == 2
    assert len(adapter.sent) == 1


@pytest.mark.asyncio
async def test_reply_decision_runner_retries_only_released_later_slot() -> None:
    adapter = _ReviewActionAdapter(fail_on_attempts={2})
    tool_manager = _real_review_tool_manager(
        adapter,
        store=SendReplyIdempotencyStore(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="reply_decision",
        source_messages=[{"id": 7, "raw_text": "hello"}],
        metadata={"candidate_message_ids": [7]},
    )

    def make_runner(execution_id: str, prefix: str) -> LLMReplyDecisionStageRunner:
        return LLMReplyDecisionStageRunner(
            FakeModelRuntime(
                [
                    {
                        "execution_id": execution_id,
                        "tool_calls": [
                            _reply_tool_call(
                                f"{prefix}-first",
                                text=f"{prefix} first",
                                quote_message_log_id=7,
                            ),
                            _reply_tool_call(
                                f"{prefix}-second",
                                text=f"{prefix} second",
                            ),
                        ],
                    }
                ]
            ),
            config=ReviewLLMRunnerConfig(caller="test.review"),
            prompt_registry=_make_prompt_registry(),
            tool_manager=tool_manager,
        )

    old_result = await make_runner("exec-old", "old").run(stage_input)
    replacement_result = await make_runner("exec-replacement", "new").run(stage_input)

    assert old_result.reason == "reply_tool_failed:tool_execution_failed"
    assert replacement_result.replied is True
    assert replacement_result.reason == "send_reply_tool:2"
    assert len(adapter.attempts) == 3
    assert len(adapter.sent) == 2


@pytest.mark.asyncio
async def test_reply_decision_runner_deduplicates_committed_companion_actions() -> None:
    adapter = _ReviewActionAdapter(fail_on_attempts={2})
    tool_manager = _real_review_tool_manager(
        adapter,
        store=SendReplyIdempotencyStore(),
    )
    stage_input = ReviewStageInput(
        session_id="bot:group:room",
        purpose="reply_decision",
        source_messages=[
            {
                "id": 7,
                "platform_msg_id": "platform-7",
                "raw_text": "hello",
            }
        ],
        metadata={"candidate_message_ids": [7]},
    )
    tool_calls = [
        _reply_tool_call(
            "reply-first",
            text="first reply",
            quote_message_log_id=7,
        ),
        {
            "id": "poke-after-reply",
            "function": {
                "name": "send_poke",
                "arguments": '{"user_id": "user-1"}',
            },
        },
        {
            "id": "reaction-after-reply",
            "function": {
                "name": "send_reaction",
                "arguments": (
                    '{"message_id": "platform-7", "emoji_id": "128077"}'
                ),
            },
        },
        _reply_tool_call("reply-second", text="second reply"),
    ]

    def make_runner(execution_id: str) -> LLMReplyDecisionStageRunner:
        return LLMReplyDecisionStageRunner(
            FakeModelRuntime(
                [{"execution_id": execution_id, "tool_calls": tool_calls}]
            ),
            config=ReviewLLMRunnerConfig(caller="test.review"),
            prompt_registry=_make_prompt_registry(),
            tool_manager=tool_manager,
        )

    first_result = await make_runner("exec-old").run(stage_input)
    replacement_result = await make_runner("exec-replacement").run(stage_input)

    assert first_result.reason == "reply_tool_failed:tool_execution_failed"
    assert replacement_result.replied is True
    assert replacement_result.reason == "send_reply_tool:2;send_poke_tool:1;send_reaction_tool"
    assert len(adapter.attempts) == 3
    assert len(adapter.sent) == 2
    assert [method for method, _params in adapter.api_calls] == [
        "internal.test.poke",
        "reaction.create",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_allows_poke_after_reply_only() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-3",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == [42]
    assert result.reason == "send_reply_tool:1;send_poke_tool:1"
    assert [call.tool_name for call in tool_manager.execute_calls] == [
        "send_reply",
        "send_poke",
    ]


@pytest.mark.asyncio
async def test_reply_decision_runner_ignores_standalone_poke() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_poke",
                            "arguments": '{"user_id": "user-1"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.reason == "llm_reply_decision_no_terminal_tool"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_allows_standalone_reaction() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reaction",
                            "arguments": '{"message_log_id": 7, "emoji_id": "128077"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == []
    assert result.target_message_ids == [7]
    assert result.reason == "send_reaction_tool"
    assert [call.tool_name for call in tool_manager.execute_calls] == ["send_reaction"]


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_reaction_to_target_candidate() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reaction",
                            "arguments": '{"message_log_id": 99, "emoji_id": "128077"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 99, "raw_text": "nearby"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reaction_tool_message_log_id_not_candidate"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_platform_reaction_to_target_candidate() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reaction",
                            "arguments": (
                                '{"message_id": "platform-nearby", "emoji_id": "128077"}'
                            ),
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[
                {"id": 7, "platform_msg_id": "platform-candidate", "raw_text": "hello"},
                {"id": 99, "platform_msg_id": "platform-nearby", "raw_text": "nearby"},
            ],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reaction_tool_platform_message_id_not_candidate"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_quoted_reply_message() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello"}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reply_tool_missing_quote_message_log_id"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_requires_first_quote_to_target_candidate() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 99}',
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}, {"id": 99, "raw_text": "nearby"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "reply_tool_quote_message_log_id_not_candidate"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_blocks_other_target_only_candidate_reply() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": (
                                '{"text": "是真红，不是风子哦", '
                                '"quote_message_log_id": 61698}'
                            ),
                        },
                    }
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[
                {
                    "id": 61698,
                    "raw_text": "@月梅塩絮 风子",
                    "content_json": (
                        '[{"type":"at","attrs":{"id":"2898394893","name":"月梅塩絮"}},'
                        '{"type":"text","attrs":{"content":" 风子"}}]'
                    ),
                }
            ],
            metadata={
                "candidate_message_ids": [61698],
                "other_target_only_candidate_message_ids": [61698],
                "candidate_target_facts": [
                    {
                        "message_id": 61698,
                        "mentions_other": True,
                        "targeted_to_other_only": True,
                        "other_target_ids": ["2898394893"],
                        "text_without_target_markers": "风子",
                    }
                ],
            },
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [61698]
    assert result.reason == "reply_tool_quote_message_log_id_targets_other_only"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_blocks_later_other_target_only_quoted_reply() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "first", "quote_message_log_id": 7}',
                        },
                    },
                    {
                        "id": "tool-2",
                        "function": {
                            "name": "send_reply",
                            "arguments": (
                                '{"text": "是真红，不是风子哦", '
                                '"quote_message_log_id": 61698}'
                            ),
                        },
                    },
                ]
            }
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[
                {"id": 7, "raw_text": "hello"},
                {
                    "id": 61698,
                    "raw_text": "@月梅塩絮 风子",
                    "content_json": (
                        '[{"type":"at","attrs":{"id":"2898394893","name":"月梅塩絮"}},'
                        '{"type":"text","attrs":{"content":" 风子"}}]'
                    ),
                },
            ],
            metadata={
                "candidate_message_ids": [7, 61698],
                "other_target_only_candidate_message_ids": [61698],
            },
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7, 61698]
    assert result.reason == "reply_tool_quote_message_log_id_targets_other_only"
    assert tool_manager.execute_calls == []


@pytest.mark.asyncio
async def test_reply_decision_runner_repairs_toolless_text_response() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(
        [
            "我应该回复一下",
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "send_reply",
                            "arguments": '{"text": "hello", "quote_message_log_id": 7}',
                        },
                    }
                ]
            },
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is True
    assert result.reply_message_ids == [42]
    assert result.reason == "send_reply_tool"
    assert len(model_runtime.calls) == 2
    repair_call = model_runtime.calls[1]
    assert repair_call.metadata["repair_attempt"] == 1
    assert repair_call.metadata["repair_reason"] == "reply_decision_toolless_output"
    assert repair_call.messages[-2] == {
        "role": "assistant",
        "content": "我应该回复一下",
    }
    repair_text = repair_call.messages[-1]["content"][0]["text"]
    assert "必须调用工具" in repair_text
    assert "第一条 send_reply 必须带 quote_message_log_id" in repair_text
    assert tool_manager.execute_calls[0].tool_name == "send_reply"


@pytest.mark.asyncio
async def test_reply_decision_runner_uses_configured_repair_prompt() -> None:
    prompt_registry = _make_prompt_registry()
    prompt_registry.register_component(
        PromptComponent(
            id="custom.reply.repair",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="Custom reply repair prompt.",
        )
    )
    model_runtime = FakeModelRuntime(
        [
            "raw text",
            {
                "tool_calls": [
                    {
                        "id": "tool-1",
                        "function": {
                            "name": "no_reply",
                            "arguments": '{"internal_summary": "fixed"}',
                        },
                    }
                ]
            },
        ]
    )
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(
            caller="test.review",
            special_prompt_ids={"repair": "custom.reply.repair"},
        ),
        prompt_registry=prompt_registry,
        tool_manager=FakeReviewToolManager(),
    )

    await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert model_runtime.calls[1].messages[-1]["content"][0]["text"] == (
        "Custom reply repair prompt."
    )


@pytest.mark.asyncio
async def test_reply_decision_runner_fails_after_toolless_repair() -> None:
    tool_manager = FakeReviewToolManager()
    model_runtime = FakeModelRuntime(["raw text", "still raw"])
    runner = LLMReplyDecisionStageRunner(
        model_runtime,
        config=ReviewLLMRunnerConfig(caller="test.review"),
        prompt_registry=_make_prompt_registry(),
        tool_manager=tool_manager,
    )

    result = await runner.run(
        ReviewStageInput(
            session_id="bot:group:room",
            purpose="reply_decision",
            source_messages=[{"id": 7, "raw_text": "hello"}],
            metadata={"candidate_message_ids": [7]},
        )
    )

    assert result.replied is False
    assert result.target_message_ids == [7]
    assert result.reason == "llm_reply_decision_toolless_after_repair"
    assert len(model_runtime.calls) == 2
    assert tool_manager.execute_calls == []
