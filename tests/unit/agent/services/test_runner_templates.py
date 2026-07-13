"""Unit tests for runner templates."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from shinbot.agent.runners.templates import (
    OneShotTextRunner,
    RunnerTemplateConfig,
    StructuredOutputRunner,
    ToolCallPlanResult,
    ToolCallPlanRunner,
    parse_tool_call_payload,
)
from shinbot.agent.runtime.instance_config import RuntimeModelTarget
from shinbot.agent.runtime.tool_config import StageToolConfig
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.model_runtime import GenerateResult, ModelCallError
from shinbot.core.instance_config import resolve_instance_runtime_config

# -- helpers --


def _stage_input(**kwargs: Any) -> ReviewStageInput:
    defaults: dict[str, Any] = {
        "session_id": "bot:group:room",
        "purpose": "review_scan",
        "source_messages": [],
        "metadata": {},
    }
    defaults.update(kwargs)
    return ReviewStageInput(**defaults)


def _generate_result(
    text: str = '{"ok": true}',
    tool_calls: list[dict[str, Any]] | None = None,
) -> GenerateResult:
    return GenerateResult(
        text=text,
        tool_calls=tool_calls or [],
        raw_response=None,
        execution_id="exec-1",
        route_id="default",
        provider_id="mock",
        model_id="mock-model",
        usage={},
    )


def _mock_prompt_registry() -> MagicMock:
    registry = MagicMock()
    registry.build_messages.return_value = MagicMock(
        messages=[{"role": "user", "content": "test"}],
        metadata={},
    )
    registry.get_component.return_value = None
    return registry


# -- RunnerTemplateConfig --


def test_runner_template_config_defaults() -> None:
    cfg = RunnerTemplateConfig()
    assert cfg.caller == "agent.review"
    assert cfg.route_id is None
    assert cfg.model_id is None
    assert cfg.profile_id == ""
    assert cfg.response_format is None
    assert cfg.component_ids_by_stage == {}
    assert cfg.params == {}


def test_runner_template_config_custom() -> None:
    cfg = RunnerTemplateConfig(
        caller="test.caller",
        response_format={"type": "object"},
        params={"temperature": 0.5},
    )
    assert cfg.caller == "test.caller"
    assert cfg.response_format == {"type": "object"}
    assert cfg.params == {"temperature": 0.5}


# -- StructuredOutputRunner --


@pytest.mark.asyncio
async def test_structured_output_runner_returns_payload() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(
        text='{"candidate_message_ids": [1, 2], "reason": "test"}'
    )
    config = RunnerTemplateConfig(
        response_format={"type": "object"},
    )
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result is not None
    assert result["candidate_message_ids"] == [1, 2]
    assert result["reason"] == "test"


@pytest.mark.asyncio
async def test_structured_output_runner_returns_model_provenance() -> None:
    """The richer API preserves exact model and prompt identifiers per call."""

    registry = _mock_prompt_registry()
    registry.build_messages.return_value.prompt_signature = "prompt-signature-a"
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    runner = StructuredOutputRunner(
        model_runtime,
        prompt_registry=registry,
        config=RunnerTemplateConfig(),
    )

    result = await runner.run_with_provenance(_stage_input())

    assert result.payload == {"ok": True}
    assert result.model_execution_id == "exec-1"
    assert result.prompt_signature == "prompt-signature-a"
    call = model_runtime.generate.call_args[0][0]
    assert call.metadata["prompt_signature"] == "prompt-signature-a"


@pytest.mark.asyncio
async def test_structured_output_runner_returns_none_on_build_failure() -> None:
    registry = _mock_prompt_registry()
    registry.build_messages.side_effect = RuntimeError("build failed")
    model_runtime = AsyncMock()
    config = RunnerTemplateConfig()
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result is None
    model_runtime.generate.assert_not_called()


@pytest.mark.asyncio
async def test_structured_output_runner_returns_none_on_llm_failure() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    config = RunnerTemplateConfig()
    model_runtime.generate.side_effect = ModelCallError("boom")
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result is None


@pytest.mark.asyncio
async def test_structured_output_runner_retries_rate_limit_once() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.side_effect = [
        ModelCallError("429 rate limit"),
        _generate_result(text='{"ok": true}'),
    ]
    config = RunnerTemplateConfig(
        max_model_retries=1,
        retry_backoff_seconds=0,
    )
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )

    result = await runner.run(_stage_input())

    assert result == {"ok": True}
    assert model_runtime.generate.call_count == 2


@pytest.mark.asyncio
async def test_structured_output_runner_returns_none_on_invalid_json() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    config = RunnerTemplateConfig()
    model_runtime.generate.return_value = _generate_result(text="not json")
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result is None


@pytest.mark.asyncio
async def test_structured_output_runner_passes_response_format() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    fmt = {"type": "object", "properties": {"ok": {"type": "boolean"}}}
    config = RunnerTemplateConfig(response_format=fmt)
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    await runner.run(_stage_input())
    call_args = model_runtime.generate.call_args[0][0]
    assert call_args.response_format == fmt


@pytest.mark.asyncio
async def test_structured_output_runner_passes_params() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    config = RunnerTemplateConfig(params={"temperature": 0.3})
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    await runner.run(_stage_input())
    call_args = model_runtime.generate.call_args[0][0]
    assert call_args.params == {"temperature": 0.3}


@pytest.mark.asyncio
async def test_structured_output_runner_applies_instance_runtime_config() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    config = RunnerTemplateConfig(
        instance_config_resolver=lambda _instance_id: resolve_instance_runtime_config(
            {
                "main_llm": "route-main",
                "config": {"explicit_prompt_cache_enabled": True},
            }
        ),
        model_target_resolver=lambda target: RuntimeModelTarget(route_id=target),
    )
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )

    await runner.run(_stage_input())

    prompt_request = registry.build_messages.call_args[0][0]
    assert prompt_request.route_id == "route-main"
    assert prompt_request.metadata["explicit_prompt_cache_enabled"] is True
    call_args = model_runtime.generate.call_args[0][0]
    assert call_args.route_id == "route-main"
    assert call_args.metadata["explicit_prompt_cache_enabled"] is True


@pytest.mark.asyncio
async def test_structured_output_runner_keeps_explicit_model_target() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    config = RunnerTemplateConfig(
        route_id="route-explicit",
        instance_config_resolver=lambda _instance_id: resolve_instance_runtime_config(
            {
                "main_llm": "route-main",
                "config": {"explicit_prompt_cache_enabled": True},
            }
        ),
        model_target_resolver=lambda target: RuntimeModelTarget(route_id=target),
    )
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )

    await runner.run(_stage_input())

    call_args = model_runtime.generate.call_args[0][0]
    assert call_args.route_id == "route-explicit"
    assert call_args.model_id is None


# -- ToolCallPlanRunner --


@pytest.mark.asyncio
async def test_tool_call_plan_runner_returns_tool_calls() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    tool_calls = [
        {"function": {"name": "send_reply", "arguments": '{"text": "hi"}'}},
    ]
    model_runtime.generate.return_value = _generate_result(
        text="", tool_calls=tool_calls,
    )
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = [
        {"function": {"name": "send_reply", "parameters": {}}},
    ]
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["send_reply"],
    )
    result = await runner.run(_stage_input())
    assert result.has_tool_calls
    assert len(result.tool_calls) == 1
    assert result.reason == "tool_call_plan"


@pytest.mark.asyncio
async def test_tool_call_plan_runner_toolless_returns_reason() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="no tools here")
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = []
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=[],
    )
    result = await runner.run(_stage_input())
    assert not result.has_tool_calls
    assert result.reason == "tool_call_plan_toolless"
    assert result.text == "no tools here"


@pytest.mark.asyncio
async def test_tool_call_plan_runner_repair_succeeds() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    tool_calls = [
        {"function": {"name": "no_reply", "arguments": "{}"}},
    ]
    # First call returns bare text, second call returns tool calls.
    model_runtime.generate.side_effect = [
        _generate_result(text="I think I should reply"),
        _generate_result(text="", tool_calls=tool_calls),
    ]
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = [
        {"function": {"name": "no_reply", "parameters": {}}},
    ]
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["no_reply"],
        repair_prompt="please use tools",
    )
    result = await runner.run(_stage_input())
    assert result.has_tool_calls
    assert result.reason == "tool_call_plan_after_repair"


@pytest.mark.asyncio
async def test_tool_call_plan_runner_no_repair_without_prompt() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="bare text")
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = [
        {"function": {"name": "no_reply", "parameters": {}}},
    ]
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["no_reply"],
        repair_prompt="",  # no repair prompt
    )
    result = await runner.run(_stage_input())
    assert not result.has_tool_calls
    assert result.reason == "tool_call_plan_toolless"


@pytest.mark.asyncio
async def test_tool_call_plan_runner_uses_response_format_without_tools() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(
        text='{"replied": false, "reason": "no tools"}'
    )
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = []
    response_format = {"type": "json_schema", "json_schema": {"name": "reply"}}
    config = RunnerTemplateConfig(
        response_format=response_format,
    )
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["send_reply"],
        repair_prompt="please use tools",
    )
    result = await runner.run(_stage_input())
    call_args = model_runtime.generate.call_args[0][0]
    assert call_args.tools == []
    assert call_args.response_format == response_format
    assert not result.has_tool_calls


@pytest.mark.asyncio
async def test_tool_call_plan_runner_skips_repair_when_disabled() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="bare text")
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = [
        {"function": {"name": "no_reply", "parameters": {}}},
    ]
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["no_reply"],
        repair_prompt="please use tools",
        max_repair_attempts=0,
    )
    result = await runner.run(_stage_input())
    assert not result.has_tool_calls
    assert result.reason == "tool_call_plan_toolless"
    assert model_runtime.generate.call_count == 1


@pytest.mark.asyncio
async def test_tool_call_plan_runner_build_tools_calls_transform() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = [
        {"function": {"name": "send_reply", "description": "original"}},
    ]

    def transform(tool: dict[str, Any]) -> dict[str, Any]:
        func = tool.get("function", {})
        return {**tool, "function": {**func, "description": func.get("description", "") + " extra"}}

    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["send_reply"],
        tool_transform=transform,
    )
    tools = runner.build_tools(_stage_input())
    assert len(tools) == 1
    assert tools[0]["function"]["description"] == "original extra"


@pytest.mark.asyncio
async def test_tool_call_plan_runner_passes_tool_tags() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="bare text")
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = []
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["send_reply"],
        tool_tags={"chat_action"},
    )

    await runner.run(_stage_input())

    assert tool_manager.build_request_tools.call_args.kwargs["tags"] == {"chat_action"}


@pytest.mark.asyncio
async def test_tool_call_plan_runner_adds_configured_extra_tools() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="bare text")
    tool_manager = MagicMock()
    tool_manager.build_request_tools.side_effect = [
        [
            {"function": {"name": "send_reply", "parameters": {}}},
        ],
        [
            {"function": {"name": "search_memory", "parameters": {}}},
        ],
    ]
    tool_manager.export_model_tools.return_value = [
        {"function": {"name": "lookup_profile", "parameters": {}}},
        {"function": {"name": "send_reply", "parameters": {}}},
    ]
    config = RunnerTemplateConfig(
        tool_config=StageToolConfig(
            extra_names=("search_memory",),
            extra_tags=("knowledge",),
        )
    )
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=["send_reply"],
        tool_tags={"chat_action"},
    )

    tools = runner.build_tools(_stage_input())

    assert [tool["function"]["name"] for tool in tools] == [
        "send_reply",
        "search_memory",
        "lookup_profile",
    ]
    assert tool_manager.build_request_tools.call_args_list[0].kwargs["tags"] == {
        "chat_action"
    }
    assert "tags" not in tool_manager.build_request_tools.call_args_list[1].kwargs
    assert tool_manager.export_model_tools.call_args.kwargs["tags"] == {"knowledge"}


@pytest.mark.asyncio
async def test_tool_call_plan_runner_llm_failure() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()

    model_runtime.generate.side_effect = ModelCallError("boom")
    tool_manager = MagicMock()
    tool_manager.build_request_tools.return_value = []
    config = RunnerTemplateConfig()
    runner = ToolCallPlanRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        tool_manager=tool_manager,
        tool_names=[],
    )
    result = await runner.run(_stage_input())
    assert not result.has_tool_calls
    assert result.reason == "tool_call_plan_llm_failed"


# -- OneShotTextRunner --


@pytest.mark.asyncio
async def test_one_shot_text_runner_returns_text() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="Hello world")
    config = RunnerTemplateConfig()
    runner = OneShotTextRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result == "Hello world"


@pytest.mark.asyncio
async def test_one_shot_text_runner_returns_none_on_empty() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="   ")
    config = RunnerTemplateConfig()
    runner = OneShotTextRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result is None


@pytest.mark.asyncio
async def test_one_shot_text_runner_returns_none_on_failure() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()

    model_runtime.generate.side_effect = ModelCallError("boom")
    config = RunnerTemplateConfig()
    runner = OneShotTextRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    result = await runner.run(_stage_input())
    assert result is None


@pytest.mark.asyncio
async def test_one_shot_text_runner_passes_no_response_format() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text="ok")
    config = RunnerTemplateConfig()
    runner = OneShotTextRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    await runner.run(_stage_input())
    call_args = model_runtime.generate.call_args[0][0]
    assert call_args.response_format is None
    assert call_args.tools == []


# -- ToolCallPlanResult --


def test_tool_call_plan_result_has_tool_calls() -> None:
    assert ToolCallPlanResult(tool_calls=[{"x": 1}]).has_tool_calls
    assert not ToolCallPlanResult().has_tool_calls
    assert not ToolCallPlanResult(tool_calls=[]).has_tool_calls


def test_parse_tool_call_payload_normalizes_json_arguments() -> None:
    parsed = parse_tool_call_payload(
        {
            "id": "call-1",
            "function": {
                "name": "send_reply",
                "arguments": '{"text": "hello", "quote_message_log_id": 7}',
            },
        }
    )

    assert parsed.name == "send_reply"
    assert parsed.arguments == {"text": "hello", "quote_message_log_id": 7}


def test_parse_tool_call_payload_handles_invalid_arguments() -> None:
    parsed = parse_tool_call_payload(
        {
            "id": "call-1",
            "function": {"name": "send_reply", "arguments": "{bad json"},
        }
    )

    assert parsed.name == "send_reply"
    assert parsed.arguments == {}


# -- prompt injection --


@pytest.mark.asyncio
async def test_structured_output_runner_includes_instruction_content() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    config = RunnerTemplateConfig()
    runner = StructuredOutputRunner(
        model_runtime, prompt_registry=registry, config=config,
    )
    stage = _stage_input(
        purpose="test_stage",
        source_messages=[{"id": 1, "text": "hello"}],
        instruction_content=[{"type": "text", "text": "rendered input"}],
        metadata={"key": "value"},
    )
    await runner.run(stage)
    build_request = registry.build_messages.call_args[0][0]
    assert build_request.component_ids_by_stage == {}
    assert build_request.metadata["review_stage"] == "test_stage"
    assert build_request.metadata["review_stage_metadata"] == {"key": "value"}
    assert "review_source_messages" not in build_request.metadata
    assert "review_instruction_content" not in build_request.metadata
    assert build_request.disabled_components == ["review.test_stage.instruction"]
    assert len(build_request.injections) == 1
    injection = build_request.injections[0]
    assert injection.component_id == "review.test_stage.instruction"
    rendered = "\n".join(str(block.get("text") or "") for block in injection.content_blocks)
    assert rendered.count("rendered input") == 1
    assert "Source messages JSON" not in rendered


@pytest.mark.asyncio
async def test_structured_output_runner_uses_message_formatter() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    formatter = MagicMock()
    formatter.format_text.return_value = "Alice: hello"
    config = RunnerTemplateConfig()
    runner = StructuredOutputRunner(
        model_runtime,
        prompt_registry=registry,
        config=config,
        message_formatter=formatter,
    )

    await runner.run(
        _stage_input(
            source_messages=[{"id": 1, "sender_id": "alice", "raw_text": "hello"}],
        )
    )

    formatter.format_text.assert_called_once()
    build_request = registry.build_messages.call_args[0][0]
    assert "review_source_messages_text" not in build_request.metadata
    assert "review_source_messages" not in build_request.metadata
    assert len(build_request.injections) == 1
    rendered = "\n".join(
        str(block.get("text") or "") for block in build_request.injections[0].content_blocks
    )
    assert rendered.count("Source messages:\nAlice: hello") == 1


@pytest.mark.asyncio
async def test_structured_output_runner_projects_context_messages_in_order() -> None:
    registry = _mock_prompt_registry()
    model_runtime = AsyncMock()
    model_runtime.generate.return_value = _generate_result(text='{"ok": true}')
    runner = StructuredOutputRunner(
        model_runtime,
        prompt_registry=registry,
        config=RunnerTemplateConfig(),
    )
    tail = [
        {"role": "user", "content": "first"},
        {"role": "assistant", "content": "second"},
        {"role": "user", "content": "third"},
    ]

    await runner.run(_stage_input(context_messages=tail))

    build_request = registry.build_messages.call_args[0][0]
    assert len(build_request.injections) == 2
    context_injection = build_request.injections[0]
    assert context_injection.stage.value == "context"
    assert context_injection.messages == tail
    assert build_request.metadata["review_input_projection"] == {
        "context_message_count": 3,
        "source_message_count": 0,
        "instruction_block_count": 0,
    }
