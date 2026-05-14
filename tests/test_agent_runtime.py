from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from shinbot.agent.coordinators.review.factory import ReviewRuntimeConfig, ReviewStageRuntimeConfig
from shinbot.agent.runners.review_scan import LLMReviewScanStageRunner
from shinbot.agent.runtime import install_agent_runtime
from shinbot.agent.runtime.config import (
    AgentRuntimeConfigError,
    agent_runtime_config_from_mapping,
    load_agent_runtime_config,
    validate_agent_runtime_config_mapping,
    validate_agent_runtime_config_references,
)
from shinbot.agent.scheduler import ActiveChatState, AgentScheduler, AgentState
from shinbot.agent.services.message_formatter import ImageMode
from shinbot.agent.services.model_runtime import GenerateResult
from shinbot.agent.services.prompt_engine import (
    PromptComponent,
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
)
from shinbot.core.application.app import ShinBot
from shinbot.core.dispatch.dispatchers import AgentEntrySignal
from shinbot.persistence import DatabaseManager
from shinbot.persistence.records import (
    InstanceConfigRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
)


class FakeModelRuntime:
    def __init__(self, responses: list[GenerateResult]) -> None:
        self.responses = list(responses)
        self.calls: list[Any] = []
        self.on_generate: Any | None = None

    async def generate(self, call: Any) -> GenerateResult:
        self.calls.append(call)
        if self.on_generate is not None:
            await self.on_generate(call)
        return self.responses.pop(0)


class RecordingWorkflowDispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def run_active_reply(
        self,
        *,
        session_id: str,
        message_log_id: int,
        sender_id: str,
        **kwargs: Any,
    ) -> None:
        self.calls.append(
            {
                "session_id": session_id,
                "message_log_id": message_log_id,
                "sender_id": sender_id,
                **kwargs,
            }
        )


def make_signal(
    *,
    message_log_id: int = 123,
    instance_id: str = "test-bot",
    bot_id: str = "",
    is_private: bool = False,
    is_mentioned: bool = False,
    is_reply_to_bot: bool = False,
) -> AgentEntrySignal:
    return AgentEntrySignal(
        session_id="test-bot:group:group:1",
        message_log_id=message_log_id,
        event_type="message-created",
        sender_id="user-1",
        instance_id=instance_id,
        platform="mock",
        self_id="bot-1",
        is_private=is_private,
        is_mentioned=is_mentioned,
        is_reply_to_bot=is_reply_to_bot,
        bot_id=bot_id,
    )


def make_tool_call(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": f"call-{name}",
        "type": "function",
        "function": {
            "name": name,
            "arguments": json.dumps(arguments),
        },
    }


def make_generate_result(
    *,
    text: str = "",
    tool_calls: list[dict[str, Any]] | None = None,
) -> GenerateResult:
    return GenerateResult(
        text=text,
        tool_calls=list(tool_calls or []),
        raw_response={},
        execution_id="exec-active-chat",
        route_id="",
        provider_id="",
        model_id="",
        usage={},
    )


def seed_model_registry(db: DatabaseManager, *, route_id: str = "chat.default") -> None:
    db.model_registry.upsert_provider(
        ModelProviderRecord(
            id="test-provider",
            type="openai",
            display_name="Test Provider",
            auth={"api_key": "secret-key"},
        )
    )
    db.model_registry.upsert_model(
        ModelDefinitionRecord(
            id="test-provider/gpt-fast",
            provider_id="test-provider",
            litellm_model="openai/gpt-4.1-mini",
            display_name="GPT Fast",
            capabilities=["chat"],
        )
    )
    db.model_registry.upsert_route(
        ModelRouteRecord(id=route_id, purpose="chat", strategy="priority"),
        members=[
            ModelRouteMemberRecord(
                route_id=route_id,
                model_id="test-provider/gpt-fast",
                priority=10,
            )
        ],
    )


def make_prompt_registry(
    *component_ids: str,
    stage: PromptStage = PromptStage.SYSTEM_BASE,
) -> PromptRegistry:
    registry = PromptRegistry()
    for component_id in component_ids:
        registry.register_component(
            PromptComponent(
                id=component_id,
                stage=stage,
                kind=PromptComponentKind.STATIC_TEXT,
                content=f"{component_id} content",
            )
        )
    return registry


class RecordingScheduler:
    def __init__(self) -> None:
        self.calls: list[AgentEntrySignal] = []

    async def accept_signal(self, signal: AgentEntrySignal) -> None:
        self.calls.append(signal)


def test_agent_runtime_config_mapping_wires_runtime_knobs(tmp_path: Path) -> None:
    config = agent_runtime_config_from_mapping(
        {
            "agent": {
                "id": "full-agent",
                "prompt_files": {
                    "locale": "en-US",
                    "fallback_locales": ["zh-CN"],
                    "data_root": "custom-prompts",
                },
                "defaults": {
                    "llm": "[route]route-default",
                    "max_model_retries": 2,
                    "retry_backoff_seconds": 0.5,
                    "params": {"temperature": 0.2},
                    "message_format": {
                        "image_mode": "thumbnail",
                        "include_sender": False,
                        "include_message_id": True,
                    },
                },
                "review": {
                    "scan_batch_size": 7,
                    "mention_wake_count": 3,
                    "scan": {
                        "llm": "[model]model-scan",
                        "prompts": {
                            "system": "review.custom.system",
                            "task": ["review.custom.task"],
                        },
                    },
                    "reply_decision": {
                        "prompts": {
                            "repair": "review.reply_decision.repair",
                        },
                        "tools": {
                            "extra": ["search_memory"],
                            "tags": ["knowledge"],
                        },
                    },
                },
                "summaries": {
                    "active_chat_summary_max_age_seconds": 999,
                    "markdown": {
                        "enabled": True,
                        "dir": "summary-docs",
                    },
                },
                "active_chat": {
                    "initial_interest": 42,
                    "half_life_seconds": 60,
                    "interest_delta": {
                        "mention_other": 2,
                        "poke": 4,
                        "send_reply": 11,
                        "no_reply": -6,
                    },
                    "attention": {
                        "threshold": 9,
                        "semantic_wait_ms": 123,
                    },
                    "fast_mode": {
                        "llm": "[route]route-fast",
                        "params": {"top_p": 0.8},
                        "tools": {
                            "extra": ["lookup_user_profile"],
                            "tags": ["utility"],
                        },
                    },
                },
            }
        },
        data_dir=tmp_path,
    )

    assert config.agent_id == "full-agent"
    assert config.prompt_file_config is not None
    assert config.prompt_file_config.data_root == tmp_path / "custom-prompts"
    assert config.default_message_format_config.image_mode == ImageMode.THUMBNAIL
    assert config.default_message_format_config.inject_sender is False
    assert config.default_message_format_config.inject_record_id is True
    assert config.review_workflow_config.review_scan_batch_size == 7
    assert config.review_workflow_config.active_chat_summary_max_age_seconds == 999
    assert config.summary_markdown_config.enabled is True
    assert config.summary_markdown_config.directory == tmp_path / "summary-docs"
    assert config.agent_scheduler_config.mention_wake_count == 3
    assert config.review_runtime_config.review_scan.llm == "[model]model-scan"
    assert config.review_runtime_config.review_scan.default_llm == "[route]route-default"
    assert config.review_runtime_config.review_scan.max_model_retries == 2
    assert config.review_runtime_config.reply_decision.llm == ""
    assert config.review_runtime_config.reply_decision.default_llm == "[route]route-default"
    assert config.review_runtime_config.reply_decision.tool_config.extra_names == (
        "search_memory",
    )
    assert config.review_runtime_config.reply_decision.tool_config.extra_tags == ("knowledge",)
    assert config.review_runtime_config.reply_decision.special_prompt_ids == {
        "repair": "review.reply_decision.repair",
    }
    assert config.review_runtime_config.review_scan.component_ids_by_stage == {
        PromptStage.SYSTEM_BASE: ["review.custom.system"],
        PromptStage.INSTRUCTIONS: ["review.custom.task"],
    }
    assert config.active_chat_policy_config.initial_interest_value == 42
    assert config.active_chat_policy_config.decay_half_life_seconds == 60
    assert config.active_chat_policy_config.mention_other_interest_delta == 2
    assert config.active_chat_policy_config.poke_interest_delta == 4
    assert config.active_chat_attention_config.base_threshold == 9
    assert config.active_chat_attention_config.semantic_wait_ms == 123
    assert config.active_chat_interest_effect_config.send_reply_delta == 11
    assert config.active_chat_interest_effect_config.no_reply_delta == -6
    assert config.active_chat_fast_runner_config.llm == "[route]route-fast"
    assert config.active_chat_fast_runner_config.default_llm == "[route]route-default"
    assert config.active_chat_fast_runner_config.special_prompt_ids == {}
    assert config.active_chat_fast_runner_config.params == {
        "temperature": 0.2,
        "top_p": 0.8,
    }
    assert config.active_chat_fast_runner_config.tool_config.extra_names == (
        "lookup_user_profile",
    )
    assert config.active_chat_fast_runner_config.tool_config.extra_tags == ("utility",)


def test_agent_runtime_config_schema_accepts_example(tmp_path: Path) -> None:
    source = Path("agent.example.toml")
    config_path = tmp_path / "full-agent.toml"
    config_path.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    config = load_agent_runtime_config(config_path, data_dir=tmp_path)

    assert config.agent_id == "full-agent"
    assert config.active_chat_policy_config.initial_interest_value == 15
    assert config.review_runtime_config.reply_decision.special_prompt_ids == {
        "repair": "review.reply_decision.repair",
    }
    assert config.active_chat_fast_runner_config.special_prompt_ids == {
        "repair": "active_chat.fast_mode.repair",
        "conversation_summary": "active_chat.fast_mode.conversation_summary",
        "handoff_overflow": "active_chat.handoff.overflow",
        "handoff_digest": "active_chat.handoff.digest",
        "handoff_legacy": "active_chat.handoff.legacy",
    }


def test_agent_runtime_config_schema_rejects_unknown_fields() -> None:
    issues = validate_agent_runtime_config_mapping(
        {
            "agent": {
                "id": "bad-agent",
                "review": {"unknown": True},
                "active_chat": {
                    "interest_delta": {"not_real": 1},
                    "fast_mode": {"route_id": "old-route"},
                },
            }
        }
    )

    assert [(issue.path, issue.code) for issue in issues] == [
        ("agent.review.unknown", "unknown"),
        ("agent.active_chat.interest_delta.not_real", "unknown"),
        ("agent.active_chat.fast_mode.route_id", "unknown"),
    ]


def test_agent_runtime_config_reference_validation_rejects_unknown_prompt_slots() -> None:
    issues = validate_agent_runtime_config_references(
        {
            "agent": {
                "review": {
                    "scan": {
                        "prompts": {
                            "repair": "review.reply_decision.repair",
                            "typo": "review.scan.system",
                        },
                    },
                    "reply_decision": {
                        "prompts": {
                            "repair": "review.reply_decision.repair",
                        }
                    },
                },
                "active_chat": {
                    "fast_mode": {
                        "prompts": {
                            "handoff_digest": "active_chat.handoff.digest",
                            "typo": "active_chat.fast_mode.system",
                        }
                    }
                },
            }
        }
    )

    assert [(issue.path, issue.code) for issue in issues] == [
        ("agent.review.scan.prompts.repair", "unknown_prompt_slot"),
        ("agent.review.scan.prompts.typo", "unknown_prompt_slot"),
        ("agent.active_chat.fast_mode.prompts.typo", "unknown_prompt_slot"),
    ]


def test_agent_runtime_config_reference_validation_checks_llm_and_prompt_refs(
    tmp_path: Path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    seed_model_registry(db, route_id="chat.default")
    registry = PromptRegistry()
    registry.register_component(
        PromptComponent(
            id="review.custom.system",
            stage=PromptStage.SYSTEM_BASE,
            kind=PromptComponentKind.STATIC_TEXT,
            content="system content",
        )
    )
    registry.register_component(
        PromptComponent(
            id="active_chat.wrong.stage",
            stage=PromptStage.INSTRUCTIONS,
            kind=PromptComponentKind.STATIC_TEXT,
            content="wrong stage content",
        )
    )

    issues = validate_agent_runtime_config_references(
        {
            "agent": {
                "defaults": {"llm": "[route]chat.default"},
                "review": {
                    "scan": {
                        "llm": "[model]missing-model",
                        "prompts": {
                            "system": "review.custom.system",
                            "task": "review.missing.task",
                        },
                    }
                },
                "active_chat": {
                    "fast_mode": {
                        "llm": "missing-untagged",
                        "prompts": {
                            "system": [
                                "review.custom.system",
                                "active_chat.missing.system",
                                "active_chat.wrong.stage",
                            ],
                        },
                    }
                },
            }
        },
        model_registry=db.model_registry,
        prompt_registry=registry,
    )

    assert [(issue.path, issue.code) for issue in issues] == [
        ("agent.review.scan.llm", "unknown_model"),
        ("agent.active_chat.fast_mode.llm", "unknown_llm_ref"),
        ("agent.review.scan.prompts.task", "unknown_prompt_component"),
        ("agent.active_chat.fast_mode.prompts.system.1", "unknown_prompt_component"),
        ("agent.active_chat.fast_mode.prompts.system.2", "prompt_stage"),
    ]


def test_agent_runtime_rejects_invalid_agent_config_references(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    seed_model_registry(bot.database, route_id="chat.default")

    with pytest.raises(AgentRuntimeConfigError, match="review\\.missing\\.system"):
        install_agent_runtime(
            bot,
            agent_configs_by_bot_id={
                "bot-a": {
                    "agent": {
                        "id": "agent-a",
                        "defaults": {"llm": "[route]chat.default"},
                        "review": {
                            "scan": {
                                "prompts": {"system": "review.missing.system"},
                            }
                        },
                    }
                }
            },
        )


def test_agent_runtime_rejects_missing_llm_reference(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    seed_model_registry(bot.database, route_id="chat.default")

    with pytest.raises(AgentRuntimeConfigError, match="missing-route"):
        install_agent_runtime(
            bot,
            agent_configs_by_bot_id={
                "bot-a": {
                    "agent": {
                        "id": "agent-a",
                        "defaults": {"llm": "[route]missing-route"},
                    }
                }
            },
        )


def test_agent_runtime_wires_review_runner_config(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        review_runtime_config=ReviewRuntimeConfig(
            review_scan=ReviewStageRuntimeConfig(
                enabled=True,
                route_id="route-a",
                model_id="model-a",
            ),
        ),
    )

    dispatcher = runtime.agent_scheduler._workflow_dispatcher
    workflow = dispatcher._review_coordinator

    assert isinstance(workflow._scan_runner, LLMReviewScanStageRunner)
    assert workflow._scan_runner._config.route_id == "route-a"
    assert workflow._scan_runner._config.model_id == "model-a"
    assert workflow._context_builder._context_manager is None
    assert workflow._scan_runner._template._message_formatter is runtime.message_formatter


def test_agent_runtime_accepts_review_runner_config_mapping(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        review_runtime_config={
            "review_scan": {
                "enabled": True,
                "route_id": "route-a",
            },
        },
    )

    dispatcher = runtime.agent_scheduler._workflow_dispatcher
    workflow = dispatcher._review_coordinator

    assert isinstance(workflow._scan_runner, LLMReviewScanStageRunner)
    assert workflow._scan_runner._config.route_id == "route-a"


@pytest.mark.asyncio
async def test_agent_runtime_selects_profile_by_bot_id(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {
                "agent": {
                    "id": "agent-a",
                    "active_chat": {"initial_interest": 77},
                }
            }
        },
    )
    bot_a_scheduler = RecordingScheduler()
    default_scheduler = RecordingScheduler()
    runtime.agent_profile_for_bot("bot-a").agent_scheduler = bot_a_scheduler
    runtime.agent_scheduler = default_scheduler

    await runtime.handle_agent_entry(make_signal(bot_id="bot-a"))
    await runtime.handle_agent_entry(make_signal(bot_id="bot-b"))

    assert runtime.agent_profile_for_bot("bot-a").profile_id == "agent-a"
    assert (
        runtime.agent_profile_for_bot("bot-a")
        .config.active_chat_policy_config.initial_interest_value
        == 77
    )
    assert [signal.bot_id for signal in bot_a_scheduler.calls] == ["bot-a"]
    assert [signal.bot_id for signal in default_scheduler.calls] == ["bot-b"]


def test_agent_runtime_syncs_builtin_prompt_files_to_data_dir(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)

    runtime_prompt = tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md"
    component = runtime.prompt_registry.get_component("review.review_scan.task")

    assert runtime_prompt.exists()
    assert component is not None
    assert component.metadata["prompt_file"] == str(runtime_prompt)
    assert component.metadata["runtime_prompt_file"] == str(runtime_prompt)
    assert "review_scan" in component.metadata["source_prompt_file"]


def test_agent_runtime_uses_existing_data_prompt_without_overwrite(tmp_path: Path) -> None:
    runtime_dir = tmp_path / "prompts" / "zh-CN"
    runtime_dir.mkdir(parents=True)
    runtime_prompt = runtime_dir / "review.review_scan.task.md"
    runtime_prompt.write_text(
        """---
id: review.review_scan.task
stage: instructions
kind: static_text
priority: 100
enabled: true
---

用户自定义 review scan prompt。
""",
        encoding="utf-8",
    )

    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    component = runtime.prompt_registry.get_component("review.review_scan.task")

    assert component is not None
    assert component.content == "用户自定义 review scan prompt。"
    assert runtime_prompt.read_text(encoding="utf-8").endswith(
        "用户自定义 review scan prompt。\n"
    )


def test_agent_runtime_reload_prompt_files_picks_up_data_edits(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    runtime_prompt = tmp_path / "prompts" / "zh-CN" / "review.review_scan.task.md"

    text = runtime_prompt.read_text(encoding="utf-8")
    runtime_prompt.write_text(
        text.replace("评估提供的未读消息", "用户在 WebUI 中修改后的审查提示"),
        encoding="utf-8",
    )

    runtime.reload_prompt_files()
    component = runtime.prompt_registry.get_component("review.review_scan.task")

    assert component is not None
    assert "用户在 WebUI 中修改后的审查提示" in component.content


@pytest.mark.asyncio
async def test_agent_runtime_without_database_shutdown_is_noop() -> None:
    bot = ShinBot()
    runtime = install_agent_runtime(bot)

    await runtime.shutdown()

    assert runtime.review_coordinator is None
    assert runtime.active_chat_workflow.active_session_ids() == []


@pytest.mark.asyncio
async def test_agent_runtime_resolves_response_profile_from_agent_boundary(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )
    bot.database.instance_configs.upsert(
        InstanceConfigRecord(
            uuid="cfg-group-profile",
            instance_id="test-bot",
            config={
                "response_profile_group": "passive",
                "response_profile_priority": "balanced",
                "response_profile_private": "disabled",
            },
        )
    )

    await runtime.handle_agent_entry(make_signal())
    await runtime.handle_agent_entry(make_signal(is_mentioned=True))
    await runtime.handle_agent_entry(make_signal(is_private=True))

    assert [call["response_profile"] for call in dispatcher.calls] == [
        "balanced",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_skips_unusable_agent_entry_signals(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )

    await runtime.handle_agent_entry(make_signal(is_reply_to_bot=True))
    await runtime.handle_agent_entry(make_signal(is_mentioned=True, is_private=False))
    await runtime.handle_agent_entry(
        AgentEntrySignal(
            session_id="test-bot:group:group:1",
            message_log_id=None,
            event_type="message-created",
            sender_id="user-1",
            instance_id="test-bot",
            platform="mock",
            self_id="bot-1",
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
    )

    assert [call["response_profile"] for call in dispatcher.calls] == [
        "immediate",
        "immediate",
    ]


@pytest.mark.asyncio
async def test_agent_runtime_records_ordinary_messages_without_active_reply(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    runtime = install_agent_runtime(bot)
    dispatcher = RecordingWorkflowDispatcher()
    runtime.agent_scheduler = AgentScheduler(
        workflow_dispatcher=dispatcher,
        response_profile_resolver=runtime._resolve_response_profile,
    )

    await runtime.handle_agent_entry(make_signal())

    assert dispatcher.calls == []
    assert [
        item.message_log_id
        for item in runtime.agent_scheduler.unread_messages("test-bot:group:group:1")
    ] == [123]


@pytest.mark.asyncio
async def test_agent_runtime_wires_active_chat_fast_runner_end_to_end(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "watching the live chat"},
                    )
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    bot.database.instance_configs.upsert(
        InstanceConfigRecord(
            uuid="cfg-active-chat-runtime",
            instance_id="test-bot",
            main_llm="route-main",
            config={"explicit_prompt_cache_enabled": True},
        )
    )
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-1",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot hello",
            content_json="[]",
            role="user",
            created_at=10_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=3,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 1
        call = model_runtime.calls[0]
        assert call.purpose == "active_chat_fast"
        assert call.route_id == "route-main"
        assert call.metadata["message_log_ids"] == [message_log_id]
        assert call.metadata["explicit_prompt_cache_enabled"] is True
        assert {
            tool["function"]["name"]
            for tool in call.tools
        } >= {"send_reply", "no_reply", "send_poke", "exit_active"}
        assert "request_think_mode" not in {
            tool["function"]["name"]
            for tool in call.tools
        }
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert state.pending_buffer == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_keeps_active_chat_pending_unread_on_exit(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call("no_reply", {"internal_summary": "unused"}),
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-2",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot are you there?",
            content_json="[]",
            role="user",
            created_at=20_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=4,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert [message.message_log_id for message in state.pending_buffer] == [message_log_id]

        decision = runtime.agent_scheduler.adjust_active_chat_interest(
            session_id,
            force_exit=True,
            reason="test_exit_before_batch",
        )

        assert decision.returned_to_idle is True
        unread_message_ids = [
            message.message_log_id
            for message in runtime.agent_scheduler.unread_messages(session_id)
        ]
        assert unread_message_ids == [message_log_id]
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        assert model_runtime.calls == []
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_repair_merges_active_chat_pending_messages(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(text="I would answer without a tool."),
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "no_reply",
                        {"internal_summary": "merged live batch"},
                    )
                ]
            ),
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    first_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-3",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot first",
            content_json="[]",
            role="user",
            created_at=30_000.0,
            is_mentioned=True,
        )
    )
    second_message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-4",
            sender_id="user-2",
            sender_name="User 2",
            raw_text="@bot second",
            content_json="[]",
            role="user",
            created_at=31_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=5,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    async def inject_pending_message(_call: Any) -> None:
        if len(model_runtime.calls) != 1:
            return
        await runtime.handle_agent_entry(
            make_signal(message_log_id=second_message_log_id, is_mentioned=True)
        )

    model_runtime.on_generate = inject_pending_message

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=first_message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 2
        assert model_runtime.calls[0].metadata["message_log_ids"] == [first_message_log_id]
        assert model_runtime.calls[0].metadata["repair_attempt"] == 0
        assert model_runtime.calls[1].metadata["message_log_ids"] == [
            first_message_log_id,
            second_message_log_id,
        ]
        assert model_runtime.calls[1].metadata["repair_attempt"] == 1
        assert runtime.agent_scheduler.unread_messages(session_id) == []
        state = runtime.active_chat_workflow.attention_state_for(session_id)
        assert state is not None
        assert state.pending_buffer == []
        assert state.conversation_messages[0]["tool_calls"][0]["function"]["name"] == (
            "no_reply"
        )
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_agent_runtime_exit_active_returns_idle_with_review_plan(
    tmp_path: Path,
) -> None:
    bot = ShinBot(data_dir=tmp_path)
    model_runtime = FakeModelRuntime(
        [
            make_generate_result(
                tool_calls=[
                    make_tool_call(
                        "exit_active",
                        {"reason": "conversation has clearly ended"},
                    )
                ]
            )
        ]
    )
    bot.mount_model_runtime(model_runtime)
    runtime = install_agent_runtime(bot)
    session_id = "test-bot:group:group:1"
    message_log_id = bot.database.message_logs.insert(
        MessageLogRecord(
            session_id=session_id,
            platform_msg_id="platform-msg-5",
            sender_id="user-1",
            sender_name="User",
            raw_text="@bot bye",
            content_json="[]",
            role="user",
            created_at=40_000.0,
            is_mentioned=True,
        )
    )
    active_state = ActiveChatState(
        session_id=session_id,
        interest_value=60.0,
        decay_half_life_seconds=20.0,
        entered_at=10.0,
        updated_at=10.0,
        active_epoch=6,
    )
    runtime.agent_scheduler._state_store.set_state(session_id, AgentState.ACTIVE_CHAT)
    runtime.agent_scheduler._state_store.set_active_chat_state(active_state)
    await runtime.active_chat_workflow.start_active_chat(
        session_id=session_id,
        active_chat_state=active_state,
    )

    try:
        await runtime.handle_agent_entry(
            make_signal(message_log_id=message_log_id, is_mentioned=True)
        )
        await asyncio.sleep(
            runtime.active_chat_workflow.attention_config.semantic_wait_ms / 1000.0
            + 0.1
        )

        assert len(model_runtime.calls) == 1
        assert runtime.agent_scheduler.state_for(session_id) == AgentState.IDLE
        assert runtime.agent_scheduler.active_chat_state_for(session_id) is None
        assert runtime.agent_scheduler.review_plan_for(session_id) is not None
        assert runtime.active_chat_workflow.attention_state_for(session_id) is None
        assert runtime.agent_scheduler.unread_messages(session_id) == []
    finally:
        await runtime.shutdown()
