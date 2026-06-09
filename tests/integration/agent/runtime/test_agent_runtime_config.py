from __future__ import annotations

from agent_runtime_support import (
    AgentRuntimeConfigError,
    DatabaseManager,
    ImageMode,
    LLMReviewScanStageRunner,
    Path,
    PersonaFileRepository,
    PromptBuildRequest,
    PromptComponent,
    PromptComponentKind,
    PromptContextPolicy,
    PromptRegistry,
    PromptStage,
    ReviewRuntimeConfig,
    ReviewStageRuntimeConfig,
    ShinBot,
    agent_runtime_config_from_mapping,
    install_agent_runtime,
    load_agent_runtime_config,
    pytest,
    seed_model_registry,
    seed_persona,
    validate_agent_runtime_config_mapping,
    validate_agent_runtime_config_references,
)


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
                    "default_review_after_seconds": 12,
                    "default_review_reason": "e2e_fast_review",
                    "review_due_tick_interval_seconds": 0.5,
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
                        "send_reaction": 2.5,
                        "no_reply": -6,
                    },
                    "attention": {
                        "threshold": 9,
                        "semantic_wait_ms": 123,
                    },
                    "fast_mode": {
                        "llm": "[route]route-fast",
                        "source_context_before_messages": 12,
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
    assert config.review_policy_config.default_review_after_seconds == 12
    assert config.review_policy_config.default_reason == "e2e_fast_review"
    assert config.review_due_tick_interval_seconds == 0.5
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
    assert config.active_chat_interest_effect_config.send_reaction_delta == 2.5
    assert config.active_chat_interest_effect_config.no_reply_delta == -6
    assert config.active_chat_fast_runner_config.llm == "[route]route-fast"
    assert config.active_chat_fast_runner_config.default_llm == "[route]route-default"
    assert config.active_chat_fast_runner_config.source_context_before_messages == 12
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
    assert config.active_chat_fast_runner_config.source_context_before_messages == 50
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


def test_agent_runtime_config_reference_validation_checks_persona(
    tmp_path: Path,
) -> None:
    db = DatabaseManager.from_bootstrap(data_dir=tmp_path)
    db.initialize()
    seed_persona(db, persona_id="persona-good")
    seed_persona(db, persona_id="persona-disabled", enabled=False)
    personas = PersonaFileRepository.from_data_dir(tmp_path)

    assert validate_agent_runtime_config_references(
        {"agent": {"persona_id": "persona-good"}},
        persona_repository=personas,
    ) == []

    issues = validate_agent_runtime_config_references(
        {"agent": {"persona_id": "persona-disabled"}},
        persona_repository=personas,
    )

    assert [(issue.path, issue.code) for issue in issues] == [
        ("agent.persona_id", "disabled_ref"),
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


def test_agent_runtime_injects_configured_persona_prompt(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)
    persona_component_id = seed_persona(bot.database, persona_id="persona-agent")

    runtime = install_agent_runtime(
        bot,
        agent_configs_by_bot_id={
            "bot-a": {
                "agent": {
                    "id": "agent-a",
                    "persona_id": "persona-agent",
                }
            }
        },
    )
    profile = runtime.agent_profile_for_bot("bot-a")

    assert profile.prompt_registry.get_component(persona_component_id) is not None
    assert profile.config.review_runtime_config.review_scan.component_ids_by_stage[
        PromptStage.IDENTITY
    ] == [persona_component_id]
    assert profile.config.review_runtime_config.reply_decision.component_ids_by_stage[
        PromptStage.IDENTITY
    ] == [persona_component_id]
    assert profile.config.active_chat_fast_runner_config.component_ids_by_stage[
        PromptStage.IDENTITY
    ] == [persona_component_id]
    build_result = profile.prompt_registry.build_messages(
        PromptBuildRequest(
            caller="test.review",
            workflow_id="review",
            stage_id="review_scan",
            component_ids_by_stage=(
                profile.config.review_runtime_config.review_scan.component_ids_by_stage
            ),
            context_policy=PromptContextPolicy.DISABLED,
        )
    )
    assert "configured test persona" in str(build_result.messages)


def test_agent_runtime_rejects_missing_persona_reference(tmp_path: Path) -> None:
    bot = ShinBot(data_dir=tmp_path)

    with pytest.raises(AgentRuntimeConfigError, match="missing-persona"):
        install_agent_runtime(
            bot,
            agent_configs_by_bot_id={
                "bot-a": {
                    "agent": {
                        "id": "agent-a",
                        "persona_id": "missing-persona",
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
