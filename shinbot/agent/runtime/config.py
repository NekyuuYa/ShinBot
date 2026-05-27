"""Agent runtime config file loading and normalization."""

from __future__ import annotations

import tomllib
from copy import deepcopy
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from shinbot.agent.coordinators.active_chat.actions import ActiveChatInterestEffectConfig
from shinbot.agent.coordinators.active_chat.attention import ActiveChatAttentionConfig
from shinbot.agent.coordinators.review.factory import (
    ReviewRuntimeConfig,
    ReviewStageRuntimeConfig,
)
from shinbot.agent.coordinators.review.models import ReviewWorkflowConfig
from shinbot.agent.runtime.instance_config import parse_tagged_llm_ref
from shinbot.agent.runtime.tool_config import stage_tool_config_from_mapping
from shinbot.agent.scheduler.active_chat_policy import ActiveChatPolicyConfig
from shinbot.agent.scheduler.priority_policy import PriorityPolicyConfig
from shinbot.agent.scheduler.review_policy import ReviewPolicyConfig
from shinbot.agent.scheduler.scheduler import AgentSchedulerConfig
from shinbot.agent.services.message_formatter import (
    EmojiMode,
    ImageMode,
    MessageFormatConfig,
    PackMode,
)
from shinbot.agent.services.prompt_engine import PromptFileLoadConfig, PromptStage
from shinbot.agent.workflows.active_chat.runner import ActiveChatFastRunnerConfig
from shinbot.core.config_provider import (
    ConfigProviderKind,
    ConfigProviderRegistry,
    ConfigValidationIssue,
)


class AgentRuntimeConfigError(ValueError):
    """Raised when an agent config file cannot be loaded."""


_PROMPT_STAGE_KEYS: dict[str, PromptStage] = {
    "system": PromptStage.SYSTEM_BASE,
    "task": PromptStage.INSTRUCTIONS,
    "instructions": PromptStage.INSTRUCTIONS,
    "constraints": PromptStage.CONSTRAINTS,
    "context": PromptStage.CONTEXT,
    "abilities": PromptStage.ABILITIES,
    "compatibility": PromptStage.COMPATIBILITY,
}

_REVIEW_SPECIAL_PROMPT_KEYS = frozenset({"repair"})
_REVIEW_SPECIAL_PROMPT_STAGES: dict[str, PromptStage] = {
    "repair": PromptStage.INSTRUCTIONS,
}
_ACTIVE_CHAT_SPECIAL_PROMPT_KEYS = frozenset(
    {
        "repair",
        "conversation_summary",
        "handoff_overflow",
        "handoff_digest",
        "handoff_legacy",
    }
)
_ACTIVE_CHAT_SPECIAL_PROMPT_STAGES: dict[str, PromptStage] = {
    "repair": PromptStage.INSTRUCTIONS,
    "conversation_summary": PromptStage.CONTEXT,
    "handoff_overflow": PromptStage.CONTEXT,
    "handoff_digest": PromptStage.CONTEXT,
    "handoff_legacy": PromptStage.CONTEXT,
}

_REVIEW_STAGE_NAMES = (
    "overflow_compression",
    "scan",
    "block_digest",
    "reply_decision",
    "active_chat_bootstrap",
    "idle_review_planning",
)


@dataclass(slots=True, frozen=True)
class SummaryMarkdownConfig:
    """Markdown mirror settings for agent summaries."""

    enabled: bool = True
    directory: Path = field(default_factory=lambda: Path("summary"))


@dataclass(slots=True, frozen=True)
class AgentRuntimeConfig:
    """Runtime knobs loaded from one ``data/agents/*.toml`` file."""

    agent_id: str = ""
    mode: str = "full"
    persona_id: str = ""
    source_path: str = ""
    prompt_file_config: PromptFileLoadConfig | None = None
    default_message_format_config: MessageFormatConfig = field(
        default_factory=MessageFormatConfig
    )
    review_workflow_config: ReviewWorkflowConfig = field(default_factory=ReviewWorkflowConfig)
    review_runtime_config: ReviewRuntimeConfig = field(default_factory=ReviewRuntimeConfig)
    agent_scheduler_config: AgentSchedulerConfig = field(default_factory=AgentSchedulerConfig)
    priority_policy_config: PriorityPolicyConfig = field(default_factory=PriorityPolicyConfig)
    review_policy_config: ReviewPolicyConfig = field(default_factory=ReviewPolicyConfig)
    review_due_tick_interval_seconds: float = 5.0
    active_chat_policy_config: ActiveChatPolicyConfig = field(
        default_factory=ActiveChatPolicyConfig
    )
    active_chat_attention_config: ActiveChatAttentionConfig = field(
        default_factory=ActiveChatAttentionConfig
    )
    active_chat_interest_effect_config: ActiveChatInterestEffectConfig = field(
        default_factory=ActiveChatInterestEffectConfig
    )
    active_chat_fast_runner_config: ActiveChatFastRunnerConfig = field(
        default_factory=ActiveChatFastRunnerConfig
    )
    active_chat_conversation_message_limit: int = 80
    summary_markdown_config: SummaryMarkdownConfig = field(
        default_factory=SummaryMarkdownConfig
    )
    raw_mapping: dict[str, Any] = field(default_factory=dict)


def load_agent_runtime_config(
    path: Path | str,
    *,
    data_dir: Path | str,
) -> AgentRuntimeConfig:
    """Load one agent runtime config TOML file."""

    config_path = Path(path)
    if not config_path.exists():
        raise AgentRuntimeConfigError(f"Agent config not found: {config_path}")
    if not config_path.is_file():
        raise AgentRuntimeConfigError(f"Agent config is not a file: {config_path}")
    try:
        with config_path.open("rb") as file_obj:
            payload = tomllib.load(file_obj)
    except tomllib.TOMLDecodeError as exc:
        raise AgentRuntimeConfigError(f"Invalid agent config TOML {config_path}: {exc}") from exc
    except OSError as exc:
        raise AgentRuntimeConfigError(f"Failed to read agent config {config_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise AgentRuntimeConfigError(f"Agent config {config_path} must be a TOML table")
    issues = validate_agent_runtime_config_mapping(payload)
    if issues:
        raise AgentRuntimeConfigError(
            _format_agent_config_issues(config_path, list(issues))
        )
    return agent_runtime_config_from_mapping(
        payload,
        data_dir=Path(data_dir),
        source_path=config_path,
    )


def agent_runtime_config_from_mapping(
    payload: dict[str, Any] | None,
    *,
    data_dir: Path | str,
    source_path: Path | str | None = None,
) -> AgentRuntimeConfig:
    """Normalize an agent runtime config mapping."""

    root = Path(data_dir)
    config = payload or {}
    agent = _mapping(config.get("agent"))
    defaults = _mapping(agent.get("defaults"))
    defaults_llm = _llm_defaults_config(defaults)
    review = _mapping(agent.get("review"))
    active_chat = _mapping(agent.get("active_chat"))
    summaries = _mapping(agent.get("summaries"))
    prompt_file_config = _prompt_file_config(
        _mapping(agent.get("prompt_files")),
        data_dir=root,
    )
    message_format = _message_format_config(
        _mapping(_mapping(defaults.get("message_format")))
    )
    review_workflow_config = _review_workflow_config(review, active_chat, summaries)
    summary_markdown_config = _summary_markdown_config(
        _mapping(summaries.get("markdown")),
        data_dir=root,
    )

    scheduler_config = _agent_scheduler_config(review)
    priority_policy_config = scheduler_config.to_priority_policy_config()
    review_policy_config = _review_policy_config(review, scheduler_config)
    review_due_tick_interval_seconds = _float(
        review.get("review_due_tick_interval_seconds"),
        5.0,
    )
    active_chat_policy_config = _active_chat_policy_config(active_chat)
    active_chat_attention_config = _active_chat_attention_config(active_chat)
    interest_effect_config = _active_chat_interest_effect_config(active_chat)
    review_runtime_config = _review_runtime_config(review, message_format, defaults_llm)
    active_chat_fast_runner_config = _active_chat_fast_runner_config(
        _mapping(active_chat.get("fast_mode")),
        message_format,
        defaults_llm,
    )

    return AgentRuntimeConfig(
        agent_id=str(agent.get("id") or "").strip(),
        mode=str(agent.get("mode") or "full").strip() or "full",
        persona_id=str(agent.get("persona_id") or "").strip(),
        source_path=str(source_path or ""),
        prompt_file_config=prompt_file_config,
        default_message_format_config=message_format,
        review_workflow_config=review_workflow_config,
        review_runtime_config=review_runtime_config,
        agent_scheduler_config=scheduler_config,
        priority_policy_config=priority_policy_config,
        review_policy_config=review_policy_config,
        review_due_tick_interval_seconds=review_due_tick_interval_seconds,
        active_chat_policy_config=active_chat_policy_config,
        active_chat_attention_config=active_chat_attention_config,
        active_chat_interest_effect_config=interest_effect_config,
        active_chat_fast_runner_config=active_chat_fast_runner_config,
        active_chat_conversation_message_limit=_int(
            active_chat.get("conversation_message_limit"),
            80,
        ),
        summary_markdown_config=summary_markdown_config,
        raw_mapping=deepcopy(config),
    )


def validate_agent_runtime_config_mapping(
    payload: dict[str, Any],
    *,
    path_prefix: str = "",
) -> list[ConfigValidationIssue]:
    """Validate a raw Agent runtime config mapping against the provider schema."""

    from shinbot.agent.runtime.config_provider import (
        AGENT_RUNTIME_CONFIG_PROVIDER_ID,
        load_agent_runtime_config_provider,
    )

    registry = ConfigProviderRegistry()
    registry.register(load_agent_runtime_config_provider())
    return registry.validate(
        ConfigProviderKind.AGENT,
        AGENT_RUNTIME_CONFIG_PROVIDER_ID,
        payload,
        path_prefix=path_prefix,
        strict=True,
    )


def validate_agent_runtime_config_references(
    payload: dict[str, Any],
    *,
    model_registry: Any | None = None,
    prompt_registry: Any | None = None,
    persona_repository: Any | None = None,
    prompt_definition_repository: Any | None = None,
    path_prefix: str = "",
) -> list[ConfigValidationIssue]:
    """Validate Agent runtime config references against live registries."""

    issues: list[ConfigValidationIssue] = []
    if persona_repository is not None:
        issues.extend(
            _validate_agent_persona_ref(
                payload,
                persona_repository=persona_repository,
                path_prefix=path_prefix,
            )
        )
    if model_registry is not None:
        for path, llm_ref in _iter_agent_llm_refs(payload):
            issues.extend(
                _validate_agent_llm_ref(
                    llm_ref,
                    model_registry=model_registry,
                    path=_join_issue_path(path_prefix, path),
                )
            )
    if prompt_registry is not None:
        for path, component_id, expected_stage in _iter_agent_prompt_component_refs(payload):
            component = prompt_registry.get_component(component_id)
            if component is None:
                issues.append(
                    ConfigValidationIssue(
                        path=_join_issue_path(path_prefix, path),
                        message=f"Prompt component {component_id!r} is not registered",
                        code="unknown_prompt_component",
                    )
                )
                continue
            if component.stage != expected_stage:
                issues.append(
                    ConfigValidationIssue(
                        path=_join_issue_path(path_prefix, path),
                        message=(
                            f"Prompt component {component_id!r} belongs to stage "
                            f"{component.stage.value!r}, not {expected_stage.value!r}"
                        ),
                        code="prompt_stage",
                    )
                )
    issues.extend(_validate_agent_prompt_slots(payload, path_prefix=path_prefix))
    return issues


def _prompt_file_config(
    value: dict[str, Any],
    *,
    data_dir: Path,
) -> PromptFileLoadConfig | None:
    if not value:
        return None
    data_root = value.get("data_root") or value.get("prompt_data_root")
    resolved_data_root = Path(data_root) if data_root else data_dir / "prompts"
    if not resolved_data_root.is_absolute():
        resolved_data_root = data_dir / resolved_data_root
    return PromptFileLoadConfig(
        locale=str(value.get("locale") or "zh-CN"),
        fallback_locales=_string_tuple(value.get("fallback_locales"), default=("en-US",)),
        data_root=resolved_data_root,
        sync_to_data=_bool(value.get("sync_to_data"), True),
    )


def _message_format_config(value: dict[str, Any]) -> MessageFormatConfig:
    if not value:
        return MessageFormatConfig()
    allowed = set(MessageFormatConfig.__dataclass_fields__)
    kwargs = {key: raw for key, raw in value.items() if key in allowed}
    if "use_thumbnail" in value and "image_mode" not in kwargs:
        kwargs["image_mode"] = (
            ImageMode.THUMBNAIL
            if _bool(value["use_thumbnail"], True)
            else ImageMode.DESCRIPTION
        )
    if "include_sender" in value and "inject_sender" not in kwargs:
        kwargs["inject_sender"] = _bool(value["include_sender"], True)
    if "include_message_id" in value and "inject_record_id" not in kwargs:
        kwargs["inject_record_id"] = _bool(value["include_message_id"], False)
    if "include_time" in value and "timestamp_mode" not in kwargs:
        kwargs["timestamp_mode"] = "sparse" if _bool(value["include_time"], True) else "none"
    if "image_mode" in kwargs:
        kwargs["image_mode"] = _enum_value(ImageMode, kwargs["image_mode"], ImageMode.DESCRIPTION)
    if "emoji_mode" in kwargs:
        kwargs["emoji_mode"] = _enum_value(EmojiMode, kwargs["emoji_mode"], EmojiMode.SEMANTIC)
    if "pack_mode" in kwargs:
        kwargs["pack_mode"] = _enum_value(PackMode, kwargs["pack_mode"], PackMode.PACK)
    try:
        return MessageFormatConfig(**kwargs)
    except (TypeError, ValueError):
        return MessageFormatConfig()


def _review_workflow_config(
    review: dict[str, Any],
    active_chat: dict[str, Any],
    summaries: dict[str, Any],
) -> ReviewWorkflowConfig:
    return ReviewWorkflowConfig(
        review_scan_batch_size=_int(review.get("scan_batch_size"), 500),
        overflow_threshold_messages=_int(review.get("overflow_threshold_messages"), 3000),
        overflow_compression_batch_size=_int(
            review.get("overflow_compression_batch_size"),
            500,
        ),
        reply_context_before_messages=_int(review.get("reply_context_before_messages"), 30),
        reply_context_after_messages=_int(review.get("reply_context_after_messages"), 10),
        tail_history_before_seconds=_float(review.get("tail_history_before_seconds"), 180.0),
        tail_history_limit=_int(review.get("tail_history_limit"), 500),
        active_chat_summary_max_age_seconds=_float(
            summaries.get(
                "active_chat_summary_max_age_seconds",
                review.get("active_chat_summary_max_age_seconds"),
            ),
            1800.0,
        ),
        review_block_digest_concurrency=_int(review.get("block_digest_concurrency"), 4),
        provisional_active_chat_interest=_float(active_chat.get("initial_interest"), 15.0),
        provisional_active_chat_half_life_seconds=_float(
            active_chat.get("half_life_seconds"),
            20.0,
        ),
        active_chat_bootstrap_timeout_seconds=_float(
            review.get("bootstrap_timeout_seconds"),
            20.0,
        ),
        idle_review_planning_min_after_seconds=_float(
            review.get("idle_review_planning_min_after_seconds"),
            30.0,
        ),
        idle_review_planning_max_after_seconds=_float(
            review.get("idle_review_planning_max_after_seconds"),
            3600.0,
        ),
    )


def _summary_markdown_config(
    value: dict[str, Any],
    *,
    data_dir: Path,
) -> SummaryMarkdownConfig:
    raw_dir = str(value.get("dir") or value.get("directory") or "summary").strip()
    directory = Path(raw_dir or "summary")
    if not directory.is_absolute():
        directory = data_dir / directory
    return SummaryMarkdownConfig(
        enabled=_bool(value.get("enabled"), True),
        directory=directory,
    )


def _agent_scheduler_config(review: dict[str, Any]) -> AgentSchedulerConfig:
    return AgentSchedulerConfig(
        mention_wake_count=_int(review.get("mention_wake_count"), 1),
        mention_wake_window_seconds=_float(review.get("mention_wake_window_seconds"), 60.0),
    )


def _review_policy_config(
    review: dict[str, Any],
    scheduler_config: AgentSchedulerConfig,
) -> ReviewPolicyConfig:
    return ReviewPolicyConfig(
        default_review_after_seconds=_float(review.get("default_review_after_seconds"), 900.0),
        default_reason=str(
            review.get("default_review_reason") or "default_idle_review_interval"
        ),
        mention_wake_count=scheduler_config.mention_wake_count,
        mention_wake_window_seconds=scheduler_config.mention_wake_window_seconds,
    )


def _active_chat_policy_config(active_chat: dict[str, Any]) -> ActiveChatPolicyConfig:
    interest_delta = _mapping(active_chat.get("interest_delta"))
    return ActiveChatPolicyConfig(
        initial_interest_value=_float(active_chat.get("initial_interest"), 15.0),
        decay_half_life_seconds=_float(active_chat.get("half_life_seconds"), 20.0),
        tick_interval_seconds=_float(active_chat.get("tick_interval_seconds"), 5.0),
        idle_interest_threshold=_float(active_chat.get("idle_interest_threshold"), 5.0),
        message_interest_delta=_float(interest_delta.get("normal_message"), 1.0),
        mention_interest_delta=_float(interest_delta.get("mention_self"), 8.0),
        mention_other_interest_delta=_float(interest_delta.get("mention_other"), 0.0),
        reply_interest_delta=_float(interest_delta.get("reply_to_self"), 5.0),
        poke_interest_delta=_float(interest_delta.get("poke"), 0.0),
        max_interest_value=_float(active_chat.get("max_interest"), 100.0),
    )


def _active_chat_attention_config(active_chat: dict[str, Any]) -> ActiveChatAttentionConfig:
    attention = _mapping(active_chat.get("attention"))
    return ActiveChatAttentionConfig(
        base_contribution=_float(attention.get("base_contribution"), 1.0),
        mention_contribution=_float(attention.get("mention_self_contribution"), 4.0),
        mention_other_contribution=_float(attention.get("mention_other_contribution"), 0.5),
        reply_to_bot_contribution=_float(attention.get("reply_to_self_contribution"), 3.0),
        poke_self_contribution=_float(attention.get("poke_self_contribution"), 0.8),
        poke_other_contribution=_float(attention.get("poke_other_contribution"), 0.2),
        bot_self_contribution=_float(attention.get("bot_self_contribution"), 0.0),
        contribution_decay_k=_float(attention.get("contribution_decay_k"), 0.003),
        base_threshold=_float(attention.get("threshold"), 5.0),
        reference_interest=_float(attention.get("reference_interest"), 30.0),
        threshold_min=_float(attention.get("threshold_min"), 2.0),
        threshold_max=_float(attention.get("threshold_max"), 15.0),
        semantic_wait_ms=_float(attention.get("semantic_wait_ms"), 800.0),
        post_round_accumulated_multiplier=_float(
            attention.get(
                "post_round_accumulated_multiplier",
                attention.get(
                    "post_round_multiplier",
                    active_chat.get("post_round_attention_multiplier"),
                ),
            ),
            0.25,
        ),
    )


def _active_chat_interest_effect_config(
    active_chat: dict[str, Any],
) -> ActiveChatInterestEffectConfig:
    interest_delta = _mapping(active_chat.get("interest_delta"))
    return ActiveChatInterestEffectConfig(
        send_reply_delta=_float(interest_delta.get("send_reply"), 10.0),
        send_reply_low_delta=_float(interest_delta.get("send_reply_low"), 5.0),
        no_reply_delta=_float(interest_delta.get("no_reply"), -5.0),
        no_reply_strong_delta=_float(interest_delta.get("no_reply_strong"), -10.0),
        send_poke_delta=_float(interest_delta.get("send_poke"), 3.0),
        request_think_mode_delta=_float(interest_delta.get("request_think_mode"), 6.0),
        retry_failed_delta=_float(interest_delta.get("retry_failed"), -3.0),
    )


def _review_runtime_config(
    review: dict[str, Any],
    message_format: MessageFormatConfig,
    defaults_llm: dict[str, Any],
) -> ReviewRuntimeConfig:
    return ReviewRuntimeConfig(
        overflow_compression=_review_stage_config(
            _mapping(review.get("overflow_compression")),
            message_format,
            defaults_llm,
            special_prompt_keys=frozenset(),
        ),
        review_scan=_review_stage_config(
            _mapping(review.get("scan")),
            message_format,
            defaults_llm,
            special_prompt_keys=frozenset(),
        ),
        review_block_digest=_review_stage_config(
            _mapping(review.get("block_digest")),
            message_format,
            defaults_llm,
            special_prompt_keys=frozenset(),
        ),
        reply_decision=_review_stage_config(
            _mapping(review.get("reply_decision")),
            message_format,
            defaults_llm,
            special_prompt_keys=_REVIEW_SPECIAL_PROMPT_KEYS,
        ),
        active_chat_bootstrap=_review_stage_config(
            _mapping(review.get("active_chat_bootstrap")),
            message_format,
            defaults_llm,
            special_prompt_keys=frozenset(),
        ),
        idle_review_planning=_review_stage_config(
            _mapping(review.get("idle_review_planning")),
            message_format,
            defaults_llm,
            special_prompt_keys=frozenset(),
        ),
    )


def _review_stage_config(
    value: dict[str, Any],
    message_format: MessageFormatConfig,
    defaults_llm: dict[str, Any],
    *,
    special_prompt_keys: frozenset[str],
) -> ReviewStageRuntimeConfig:
    return ReviewStageRuntimeConfig(
        enabled=_bool(value.get("enabled"), True),
        llm=_optional_str(value.get("llm")) or "",
        default_llm=_optional_str(defaults_llm.get("llm")) or "",
        caller=str(_first(value, defaults_llm, key="caller") or "agent.review"),
        profile_id=str(_first(value, defaults_llm, key="profile_id") or ""),
        component_ids_by_stage=_prompt_components(_mapping(value.get("prompts"))),
        special_prompt_ids=_special_prompt_ids(
            _mapping(value.get("prompts")),
            allowed_keys=special_prompt_keys,
        ),
        message_format_config=message_format,
        params={
            **dict(_mapping(defaults_llm.get("params"))),
            **dict(_mapping(value.get("params"))),
        },
        tool_config=stage_tool_config_from_mapping(_mapping(value.get("tools"))),
        max_model_retries=_int(
            _first(value, defaults_llm, key="max_model_retries"),
            1,
        ),
        retry_backoff_seconds=_float(
            _first(value, defaults_llm, key="retry_backoff_seconds"),
            0.25,
        ),
    )


def _active_chat_fast_runner_config(
    value: dict[str, Any],
    message_format: MessageFormatConfig,
    defaults_llm: dict[str, Any],
) -> ActiveChatFastRunnerConfig:
    return ActiveChatFastRunnerConfig(
        caller=str(_first(value, defaults_llm, key="caller") or "agent.active_chat"),
        llm=_optional_str(value.get("llm")) or "",
        default_llm=_optional_str(defaults_llm.get("llm")) or "",
        profile_id=str(_first(value, defaults_llm, key="profile_id") or ""),
        component_ids_by_stage=_prompt_components(_mapping(value.get("prompts"))),
        special_prompt_ids=_special_prompt_ids(
            _mapping(value.get("prompts")),
            allowed_keys=_ACTIVE_CHAT_SPECIAL_PROMPT_KEYS,
        ),
        params={
            **dict(_mapping(defaults_llm.get("params"))),
            **dict(_mapping(value.get("params"))),
        },
        tool_config=stage_tool_config_from_mapping(_mapping(value.get("tools"))),
        message_format_config=message_format,
    )


def _llm_defaults_config(defaults: dict[str, Any]) -> dict[str, Any]:
    return {
        "llm": defaults.get("llm"),
        "caller": defaults.get("caller"),
        "profile_id": defaults.get("profile_id"),
        "max_model_retries": defaults.get("max_model_retries"),
        "retry_backoff_seconds": defaults.get("retry_backoff_seconds"),
        "params": defaults.get("params"),
    }


def _prompt_components(prompts: dict[str, Any]) -> dict[PromptStage, list[str]]:
    result: dict[PromptStage, list[str]] = {}
    for key, stage in _PROMPT_STAGE_KEYS.items():
        value = prompts.get(key)
        ids = [item for item in _string_tuple(value, default=()) if item]
        if ids:
            result.setdefault(stage, [])
            result[stage].extend(item for item in ids if item not in result[stage])
    return result


def _special_prompt_ids(
    prompts: dict[str, Any],
    *,
    allowed_keys: frozenset[str],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for key in allowed_keys:
        value = prompts.get(key)
        if isinstance(value, str) and value.strip():
            result[key] = value.strip()
    return result


def _iter_agent_llm_refs(payload: dict[str, Any]) -> list[tuple[str, str]]:
    agent = _mapping(payload.get("agent"))
    defaults = _mapping(agent.get("defaults"))
    result: list[tuple[str, str]] = []
    _append_optional_ref(result, "agent.defaults.llm", defaults.get("llm"))
    review = _mapping(agent.get("review"))
    for stage_name in _REVIEW_STAGE_NAMES:
        stage = _mapping(review.get(stage_name))
        _append_optional_ref(result, f"agent.review.{stage_name}.llm", stage.get("llm"))
    active_chat = _mapping(agent.get("active_chat"))
    fast_mode = _mapping(active_chat.get("fast_mode"))
    _append_optional_ref(result, "agent.active_chat.fast_mode.llm", fast_mode.get("llm"))
    return result


def _validate_agent_persona_ref(
    payload: dict[str, Any],
    *,
    persona_repository: Any,
    path_prefix: str,
) -> list[ConfigValidationIssue]:
    persona_id = _agent_persona_id(payload)
    if not persona_id:
        return []
    path = _join_issue_path(path_prefix, "agent.persona_id")
    persona = persona_repository.get(persona_id)
    if persona is None:
        return [
            ConfigValidationIssue(
                path=path,
                message=f"Persona {persona_id!r} is not registered",
                code="unknown_persona",
            )
        ]
    if not persona.get("enabled", True):
        return [
            ConfigValidationIssue(
                path=path,
                message=f"Persona {persona_id!r} is disabled",
                code="disabled_ref",
            )
        ]
    prompt_text = str(persona.get("prompt_text") or "").strip()
    if not prompt_text:
        return [
            ConfigValidationIssue(
                path=path,
                message=f"Persona {persona_id!r} prompt body is empty",
                code="empty_persona_prompt",
            )
        ]
    return []


def _agent_persona_id(payload: dict[str, Any]) -> str:
    return str(_mapping(payload.get("agent")).get("persona_id") or "").strip()


def _iter_agent_prompt_component_refs(
    payload: dict[str, Any],
) -> list[tuple[str, str, PromptStage]]:
    agent = _mapping(payload.get("agent"))
    result: list[tuple[str, str, PromptStage]] = []
    review = _mapping(agent.get("review"))
    for stage_name in _REVIEW_STAGE_NAMES:
        prompts = _mapping(_mapping(review.get(stage_name)).get("prompts"))
        special_prompt_stages = (
            _REVIEW_SPECIAL_PROMPT_STAGES
            if stage_name == "reply_decision"
            else {}
        )
        result.extend(
            _iter_prompt_component_refs(
                prompts,
                prefix=f"agent.review.{stage_name}.prompts",
                special_prompt_stages=special_prompt_stages,
            )
        )
    active_chat = _mapping(agent.get("active_chat"))
    fast_mode_prompts = _mapping(_mapping(active_chat.get("fast_mode")).get("prompts"))
    result.extend(
        _iter_prompt_component_refs(
            fast_mode_prompts,
            prefix="agent.active_chat.fast_mode.prompts",
            special_prompt_stages=_ACTIVE_CHAT_SPECIAL_PROMPT_STAGES,
        )
    )
    return result


def _validate_agent_prompt_slots(
    payload: dict[str, Any],
    *,
    path_prefix: str,
) -> list[ConfigValidationIssue]:
    agent = _mapping(payload.get("agent"))
    issues: list[ConfigValidationIssue] = []
    review = _mapping(agent.get("review"))
    for stage_name in _REVIEW_STAGE_NAMES:
        prompts = _mapping(_mapping(review.get(stage_name)).get("prompts"))
        allowed_special_keys = (
            _REVIEW_SPECIAL_PROMPT_KEYS
            if stage_name == "reply_decision"
            else frozenset()
        )
        issues.extend(
            _validate_prompt_slot_keys(
                prompts,
                prefix=f"agent.review.{stage_name}.prompts",
                allowed_special_keys=allowed_special_keys,
                path_prefix=path_prefix,
            )
        )
    active_chat = _mapping(agent.get("active_chat"))
    fast_mode_prompts = _mapping(_mapping(active_chat.get("fast_mode")).get("prompts"))
    issues.extend(
        _validate_prompt_slot_keys(
            fast_mode_prompts,
            prefix="agent.active_chat.fast_mode.prompts",
            allowed_special_keys=_ACTIVE_CHAT_SPECIAL_PROMPT_KEYS,
            path_prefix=path_prefix,
        )
    )
    return issues


def _validate_prompt_slot_keys(
    prompts: dict[str, Any],
    *,
    prefix: str,
    allowed_special_keys: frozenset[str],
    path_prefix: str,
) -> list[ConfigValidationIssue]:
    allowed_keys = set(_PROMPT_STAGE_KEYS) | set(allowed_special_keys)
    issues: list[ConfigValidationIssue] = []
    for key, value in prompts.items():
        if key in allowed_keys:
            issues.extend(
                _validate_prompt_slot_value(
                    value,
                    path=_join_issue_path(path_prefix, f"{prefix}.{key}"),
                    is_special=key in allowed_special_keys,
                )
            )
            continue
        issues.append(
            ConfigValidationIssue(
                path=_join_issue_path(path_prefix, f"{prefix}.{key}"),
                message=f"Unknown prompt slot {key!r}",
                code="unknown_prompt_slot",
            )
        )
    return issues


def _validate_prompt_slot_value(
    value: Any,
    *,
    path: str,
    is_special: bool,
) -> list[ConfigValidationIssue]:
    if isinstance(value, str):
        return []
    if is_special:
        return [
            ConfigValidationIssue(
                path=path,
                message="expected string",
                code="type",
            )
        ]
    if isinstance(value, list):
        return [
            ConfigValidationIssue(
                path=f"{path}.{index}",
                message="expected string",
                code="type",
            )
            for index, item in enumerate(value)
            if not isinstance(item, str)
        ]
    return [
        ConfigValidationIssue(
            path=path,
            message="expected string or string_list",
            code="type",
        )
    ]


def _iter_prompt_component_refs(
    prompts: dict[str, Any],
    *,
    prefix: str,
    special_prompt_stages: dict[str, PromptStage],
) -> list[tuple[str, str, PromptStage]]:
    refs: list[tuple[str, str, PromptStage]] = []
    for key, value in prompts.items():
        expected_stage = _PROMPT_STAGE_KEYS.get(key) or special_prompt_stages.get(key)
        if expected_stage is None:
            continue
        path = f"{prefix}.{key}"
        if isinstance(value, str):
            text = value.strip()
            if text:
                refs.append((path, text, expected_stage))
            continue
        if key in _PROMPT_STAGE_KEYS and isinstance(value, list):
            for index, item in enumerate(value):
                if isinstance(item, str) and item.strip():
                    text = item.strip()
                    refs.append((f"{path}.{index}", text, expected_stage))
    return refs


def _append_optional_ref(
    result: list[tuple[str, str]],
    path: str,
    value: Any,
) -> None:
    text = _optional_str(value)
    if text:
        result.append((path, text))


def _validate_agent_llm_ref(
    llm_ref: str,
    *,
    model_registry: Any,
    path: str,
) -> list[ConfigValidationIssue]:
    tagged = parse_tagged_llm_ref(llm_ref)
    if tagged is not None:
        if tagged.route_id is not None:
            return _validate_agent_route_ref(
                tagged.route_id,
                model_registry=model_registry,
                path=path,
            )
        if tagged.model_id is not None:
            return _validate_agent_model_ref(
                tagged.model_id,
                model_registry=model_registry,
                path=path,
            )
        return [
            ConfigValidationIssue(
                path=path,
                message="LLM reference must include a route or model id",
                code="empty_llm_ref",
            )
        ]

    route = model_registry.get_route(llm_ref)
    if route is not None:
        return _disabled_ref_issue(
            path=path,
            message=f"Route {llm_ref!r} is disabled",
        ) if not route.get("enabled", True) else []
    model = model_registry.get_model(llm_ref)
    if model is not None:
        return _disabled_ref_issue(
            path=path,
            message=f"Model {llm_ref!r} is disabled",
        ) if not model.get("enabled", True) else []
    return [
        ConfigValidationIssue(
            path=path,
            message=f"LLM reference {llm_ref!r} does not match a model route or model",
            code="unknown_llm_ref",
        )
    ]


def _validate_agent_route_ref(
    route_id: str,
    *,
    model_registry: Any,
    path: str,
) -> list[ConfigValidationIssue]:
    route = model_registry.get_route(route_id)
    if route is None:
        return [
            ConfigValidationIssue(
                path=path,
                message=f"Route {route_id!r} is not registered",
                code="unknown_route",
            )
        ]
    if not route.get("enabled", True):
        return _disabled_ref_issue(
            path=path,
            message=f"Route {route_id!r} is disabled",
        )
    return []


def _validate_agent_model_ref(
    model_id: str,
    *,
    model_registry: Any,
    path: str,
) -> list[ConfigValidationIssue]:
    model = model_registry.get_model(model_id)
    if model is None:
        return [
            ConfigValidationIssue(
                path=path,
                message=f"Model {model_id!r} is not registered",
                code="unknown_model",
            )
        ]
    if not model.get("enabled", True):
        return _disabled_ref_issue(
            path=path,
            message=f"Model {model_id!r} is disabled",
        )
    return []


def _disabled_ref_issue(*, path: str, message: str) -> list[ConfigValidationIssue]:
    return [ConfigValidationIssue(path=path, message=message, code="disabled_ref")]


def _join_issue_path(path_prefix: str, path: str) -> str:
    return f"{path_prefix}.{path}" if path_prefix else path


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _first(*sources: dict[str, Any], key: str) -> Any:
    for source in sources:
        if key in source and source[key] is not None:
            return source[key]
    return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_tuple(value: Any, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(value, str):
        text = value.strip()
        return (text,) if text else default
    if isinstance(value, (list, tuple)):
        result = tuple(str(item).strip() for item in value if str(item).strip())
        return result or default
    return default


def _bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on", "enabled"}:
            return True
        if normalized in {"0", "false", "no", "off", "disabled"}:
            return False
    return bool(value)


def _enum_value(enum_type: Any, value: Any, default: Any) -> Any:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value))
    except ValueError:
        return default


def _int(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def with_source_path(config: AgentRuntimeConfig, source_path: Path | str) -> AgentRuntimeConfig:
    """Return a copy with source path attached."""

    return replace(config, source_path=str(source_path))


def _format_agent_config_issues(
    path: Path,
    issues: list[ConfigValidationIssue],
) -> str:
    lines = [f"Agent config {path} is invalid:"]
    lines.extend(f"- {issue.path}: {issue.message} ({issue.code})" for issue in issues)
    return "\n".join(lines)


__all__ = [
    "AgentRuntimeConfig",
    "AgentRuntimeConfigError",
    "agent_runtime_config_from_mapping",
    "load_agent_runtime_config",
    "validate_agent_runtime_config_mapping",
    "validate_agent_runtime_config_references",
    "with_source_path",
]
