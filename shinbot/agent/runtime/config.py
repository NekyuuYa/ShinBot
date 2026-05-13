"""Agent runtime config file loading and normalization."""

from __future__ import annotations

import tomllib
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
    review_policy_config = _review_policy_config(scheduler_config)
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
        active_chat_policy_config=active_chat_policy_config,
        active_chat_attention_config=active_chat_attention_config,
        active_chat_interest_effect_config=interest_effect_config,
        active_chat_fast_runner_config=active_chat_fast_runner_config,
        active_chat_conversation_message_limit=_int(
            active_chat.get("conversation_message_limit"),
            80,
        ),
        summary_markdown_config=summary_markdown_config,
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


def _review_policy_config(scheduler_config: AgentSchedulerConfig) -> ReviewPolicyConfig:
    return ReviewPolicyConfig(
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
        ),
        review_scan=_review_stage_config(
            _mapping(review.get("scan")),
            message_format,
            defaults_llm,
        ),
        review_block_digest=_review_stage_config(
            _mapping(review.get("block_digest")),
            message_format,
            defaults_llm,
        ),
        reply_decision=_review_stage_config(
            _mapping(review.get("reply_decision")),
            message_format,
            defaults_llm,
        ),
        active_chat_bootstrap=_review_stage_config(
            _mapping(review.get("active_chat_bootstrap")),
            message_format,
            defaults_llm,
        ),
    )


def _review_stage_config(
    value: dict[str, Any],
    message_format: MessageFormatConfig,
    defaults_llm: dict[str, Any],
) -> ReviewStageRuntimeConfig:
    return ReviewStageRuntimeConfig(
        enabled=_bool(value.get("enabled"), True),
        llm=_optional_str(value.get("llm")) or "",
        default_llm=_optional_str(defaults_llm.get("llm")) or "",
        caller=str(_first(value, defaults_llm, key="caller") or "agent.review"),
        profile_id=str(_first(value, defaults_llm, key="profile_id") or ""),
        component_ids_by_stage=_prompt_components(_mapping(value.get("prompts"))),
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
    stage_keys = {
        "system": PromptStage.SYSTEM_BASE,
        "task": PromptStage.INSTRUCTIONS,
        "instructions": PromptStage.INSTRUCTIONS,
        "constraints": PromptStage.CONSTRAINTS,
        "context": PromptStage.CONTEXT,
        "abilities": PromptStage.ABILITIES,
        "compatibility": PromptStage.COMPATIBILITY,
    }
    result: dict[PromptStage, list[str]] = {}
    for key, stage in stage_keys.items():
        value = prompts.get(key)
        ids = [item for item in _string_tuple(value, default=()) if item]
        if ids:
            result.setdefault(stage, [])
            result[stage].extend(item for item in ids if item not in result[stage])
    return result


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
    "with_source_path",
]
