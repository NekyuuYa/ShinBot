"""Factory and config for review stage runners."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.runners._review_base import ReviewLLMRunnerConfig
from shinbot.agent.runners.review_block_digest import (
    LLMReviewBlockDigestStageRunner,
    NoopReviewBlockDigestStageRunner,
    ReviewBlockDigestStageRunner,
    register_review_block_digest_prompt_components,
)
from shinbot.agent.runners.review_bootstrap import (
    ActiveChatBootstrapStageRunner,
    LLMActiveChatBootstrapStageRunner,
    NoopActiveChatBootstrapStageRunner,
    register_review_bootstrap_prompt_components,
)
from shinbot.agent.runners.review_compression import (
    LLMOverflowCompressionStageRunner,
    NoopOverflowCompressionStageRunner,
    OverflowCompressionStageRunner,
    register_review_compression_prompt_components,
)
from shinbot.agent.runners.review_reply import (
    LLMReplyDecisionStageRunner,
    NoopReplyDecisionStageRunner,
    ReplyDecisionStageRunner,
    register_review_reply_prompt_components,
)
from shinbot.agent.runners.review_scan import (
    LLMReviewScanStageRunner,
    NoopReviewScanStageRunner,
    ReviewScanStageRunner,
    register_review_scan_prompt_components,
)
from shinbot.agent.runtime.instance_config import (
    InstanceRuntimeConfigResolver,
    RuntimeModelTarget,
)
from shinbot.agent.runtime.tool_config import StageToolConfig, stage_tool_config_from_mapping
from shinbot.agent.services.message_formatter import MessageFormatConfig
from shinbot.agent.services.prompt_engine import PromptFileLoadConfig, PromptStage


@dataclass(slots=True, frozen=True)
class ReviewStageRuntimeConfig:
    """Runtime model settings for one review stage."""

    enabled: bool = True
    llm: str = ""
    default_llm: str = ""
    route_id: str | None = None
    model_id: str | None = None
    caller: str = "agent.review"
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    message_format_config: MessageFormatConfig | None = None
    params: dict[str, Any] = field(default_factory=dict)
    tool_config: StageToolConfig = field(default_factory=StageToolConfig)
    max_model_retries: int = 1
    retry_backoff_seconds: float = 0.25

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> ReviewStageRuntimeConfig:
        if not value:
            return cls()
        return cls(
            enabled=bool(value.get("enabled", True)),
            llm=str(value.get("llm") or ""),
            default_llm=str(value.get("default_llm") or ""),
            route_id=_optional_str(value.get("route_id")),
            model_id=_optional_str(value.get("model_id")),
            caller=str(value.get("caller") or "agent.review"),
            profile_id=str(value.get("profile_id") or ""),
            component_ids_by_stage=_component_ids_by_stage(
                value.get("component_ids_by_stage")
            ),
            message_format_config=_message_format_config(
                value.get("message_format_config")
            ),
            params=dict(_mapping_or_empty(value.get("params"))),
            tool_config=stage_tool_config_from_mapping(_mapping_or_none(value.get("tools"))),
            max_model_retries=_int_or_default(value.get("max_model_retries"), 1),
            retry_backoff_seconds=_float_or_default(
                value.get("retry_backoff_seconds"),
                0.25,
            ),
        )

    def to_llm_config(
        self,
        *,
        instance_config_resolver: InstanceRuntimeConfigResolver | None = None,
        model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None,
    ) -> ReviewLLMRunnerConfig:
        kwargs: dict[str, Any] = {
            "caller": self.caller,
            "llm": self.llm,
            "default_llm": self.default_llm,
            "route_id": self.route_id,
            "model_id": self.model_id,
            "profile_id": self.profile_id,
            "component_ids_by_stage": dict(self.component_ids_by_stage),
            "message_format_config": self.message_format_config,
            "params": dict(self.params),
            "tool_config": self.tool_config,
            "max_model_retries": self.max_model_retries,
            "retry_backoff_seconds": self.retry_backoff_seconds,
            "instance_config_resolver": instance_config_resolver,
            "model_target_resolver": model_target_resolver,
        }
        return ReviewLLMRunnerConfig(**kwargs)

@dataclass(slots=True, frozen=True)
class ReviewRuntimeConfig:
    """Runtime runner toggles for all review workflow stages."""

    overflow_compression: ReviewStageRuntimeConfig = field(
        default_factory=ReviewStageRuntimeConfig
    )
    review_scan: ReviewStageRuntimeConfig = field(default_factory=ReviewStageRuntimeConfig)
    reply_decision: ReviewStageRuntimeConfig = field(default_factory=ReviewStageRuntimeConfig)
    active_chat_bootstrap: ReviewStageRuntimeConfig = field(
        default_factory=ReviewStageRuntimeConfig
    )
    review_block_digest: ReviewStageRuntimeConfig = field(
        default_factory=ReviewStageRuntimeConfig
    )

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> ReviewRuntimeConfig:
        if not value:
            return cls()
        return cls(
            overflow_compression=ReviewStageRuntimeConfig.from_mapping(
                _mapping_or_none(value.get("overflow_compression"))
            ),
            review_scan=ReviewStageRuntimeConfig.from_mapping(
                _mapping_or_none(value.get("review_scan"))
            ),
            reply_decision=ReviewStageRuntimeConfig.from_mapping(
                _mapping_or_none(value.get("reply_decision"))
            ),
            active_chat_bootstrap=ReviewStageRuntimeConfig.from_mapping(
                _mapping_or_none(value.get("active_chat_bootstrap"))
            ),
            review_block_digest=ReviewStageRuntimeConfig.from_mapping(
                _mapping_or_none(value.get("review_block_digest"))
            ),
        )


class ReviewRunnerFactory:
    """Creates review stage runners from runtime config."""

    def __init__(
        self,
        model_runtime: Any | None,
        *,
        config: ReviewRuntimeConfig | None = None,
        prompt_registry: Any | None = None,
        tool_manager: Any | None = None,
        summary_service: Any | None = None,
        message_formatter: Any | None = None,
        instance_config_resolver: InstanceRuntimeConfigResolver | None = None,
        model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None,
    ) -> None:
        self._model_runtime = model_runtime
        self._config = config or ReviewRuntimeConfig()
        self._prompt_registry = prompt_registry
        self._tool_manager = tool_manager
        self._summary_service = summary_service
        self._message_formatter = message_formatter
        self._instance_config_resolver = instance_config_resolver
        self._model_target_resolver = model_target_resolver

    def create_overflow_compression_runner(self) -> OverflowCompressionStageRunner:
        stage_config = self._config.overflow_compression
        if self._enabled(stage_config):
            return LLMOverflowCompressionStageRunner(
                self._model_runtime,
                config=self._llm_config(stage_config),
                prompt_registry=self._prompt_registry,
                summary_service=self._summary_service,
                message_formatter=self._message_formatter,
            )
        return NoopOverflowCompressionStageRunner()

    def create_review_scan_runner(self) -> ReviewScanStageRunner:
        stage_config = self._config.review_scan
        if self._enabled(stage_config):
            return LLMReviewScanStageRunner(
                self._model_runtime,
                config=self._llm_config(stage_config),
                prompt_registry=self._prompt_registry,
                message_formatter=self._message_formatter,
            )
        return NoopReviewScanStageRunner()

    def create_reply_decision_runner(self) -> ReplyDecisionStageRunner:
        stage_config = self._config.reply_decision
        if self._enabled(stage_config):
            return LLMReplyDecisionStageRunner(
                self._model_runtime,
                config=self._llm_config(stage_config),
                prompt_registry=self._prompt_registry,
                tool_manager=self._tool_manager,
                message_formatter=self._message_formatter,
            )
        return NoopReplyDecisionStageRunner()

    def create_active_chat_bootstrap_runner(self) -> ActiveChatBootstrapStageRunner:
        stage_config = self._config.active_chat_bootstrap
        if self._enabled(stage_config):
            return LLMActiveChatBootstrapStageRunner(
                self._model_runtime,
                config=self._llm_config(stage_config),
                prompt_registry=self._prompt_registry,
                message_formatter=self._message_formatter,
            )
        return NoopActiveChatBootstrapStageRunner()

    def create_review_block_digest_runner(self) -> ReviewBlockDigestStageRunner:
        stage_config = self._config.review_block_digest
        if self._enabled(stage_config):
            return LLMReviewBlockDigestStageRunner(
                self._model_runtime,
                config=self._llm_config(stage_config),
                prompt_registry=self._prompt_registry,
                summary_service=self._summary_service,
                message_formatter=self._message_formatter,
            )
        return NoopReviewBlockDigestStageRunner()

    def create_workflow_runner_kwargs(self) -> dict[str, Any]:
        """Return ReviewCoordinator constructor kwargs for all stage runners."""
        kwargs: dict[str, Any] = {
            "compression_runner": self.create_overflow_compression_runner(),
            "scan_runner": self.create_review_scan_runner(),
            "block_digest_runner": self.create_review_block_digest_runner(),
            "reply_runner": self.create_reply_decision_runner(),
            "bootstrap_runner": self.create_active_chat_bootstrap_runner(),
        }
        if self._summary_service is not None:
            kwargs["summary_service"] = self._summary_service
        return kwargs

    def _enabled(self, stage_config: ReviewStageRuntimeConfig) -> bool:
        return bool(stage_config.enabled and self._model_runtime is not None)

    def _llm_config(self, stage_config: ReviewStageRuntimeConfig) -> ReviewLLMRunnerConfig:
        return stage_config.to_llm_config(
            instance_config_resolver=self._instance_config_resolver,
            model_target_resolver=self._model_target_resolver,
        )


def register_review_prompt_components(
    registry,
    *,
    prompt_file_config: PromptFileLoadConfig | None = None,
) -> None:
    """Register all review stage prompt components."""
    register_review_compression_prompt_components(
        registry,
        prompt_file_config=prompt_file_config,
    )
    register_review_scan_prompt_components(
        registry,
        prompt_file_config=prompt_file_config,
    )
    register_review_block_digest_prompt_components(
        registry,
        prompt_file_config=prompt_file_config,
    )
    register_review_reply_prompt_components(
        registry,
        prompt_file_config=prompt_file_config,
    )
    register_review_bootstrap_prompt_components(
        registry,
        prompt_file_config=prompt_file_config,
    )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _message_format_config(value: Any) -> MessageFormatConfig | None:
    if isinstance(value, MessageFormatConfig):
        return value
    if not isinstance(value, dict):
        return None
    allowed = set(MessageFormatConfig.__dataclass_fields__)
    kwargs = {key: raw for key, raw in value.items() if key in allowed}
    if not kwargs:
        return None
    try:
        return MessageFormatConfig(**kwargs)
    except (TypeError, ValueError):
        return None


def _component_ids_by_stage(value: Any) -> dict[PromptStage, list[str]]:
    if not isinstance(value, dict):
        return {}
    result: dict[PromptStage, list[str]] = {}
    for raw_stage, raw_ids in value.items():
        try:
            stage = raw_stage if isinstance(raw_stage, PromptStage) else PromptStage(str(raw_stage))
        except ValueError:
            continue
        if isinstance(raw_ids, str):
            ids = [raw_ids]
        elif isinstance(raw_ids, list):
            ids = [str(item) for item in raw_ids if str(item).strip()]
        else:
            ids = []
        if ids:
            result[stage] = ids
    return result


__all__ = [
    "ReviewRunnerFactory",
    "ReviewRuntimeConfig",
    "ReviewStageRuntimeConfig",
    "register_review_prompt_components",
]
