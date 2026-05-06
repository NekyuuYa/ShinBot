"""Factory helpers for review workflow stage runners."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from shinbot.agent.prompt_manager import PromptStage
from shinbot.agent.review.stages.bootstrap import (
    ActiveChatBootstrapStageRunner,
    NoopActiveChatBootstrapStageRunner,
)
from shinbot.agent.review.stages.compression import (
    NoopOverflowCompressionStageRunner,
    OverflowCompressionStageRunner,
)
from shinbot.agent.review.stages.llm import (
    LLMActiveChatBootstrapStageRunner,
    LLMOverflowCompressionStageRunner,
    LLMReplyDecisionStageRunner,
    LLMReviewScanStageRunner,
    ReviewLLMRunnerConfig,
)
from shinbot.agent.review.stages.reply import (
    NoopReplyDecisionStageRunner,
    ReplyDecisionStageRunner,
)
from shinbot.agent.review.stages.scan import NoopReviewScanStageRunner, ReviewScanStageRunner


@dataclass(slots=True, frozen=True)
class ReviewStageRuntimeConfig:
    """Runtime model settings for one review stage."""

    enabled: bool = False
    route_id: str | None = None
    model_id: str | None = None
    caller: str = "agent.review"
    profile_id: str = ""
    component_ids_by_stage: dict[PromptStage, list[str]] = field(default_factory=dict)
    system_prompt: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> ReviewStageRuntimeConfig:
        """Build stage config from a plain runtime-config mapping."""
        if not value:
            return cls()
        return cls(
            enabled=bool(value.get("enabled", False)),
            route_id=_optional_str(value.get("route_id")),
            model_id=_optional_str(value.get("model_id")),
            caller=str(value.get("caller") or "agent.review"),
            profile_id=str(value.get("profile_id") or ""),
            component_ids_by_stage=_component_ids_by_stage(
                value.get("component_ids_by_stage")
            ),
            system_prompt=_optional_str(value.get("system_prompt")),
            params=dict(_mapping_or_empty(value.get("params"))),
        )

    def to_llm_config(self) -> ReviewLLMRunnerConfig:
        """Convert to the lower-level LLM runner config."""
        kwargs: dict[str, Any] = {
            "caller": self.caller,
            "route_id": self.route_id,
            "model_id": self.model_id,
            "profile_id": self.profile_id,
            "component_ids_by_stage": dict(self.component_ids_by_stage),
            "params": dict(self.params),
        }
        if self.system_prompt is not None:
            kwargs["system_prompt"] = self.system_prompt
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

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> ReviewRuntimeConfig:
        """Build review runtime config from a plain mapping."""
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
        )


class ReviewRunnerFactory:
    """Creates review stage runners from runtime config."""

    def __init__(
        self,
        model_runtime: Any | None,
        *,
        config: ReviewRuntimeConfig | None = None,
        prompt_registry: Any | None = None,
    ) -> None:
        self._model_runtime = model_runtime
        self._config = config or ReviewRuntimeConfig()
        self._prompt_registry = prompt_registry

    def create_overflow_compression_runner(self) -> OverflowCompressionStageRunner:
        stage_config = self._config.overflow_compression
        if self._enabled(stage_config):
            return LLMOverflowCompressionStageRunner(
                self._model_runtime,
                config=stage_config.to_llm_config(),
                prompt_registry=self._prompt_registry,
            )
        return NoopOverflowCompressionStageRunner()

    def create_review_scan_runner(self) -> ReviewScanStageRunner:
        stage_config = self._config.review_scan
        if self._enabled(stage_config):
            return LLMReviewScanStageRunner(
                self._model_runtime,
                config=stage_config.to_llm_config(),
                prompt_registry=self._prompt_registry,
            )
        return NoopReviewScanStageRunner()

    def create_reply_decision_runner(self) -> ReplyDecisionStageRunner:
        stage_config = self._config.reply_decision
        if self._enabled(stage_config):
            return LLMReplyDecisionStageRunner(
                self._model_runtime,
                config=stage_config.to_llm_config(),
                prompt_registry=self._prompt_registry,
            )
        return NoopReplyDecisionStageRunner()

    def create_active_chat_bootstrap_runner(
        self,
        *,
        fallback_initial_interest: float,
    ) -> ActiveChatBootstrapStageRunner:
        stage_config = self._config.active_chat_bootstrap
        if self._enabled(stage_config):
            return LLMActiveChatBootstrapStageRunner(
                self._model_runtime,
                config=stage_config.to_llm_config(),
                prompt_registry=self._prompt_registry,
            )
        return NoopActiveChatBootstrapStageRunner(
            initial_interest=fallback_initial_interest,
        )

    def create_workflow_runner_kwargs(
        self,
        *,
        fallback_active_chat_interest: float,
    ) -> dict[str, Any]:
        """Return ReviewWorkflow constructor kwargs for all stage runners."""
        return {
            "compression_runner": self.create_overflow_compression_runner(),
            "scan_runner": self.create_review_scan_runner(),
            "reply_runner": self.create_reply_decision_runner(),
            "bootstrap_runner": self.create_active_chat_bootstrap_runner(
                fallback_initial_interest=fallback_active_chat_interest,
            ),
        }

    def _enabled(self, stage_config: ReviewStageRuntimeConfig) -> bool:
        return bool(stage_config.enabled and self._model_runtime is not None)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping_or_none(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _mapping_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


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
]
