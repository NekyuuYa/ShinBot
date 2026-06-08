"""Review scan stage runner — selects candidate message ids."""

from __future__ import annotations

from typing import Any, Protocol

from shinbot.agent.runners.review_models import ReviewScanStageOutput
from shinbot.agent.runners.review_scan.prompt_registration import REVIEW_SCAN_COMPONENT_IDS
from shinbot.agent.runners.templates import RunnerTemplateConfig, StructuredOutputRunner
from shinbot.agent.services.context.builders.message_parts import parse_message_parts
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import MessageFormatterService
from shinbot.agent.services.prompt_engine import PromptRegistry
from shinbot.agent.utils.parsing import int_list, json_schema_response_format

_SCAN_RESPONSE_FORMAT = json_schema_response_format(
    "agent_review_scan",
    {
        "candidate_message_ids": {"type": "array", "items": {"type": "integer"}},
        "reason": {"type": "string"},
    },
    ["candidate_message_ids", "reason"],
)

class ReviewScanStageRunner(Protocol):
    """Select candidate message ids from one review_scan stage input."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        """Run one review_scan batch and return candidate ids."""


class NoopReviewScanStageRunner:
    """No-op scan runner that returns empty candidates."""

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        """Return a no-op scan output with no candidate messages.

        Args:
            stage_input: Review stage input (ignored by the no-op runner).

        Returns:
            An output with empty candidates and a noop reason.
        """
        return ReviewScanStageOutput(reason="noop_review_scan")


class LLMReviewScanStageRunner:
    """Select reply-worthy candidate message ids through the model runtime."""

    def __init__(
        self,
        model_runtime: Any,
        *,
        config: RunnerTemplateConfig | None = None,
        prompt_registry: PromptRegistry,
        message_formatter: MessageFormatterService | None = None,
    ) -> None:
        self._message_formatter = message_formatter
        routing = config or RunnerTemplateConfig()
        self._template = StructuredOutputRunner(
            model_runtime,
            prompt_registry=prompt_registry,
            config=RunnerTemplateConfig(
                caller=routing.caller,
                llm=routing.llm,
                default_llm=routing.default_llm,
                route_id=routing.route_id,
                model_id=routing.model_id,
                profile_id=routing.profile_id,
                response_format=_SCAN_RESPONSE_FORMAT,
                component_ids_by_stage=routing.component_ids_by_stage,
                builtin_component_ids=REVIEW_SCAN_COMPONENT_IDS,
                message_format_config=routing.message_format_config,
                params=routing.params,
                tool_config=routing.tool_config,
                max_model_retries=routing.max_model_retries,
                retry_backoff_seconds=routing.retry_backoff_seconds,
                instance_config_resolver=routing.instance_config_resolver,
                model_target_resolver=routing.model_target_resolver,
            ),
            message_formatter=message_formatter,
        )

    @property
    def _config(self) -> RunnerTemplateConfig:
        return self._template._config

    async def run(self, stage_input: ReviewStageInput) -> ReviewScanStageOutput:
        """Run the LLM-based review scan and return candidate message ids.

        Args:
            stage_input: Review stage input with conversation context.

        Returns:
            An output containing candidate message ids selected by the model,
            or an empty output on failure.
        """
        payload = await self._template.run(stage_input)
        if payload is None:
            return ReviewScanStageOutput(reason="llm_review_scan_failed")
        message_format_config = self._config.message_format_config
        self_platform_id = _resolve_self_platform_id(
            stage_input.metadata,
            message_format_config.self_platform_id if message_format_config is not None else "",
        )
        candidate_ids = _filter_candidate_message_ids(
            _candidate_message_ids_from_payload(payload),
            stage_input.source_messages,
            self_platform_id,
            message_formatter=self._message_formatter,
            message_format_config=message_format_config,
        )
        return ReviewScanStageOutput(
            candidate_message_ids=candidate_ids,
            reason=str(payload.get("reason") or "llm_review_scan"),
        )


def _candidate_message_ids_from_payload(payload: dict[str, Any]) -> list[int]:
    """Extract candidate ids from the scan payload, accepting observed aliases."""
    for key in (
        "candidate_message_ids",
        "candidate_msg_log_ids",
        "selected_msg_log_ids",
        "selected_message_log_ids",
        "message_log_ids",
    ):
        values = int_list(payload.get(key))
        if values:
            return values
    return []


def _resolve_self_platform_id(metadata: dict[str, Any], configured: str) -> str:
    for key in ("self_platform_id", "self_id", "bot_platform_id", "bot_self_id"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    return configured


def _filter_candidate_message_ids(
    candidate_ids: list[int],
    source_messages: list[dict[str, Any]],
    self_platform_id: str,
    *,
    message_formatter: MessageFormatterService | None = None,
    message_format_config: Any | None = None,
) -> list[int]:
    if not candidate_ids:
        return []
    source_by_id = {
        int(message["id"]): message
        for message in source_messages
        if isinstance(message.get("id"), int)
    }
    if not source_by_id:
        return candidate_ids
    filtered: list[int] = []
    for candidate_id in candidate_ids:
        message = source_by_id.get(candidate_id)
        if message is None:
            continue
        if _is_low_signal_scan_candidate(
            message,
            self_platform_id,
            message_formatter=message_formatter,
            message_format_config=message_format_config,
        ):
            continue
        filtered.append(candidate_id)
    return filtered


def _is_low_signal_scan_candidate(
    message: dict[str, Any],
    self_platform_id: str,
    *,
    message_formatter: MessageFormatterService | None = None,
    message_format_config: Any | None = None,
) -> bool:
    """Return true when a candidate lacks enough signal for reply review."""

    try:
        parts = parse_message_parts(message, self_platform_id=self_platform_id)
    except Exception:
        return False
    if not parts:
        return False
    has_media_signal = any(part.kind == "image" for part in parts)
    has_self_target = False
    has_other_target = False
    for part in parts:
        if part.kind not in {"mention", "poke"}:
            continue
        target_id = str(part.platform_id or "").strip()
        if target_id and self_platform_id and target_id == self_platform_id:
            has_self_target = True
        elif target_id:
            has_other_target = True
    if has_self_target:
        return False
    if has_other_target:
        return not _has_substantive_non_target_text(parts)
    has_text_signal = any(part.kind == "text" and part.text.strip() for part in parts)
    if has_text_signal:
        return False
    return has_media_signal and not _message_has_media_semantic_hint(
        message,
        message_formatter=message_formatter,
        message_format_config=message_format_config,
    )


def _has_substantive_non_target_text(parts: list[Any]) -> bool:
    text = "".join(str(part.text or "") for part in parts if part.kind == "text").strip()
    normalized = _compact_text_signal(text)
    return len(normalized) > 12


def _compact_text_signal(text: str) -> str:
    return "".join(
        char
        for char in text
        if not char.isspace()
        and char
        not in {
            ",",
            ".",
            "?",
            "!",
            ":",
            ";",
            "，",
            "。",
            "？",
            "！",
            "：",
            "；",
            ">",
            "<",
            "-",
            "_",
            "~",
            "、",
        }
    )


def _message_has_media_semantic_hint(
    message: dict[str, Any],
    *,
    message_formatter: MessageFormatterService | None,
    message_format_config: Any | None,
) -> bool:
    if message_formatter is not None:
        try:
            formatted = message_formatter.format_text(
                [message],
                message_format_config,
            )
        except Exception:
            formatted = ""
        if _formatted_text_has_media_summary(formatted):
            return True
    return _message_metadata_has_media_semantic_hint(message)


def _formatted_text_has_media_summary(text: str) -> bool:
    return any(
        marker in text
        for marker in (
            "[图片: ",
            "[表情: ",
            "摘要:",
        )
    )


def _message_metadata_has_media_semantic_hint(message: dict[str, Any]) -> bool:
    keys = {
        "media_digest",
        "media_summary",
        "image_digest",
        "image_description",
        "image_summary",
    }
    if any(str(message.get(key) or "").strip() for key in keys):
        return True
    metadata = message.get("metadata")
    if isinstance(metadata, dict):
        return any(str(metadata.get(key) or "").strip() for key in keys)
    return False
