"""Explicit prompt projection for review stage runtime inputs."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from shinbot.agent.runners.templates.review_instruction import (
    review_stage_instruction_component_id,
)
from shinbot.agent.services.context.review_context_builder import ReviewStageInput
from shinbot.agent.services.message_formatter import (
    MessageFormatConfig,
    MessageFormatterService,
)
from shinbot.agent.services.prompt_engine import PromptInjection, PromptStage

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class ReviewPromptProjection:
    """Prompt injections and audit metadata projected from one stage input."""

    injections: tuple[PromptInjection, ...]
    disabled_component_ids: tuple[str, ...]
    audit_metadata: dict[str, object]


class ReviewPromptProjector:
    """Project every review runtime input through explicit prompt injections."""

    def __init__(
        self,
        *,
        message_formatter: MessageFormatterService | None = None,
        message_format_config: MessageFormatConfig | None = None,
    ) -> None:
        self._message_formatter = message_formatter
        self._message_format_config = message_format_config

    def project(self, stage_input: ReviewStageInput) -> ReviewPromptProjection:
        """Convert stage context, instructions, and source records into injections.

        ``instruction_content`` is the authoritative rendered representation of
        ``source_messages`` when present. Raw source records are formatted only
        when no instruction blocks were supplied, which prevents the same input
        from appearing twice in the final model request.

        Args:
            stage_input: Structured runtime input for one review stage.

        Returns:
            Explicit prompt injections plus content-free audit metadata.
        """

        runtime_component_id = review_stage_instruction_component_id(stage_input.purpose)
        injections: list[PromptInjection] = []
        if stage_input.context_messages:
            injections.append(
                PromptInjection(
                    stage=PromptStage.CONTEXT,
                    component_id=f"review.runtime.{stage_input.purpose}.context",
                    messages=[dict(message) for message in stage_input.context_messages],
                    priority=10,
                    metadata={
                        "source": "review_stage_input.context_messages",
                        "message_count": len(stage_input.context_messages),
                    },
                )
            )

        injections.append(
            PromptInjection(
                stage=PromptStage.INSTRUCTIONS,
                component_id=runtime_component_id,
                content_blocks=self._instruction_blocks(stage_input),
                priority=10,
                metadata={
                    "source": "review_stage_input",
                    "review_stage": stage_input.purpose,
                    "source_message_count": len(stage_input.source_messages),
                    "instruction_block_count": len(stage_input.instruction_content),
                },
            )
        )
        return ReviewPromptProjection(
            injections=tuple(injections),
            disabled_component_ids=(runtime_component_id,),
            audit_metadata={
                "review_stage": stage_input.purpose,
                "review_stage_metadata": dict(stage_input.metadata),
                "review_input_projection": {
                    "context_message_count": len(stage_input.context_messages),
                    "source_message_count": len(stage_input.source_messages),
                    "instruction_block_count": len(stage_input.instruction_content),
                },
                **dict(stage_input.metadata),
            },
        )

    def _instruction_blocks(
        self,
        stage_input: ReviewStageInput,
    ) -> list[dict[str, Any]]:
        metadata_json = json.dumps(
            stage_input.metadata,
            ensure_ascii=False,
            sort_keys=True,
        )
        blocks: list[dict[str, Any]] = [
            {
                "type": "text",
                "text": (f"Stage purpose: {stage_input.purpose}\nMetadata JSON: {metadata_json}"),
            }
        ]
        if stage_input.instruction_content:
            blocks.extend(dict(block) for block in stage_input.instruction_content)
            return blocks

        formatted_text = self._format_source_messages(stage_input)
        if formatted_text:
            blocks.append(
                {
                    "type": "text",
                    "text": "Source messages:\n" + formatted_text,
                }
            )
        else:
            blocks.append(
                {
                    "type": "text",
                    "text": "Source messages JSON:\n"
                    + json.dumps(
                        stage_input.source_messages,
                        ensure_ascii=False,
                    ),
                }
            )
        return blocks

    def _format_source_messages(self, stage_input: ReviewStageInput) -> str:
        if self._message_formatter is None or not stage_input.source_messages:
            return ""
        try:
            return self._message_formatter.format_text(
                list(stage_input.source_messages),
                self._message_format_config,
            )
        except Exception:
            logger.exception(
                "ReviewPromptProjector message formatting failed for stage %s session %s",
                stage_input.purpose,
                stage_input.session_id,
            )
            return ""


__all__ = ["ReviewPromptProjection", "ReviewPromptProjector"]
