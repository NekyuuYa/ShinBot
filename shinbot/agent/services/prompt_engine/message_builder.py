"""Project prompt stage assemblies into model request messages."""

from __future__ import annotations

from typing import Any

from shinbot.agent.services.prompt_engine.schema import (
    PromptStage,
    PromptStageAssembly,
)


class PromptMessageBuilder:
    """Default Chat Completions projection for the 7-stage prompt structure."""

    def build(
        self,
        stage_assembly: PromptStageAssembly,
    ) -> list[dict[str, Any]]:
        """Project 7-stage prompt assembly into Chat Completions messages.

        Maps the structured stage blocks into a ``system`` message
        (base, identity, abilities), context messages, and a final
        ``user`` message (compatibility, instructions, constraints).

        Args:
            stage_assembly: Intermediate stage material to project.

        Returns:
            List of message dicts suitable for a Chat Completions request.
        """
        stage_by_name = {block.stage: block for block in stage_assembly.stages}

        system_content: list[dict[str, Any]] = []
        for stage_key in (
            PromptStage.SYSTEM_BASE,
            PromptStage.IDENTITY,
            PromptStage.ABILITIES,
        ):
            block = stage_by_name[stage_key]
            for record in block.components:
                if record.rendered_text:
                    system_content.append({"type": "text", "text": record.rendered_text})
        system_message: dict[str, Any] = {"role": "system", "content": system_content}

        context_messages = list(stage_by_name[PromptStage.CONTEXT].messages)

        final_content: list[dict[str, Any]] = []
        for stage_key in (
            PromptStage.COMPATIBILITY,
            PromptStage.INSTRUCTIONS,
            PromptStage.CONSTRAINTS,
        ):
            block = stage_by_name[stage_key]
            for record in block.components:
                if record.rendered_content_blocks:
                    final_content.extend(record.rendered_content_blocks)
                    continue
                if record.rendered_text:
                    final_content.append({"type": "text", "text": record.rendered_text})

        messages: list[dict[str, Any]] = [system_message, *context_messages]
        if final_content:
            messages.append({"role": "user", "content": final_content})

        return messages


__all__ = ["PromptMessageBuilder"]
