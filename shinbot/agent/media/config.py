"""Media inspection config resolution with built-in fallback scaffolding."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

BUILTIN_MEDIA_INSPECTION_AGENT_REF = "builtin.media_inspection.agent"
BUILTIN_MEDIA_INSPECTION_LLM_REF = "builtin.media_inspection.default"
BUILTIN_MEDIA_INSPECTION_PROMPT_ID = "builtin.prompt.media_inspection"
BUILTIN_MEDIA_INSPECTION_PROMPT = """
You are ShinBot's media inspection agent.

Determine whether the supplied media should be treated as:
- generic_image
- meme_image
- emoji_native

When the media is a meme or emoji-like image, produce a digest no longer than 50 Chinese characters.
Prefer concise, dialogue-oriented descriptions that preserve the main attitude, visible text, and key subject.
Return structured results only.
""".strip()


@dataclass(slots=True)
class ResolvedMediaInspectionConfig:
    """Resolved per-instance config for media/meme inspection."""

    agent_ref: str
    llm_ref: str
    uses_builtin_agent: bool
    uses_builtin_llm: bool
    builtin_prompt_id: str = BUILTIN_MEDIA_INSPECTION_PROMPT_ID
    builtin_prompt: str = BUILTIN_MEDIA_INSPECTION_PROMPT


def resolve_media_inspection_config(
    bot_config: dict[str, Any] | None,
) -> ResolvedMediaInspectionConfig:
    """Resolve inspection agent/llm, falling back to built-in defaults."""

    config = dict((bot_config or {}).get("config") or {})
    agent_ref = str(
        config.get("media_inspection_agent")
        or config.get("media_inspection_agent_uuid")
        or ""
    ).strip()
    llm_ref = str(config.get("media_inspection_llm") or "").strip()

    return ResolvedMediaInspectionConfig(
        agent_ref=agent_ref or BUILTIN_MEDIA_INSPECTION_AGENT_REF,
        llm_ref=llm_ref or BUILTIN_MEDIA_INSPECTION_LLM_REF,
        uses_builtin_agent=not bool(agent_ref),
        uses_builtin_llm=not bool(llm_ref),
    )
