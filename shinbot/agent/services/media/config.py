"""Media inspection config resolution with built-in fallback scaffolding."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from shinbot.agent.services.prompt_engine.files import load_prompt_component

BUILTIN_MEDIA_INSPECTION_AGENT_REF = "builtin.media_inspection.agent"
BUILTIN_MEDIA_INSPECTION_LLM_REF = "builtin.media_inspection.default"
BUILTIN_MEDIA_INSPECTION_PROMPT_ID = "builtin.prompt.media_inspection"
BUILTIN_STICKER_SUMMARY_AGENT_REF = "builtin.media_inspection.sticker_agent"
BUILTIN_STICKER_SUMMARY_LLM_REF = "builtin.media_inspection.sticker_default"
BUILTIN_STICKER_SUMMARY_PROMPT_ID = "builtin.prompt.sticker_summary"
BUILTIN_MEDIA_REANALYSIS_PROMPT_ID = "builtin.prompt.media_reanalysis"
_PROMPT_LOCALE = "zh-CN"
_PROMPT_ROOT = Path(__file__).resolve().parent / "prompts" / _PROMPT_LOCALE


def _load_builtin_prompt_text(prompt_id: str) -> str:
    return load_prompt_component(_PROMPT_ROOT / f"{prompt_id}.md").content


BUILTIN_MEDIA_INSPECTION_PROMPT = _load_builtin_prompt_text(BUILTIN_MEDIA_INSPECTION_PROMPT_ID)
BUILTIN_STICKER_SUMMARY_PROMPT = _load_builtin_prompt_text(BUILTIN_STICKER_SUMMARY_PROMPT_ID)
BUILTIN_MEDIA_REANALYSIS_PROMPT = _load_builtin_prompt_text(BUILTIN_MEDIA_REANALYSIS_PROMPT_ID)


@dataclass(slots=True)
class ResolvedMediaInspectionConfig:
    """Resolved per-instance config for media/meme inspection."""

    agent_ref: str
    llm_ref: str
    uses_builtin_agent: bool
    uses_builtin_llm: bool
    prompt_ref: str
    uses_builtin_prompt: bool
    sticker_agent_ref: str
    sticker_llm_ref: str
    uses_builtin_sticker_agent: bool
    uses_builtin_sticker_llm: bool
    sticker_prompt_ref: str
    uses_builtin_sticker_prompt: bool
    builtin_prompt_id: str = BUILTIN_MEDIA_INSPECTION_PROMPT_ID
    builtin_prompt: str = BUILTIN_MEDIA_INSPECTION_PROMPT
    builtin_sticker_prompt_id: str = BUILTIN_STICKER_SUMMARY_PROMPT_ID
    builtin_sticker_prompt: str = BUILTIN_STICKER_SUMMARY_PROMPT


def resolve_media_inspection_config(
    bot_config: dict[str, Any] | None,
) -> ResolvedMediaInspectionConfig:
    """Resolve inspection prompt/llm, using only the built-in internal agent wrapper."""

    config = dict((bot_config or {}).get("config") or {})
    llm_ref = str(config.get("media_inspection_llm") or "").strip()
    prompt_ref = str(
        config.get("media_inspection_prompt")
        or config.get("media_inspection_prompt_id")
        or ""
    ).strip()
    sticker_llm_ref = str(config.get("sticker_summary_llm") or "").strip()
    sticker_prompt_ref = str(
        config.get("sticker_summary_prompt")
        or config.get("sticker_summary_prompt_id")
        or ""
    ).strip()

    return ResolvedMediaInspectionConfig(
        agent_ref=BUILTIN_MEDIA_INSPECTION_AGENT_REF,
        llm_ref=llm_ref or BUILTIN_MEDIA_INSPECTION_LLM_REF,
        uses_builtin_agent=True,
        uses_builtin_llm=not bool(llm_ref),
        prompt_ref=prompt_ref or BUILTIN_MEDIA_INSPECTION_PROMPT_ID,
        uses_builtin_prompt=not bool(prompt_ref),
        sticker_agent_ref=BUILTIN_STICKER_SUMMARY_AGENT_REF,
        sticker_llm_ref=sticker_llm_ref or BUILTIN_STICKER_SUMMARY_LLM_REF,
        uses_builtin_sticker_agent=True,
        uses_builtin_sticker_llm=not bool(sticker_llm_ref),
        sticker_prompt_ref=sticker_prompt_ref or BUILTIN_STICKER_SUMMARY_PROMPT_ID,
        uses_builtin_sticker_prompt=not bool(sticker_prompt_ref),
    )
