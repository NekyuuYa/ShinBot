"""Shared heading text for context projection messages."""

from __future__ import annotations

from pathlib import Path

from shinbot.agent.services.prompt_engine.files import load_prompt_component

LONG_TERM_MEMORY_COMPONENT_ID = "builtin.context.long_term_memory"
COMPRESSED_MEMORY_COMPONENT_ID = "builtin.context.compressed_memory"
COMPRESSED_MEMORY_SOURCE_COMPONENT_ID = "builtin.context.compressed_memory_source"
COMPRESSED_MEMORY_ALIAS_COMPONENT_ID = "builtin.context.compressed_memory_alias"
INACTIVE_ALIAS_COMPONENT_ID = "builtin.context.inactive_alias"
ACTIVE_ALIAS_COMPONENT_ID = "builtin.context.active_alias"

_PROMPT_DIR = Path(__file__).resolve().parents[1] / "prompts" / "zh-CN"

LONG_TERM_MEMORY_HEADING = load_prompt_component(
    _PROMPT_DIR / f"{LONG_TERM_MEMORY_COMPONENT_ID}.md"
).content
COMPRESSED_MEMORY_HEADING = load_prompt_component(
    _PROMPT_DIR / f"{COMPRESSED_MEMORY_COMPONENT_ID}.md"
).content
COMPRESSED_MEMORY_SOURCE_HEADING = load_prompt_component(
    _PROMPT_DIR / f"{COMPRESSED_MEMORY_SOURCE_COMPONENT_ID}.md"
).content
COMPRESSED_MEMORY_ALIAS_HEADING = load_prompt_component(
    _PROMPT_DIR / f"{COMPRESSED_MEMORY_ALIAS_COMPONENT_ID}.md"
).content
INACTIVE_ALIAS_HEADING = load_prompt_component(
    _PROMPT_DIR / f"{INACTIVE_ALIAS_COMPONENT_ID}.md"
).content
ACTIVE_ALIAS_HEADING = load_prompt_component(
    _PROMPT_DIR / f"{ACTIVE_ALIAS_COMPONENT_ID}.md"
).content
