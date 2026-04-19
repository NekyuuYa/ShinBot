"""Canonical runtime bot-config resolution helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ResolvedBotRuntimeConfig:
    """Normalized runtime-facing bot configuration for a single instance."""

    default_agent_uuid: str = ""
    main_llm: str = ""
    response_profile: str = "balanced"
    response_profile_private: str = "immediate"
    response_profile_priority: str = "immediate"
    response_profile_group: str = "balanced"
    config: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


def _normalize_string(value: Any, default: str = "") -> str:
    normalized = str(value or "").strip().lower() if default else str(value or "").strip()
    return normalized or default


def resolve_bot_runtime_config(payload: dict[str, Any] | None) -> ResolvedBotRuntimeConfig:
    """Normalize raw bot-config payloads into canonical runtime fields."""

    raw_config = dict((payload or {}).get("config") or {})
    return ResolvedBotRuntimeConfig(
        default_agent_uuid=str((payload or {}).get("default_agent_uuid") or "").strip(),
        main_llm=str((payload or {}).get("main_llm") or "").strip(),
        response_profile=_normalize_string(raw_config.get("response_profile"), "balanced"),
        response_profile_private=_normalize_string(
            raw_config.get("response_profile_private"),
            "immediate",
        ),
        response_profile_priority=_normalize_string(
            raw_config.get("response_profile_priority"),
            "immediate",
        ),
        response_profile_group=_normalize_string(
            raw_config.get("response_profile_group"),
            _normalize_string(raw_config.get("response_profile"), "balanced"),
        ),
        config=raw_config,
        tags=list((payload or {}).get("tags") or []),
    )


def select_response_profile(
    payload: dict[str, Any] | None,
    *,
    is_private: bool,
    is_mentioned: bool,
    is_reply_to_bot: bool,
) -> str:
    """Select the canonical response profile for one incoming message."""

    resolved = resolve_bot_runtime_config(payload)
    if is_private:
        return resolved.response_profile_private
    if is_mentioned or is_reply_to_bot:
        return resolved.response_profile_priority
    return resolved.response_profile_group
