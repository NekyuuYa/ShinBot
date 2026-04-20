from __future__ import annotations

from shinbot.core.bot_config import resolve_bot_runtime_config, select_response_profile


def test_resolve_bot_runtime_config_normalizes_defaults() -> None:
    resolved = resolve_bot_runtime_config(None)

    assert resolved.default_agent_uuid == ""
    assert resolved.main_llm == ""
    assert resolved.response_profile == "balanced"
    assert resolved.response_profile_private == "immediate"
    assert resolved.response_profile_priority == "immediate"
    assert resolved.response_profile_group == "balanced"


def test_resolve_bot_runtime_config_reads_canonical_profiles() -> None:
    resolved = resolve_bot_runtime_config(
        {
            "default_agent_uuid": "agent-1",
            "main_llm": "route-main",
            "config": {
                "response_profile": "PASSIVE",
                "response_profile_private": "IMMEDIATE",
                "response_profile_priority": "Balanced",
                "response_profile_group": "Passive",
            },
            "tags": ["prod"],
        }
    )

    assert resolved.default_agent_uuid == "agent-1"
    assert resolved.main_llm == "route-main"
    assert resolved.response_profile == "passive"
    assert resolved.response_profile_private == "immediate"
    assert resolved.response_profile_priority == "balanced"
    assert resolved.response_profile_group == "passive"
    assert resolved.tags == ["prod"]


def test_select_response_profile_uses_message_priority_order() -> None:
    payload = {
        "config": {
            "response_profile": "balanced",
            "response_profile_private": "immediate",
            "response_profile_priority": "passive",
            "response_profile_group": "balanced",
        }
    }

    assert (
        select_response_profile(
            payload,
            is_private=True,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
        == "immediate"
    )
    assert (
        select_response_profile(
            payload,
            is_private=False,
            is_mentioned=True,
            is_reply_to_bot=False,
        )
        == "passive"
    )
    assert (
        select_response_profile(
            payload,
            is_private=False,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
        == "balanced"
    )
