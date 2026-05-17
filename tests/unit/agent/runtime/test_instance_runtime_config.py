from __future__ import annotations

from shinbot.agent.runtime.instance_config import (
    RuntimeModelTarget,
    apply_instance_runtime_config_to_call,
    parse_tagged_llm_ref,
    resolve_runtime_model_target,
)
from shinbot.agent.services.model_runtime import ModelRuntimeCall
from shinbot.core.instance_config import resolve_instance_runtime_config, select_response_profile


def test_resolve_instance_runtime_config_normalizes_defaults() -> None:
    resolved = resolve_instance_runtime_config(None)

    assert resolved.main_llm == ""
    assert resolved.explicit_prompt_cache_enabled is False
    assert resolved.response_profile == "balanced"
    assert resolved.response_profile_private == "disabled"
    assert resolved.response_profile_priority == "immediate"
    assert resolved.response_profile_group == "balanced"


def test_resolve_instance_runtime_config_reads_canonical_profiles() -> None:
    resolved = resolve_instance_runtime_config(
        {
            "main_llm": "route-main",
            "config": {
                "explicit_prompt_cache_enabled": "true",
                "response_profile": "PASSIVE",
                "response_profile_private": "IMMEDIATE",
                "response_profile_priority": "Balanced",
                "response_profile_group": "Passive",
            },
            "tags": ["prod"],
        }
    )

    assert resolved.main_llm == "route-main"
    assert resolved.explicit_prompt_cache_enabled is True
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


def test_select_response_profile_disables_private_attention_by_default() -> None:
    assert (
        select_response_profile(
            None,
            is_private=True,
            is_mentioned=False,
            is_reply_to_bot=False,
        )
        == "disabled"
    )


def test_tagged_llm_refs_force_target_kind() -> None:
    assert parse_tagged_llm_ref("[route]main") == RuntimeModelTarget(route_id="main")
    assert parse_tagged_llm_ref("[model]gemini-pro") == RuntimeModelTarget(
        model_id="gemini-pro"
    )
    assert parse_tagged_llm_ref("main") is None


def test_resolve_runtime_model_target_uses_llm_before_instance_fallback() -> None:
    resolved = resolve_instance_runtime_config({"main_llm": "[route]instance-main"})

    target = resolve_runtime_model_target(
        llm="[model]stage-model",
        route_id=None,
        model_id=None,
        resolved=resolved,
    )

    assert target == RuntimeModelTarget(model_id="stage-model")


def test_resolve_runtime_model_target_prefers_instance_before_default_llm() -> None:
    resolved = resolve_instance_runtime_config({"main_llm": "[route]instance-main"})

    target = resolve_runtime_model_target(
        llm="",
        default_llm="[route]agent-default",
        route_id=None,
        model_id=None,
        resolved=resolved,
    )

    assert target == RuntimeModelTarget(route_id="instance-main")


def test_resolve_runtime_model_target_uses_default_llm_without_instance() -> None:
    target = resolve_runtime_model_target(
        llm="",
        default_llm="[route]agent-default",
        route_id=None,
        model_id=None,
        resolved=None,
    )

    assert target == RuntimeModelTarget(route_id="agent-default")


def test_resolve_runtime_model_target_uses_resolver_for_untagged_llm() -> None:
    target = resolve_runtime_model_target(
        llm="main",
        route_id=None,
        model_id=None,
        resolved=None,
        model_target_resolver=lambda value: RuntimeModelTarget(route_id=f"route-{value}"),
    )

    assert target == RuntimeModelTarget(route_id="route-main")


def test_apply_instance_runtime_config_to_call_uses_llm_without_instance_config() -> None:
    call = ModelRuntimeCall(caller="test")

    result = apply_instance_runtime_config_to_call(
        call,
        None,
        llm="[route]stage-main",
    )

    assert result.route_id == "stage-main"
    assert result.model_id is None
