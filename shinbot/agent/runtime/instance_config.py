"""Apply per-instance runtime config to Agent model calls."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from shinbot.agent.services.model_runtime.types import ModelRuntimeCall
from shinbot.core.instance_config import ResolvedInstanceRuntimeConfig

InstanceRuntimeConfigResolver = Callable[[str], ResolvedInstanceRuntimeConfig]


@dataclass(slots=True, frozen=True)
class RuntimeModelTarget:
    """Resolved model target selected from instance config."""

    route_id: str | None = None
    model_id: str | None = None


def parse_tagged_llm_ref(value: str) -> RuntimeModelTarget | None:
    """Parse explicit ``[route]``/``[model]`` LLM references."""

    normalized = str(value or "").strip()
    if not normalized:
        return None
    lowered = normalized.lower()
    if lowered.startswith("[route]"):
        route_id = normalized[len("[route]") :].strip()
        return RuntimeModelTarget(route_id=route_id or None)
    if lowered.startswith("[model]"):
        model_id = normalized[len("[model]") :].strip()
        return RuntimeModelTarget(model_id=model_id or None)
    return None


def apply_instance_runtime_config_to_metadata(
    metadata: dict[str, Any],
    resolved: ResolvedInstanceRuntimeConfig | None,
) -> dict[str, Any]:
    """Return model-call metadata enriched with per-instance runtime config."""

    if resolved is None:
        return metadata
    if not resolved.explicit_prompt_cache_enabled:
        return metadata
    return {
        **metadata,
        "explicit_prompt_cache_enabled": True,
    }


def apply_instance_runtime_config_to_call(
    call: ModelRuntimeCall,
    resolved: ResolvedInstanceRuntimeConfig | None,
    *,
    llm: str | None = None,
    default_llm: str | None = None,
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None,
) -> ModelRuntimeCall:
    """Apply runtime config fallbacks to a model call without overriding explicit targets."""

    if resolved is not None and resolved.explicit_prompt_cache_enabled:
        call.metadata = apply_instance_runtime_config_to_metadata(call.metadata, resolved)

    target = resolve_runtime_model_target(
        llm=llm,
        default_llm=default_llm,
        route_id=call.route_id,
        model_id=call.model_id,
        resolved=resolved,
        model_target_resolver=model_target_resolver,
    )
    if target is None:
        return call
    call.route_id = target.route_id
    call.model_id = target.model_id
    return call


def resolve_runtime_model_target(
    *,
    llm: str | None = None,
    default_llm: str | None = None,
    route_id: str | None,
    model_id: str | None,
    resolved: ResolvedInstanceRuntimeConfig | None,
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None,
) -> RuntimeModelTarget | None:
    """Resolve explicit route/model ids, an LLM ref, or an instance fallback."""

    if route_id or model_id:
        return RuntimeModelTarget(route_id=route_id, model_id=model_id)

    stage_target = _resolve_llm_ref(llm, model_target_resolver=model_target_resolver)
    if stage_target is not None:
        return stage_target

    instance_target = _resolve_llm_ref(
        resolved.main_llm if resolved is not None else "",
        model_target_resolver=model_target_resolver,
    )
    if instance_target is not None:
        return instance_target

    default_target = _resolve_llm_ref(
        default_llm,
        model_target_resolver=model_target_resolver,
    )
    if default_target is not None:
        return default_target

    return RuntimeModelTarget(route_id=route_id, model_id=model_id)


def _resolve_llm_ref(
    value: str | None,
    *,
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None,
) -> RuntimeModelTarget | None:
    llm_ref = str(value or "").strip()
    if not llm_ref:
        return None
    tagged = parse_tagged_llm_ref(llm_ref)
    if tagged is not None:
        return tagged
    if model_target_resolver is not None:
        return model_target_resolver(llm_ref)
    return RuntimeModelTarget(route_id=llm_ref)


__all__ = [
    "InstanceRuntimeConfigResolver",
    "RuntimeModelTarget",
    "apply_instance_runtime_config_to_call",
    "apply_instance_runtime_config_to_metadata",
    "parse_tagged_llm_ref",
    "resolve_runtime_model_target",
]
