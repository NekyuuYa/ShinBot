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
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None,
) -> ModelRuntimeCall:
    """Apply runtime config fallbacks to a model call without overriding explicit targets."""

    if resolved is None:
        return call

    if resolved.explicit_prompt_cache_enabled:
        call.metadata = apply_instance_runtime_config_to_metadata(call.metadata, resolved)

    target = resolve_runtime_model_target(
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
    route_id: str | None,
    model_id: str | None,
    resolved: ResolvedInstanceRuntimeConfig | None,
    model_target_resolver: Callable[[str], RuntimeModelTarget | None] | None = None,
) -> RuntimeModelTarget | None:
    """Resolve explicit route/model ids or an instance ``main_llm`` fallback."""

    if route_id or model_id:
        return RuntimeModelTarget(route_id=route_id, model_id=model_id)
    if resolved is None or not resolved.main_llm:
        return RuntimeModelTarget(route_id=route_id, model_id=model_id)
    if model_target_resolver is None:
        return RuntimeModelTarget(route_id=resolved.main_llm)
    return model_target_resolver(resolved.main_llm)


__all__ = [
    "InstanceRuntimeConfigResolver",
    "RuntimeModelTarget",
    "apply_instance_runtime_config_to_call",
    "apply_instance_runtime_config_to_metadata",
    "resolve_runtime_model_target",
]
