"""Review coordinator sub-package."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ReviewCoordinator",
    "ReviewRunnerFactory",
    "ReviewRuntimeConfig",
    "ReviewStageRuntimeConfig",
    "ReviewWorkflowConfig",
    "ReviewWorkflowResult",
    "ReviewWorkflowExplanation",
    "build_review_workflow_explanation",
    "register_review_prompt_components",
]

_EXPORT_MODULES = {
    "ReviewCoordinator": "shinbot.agent.coordinators.review.coordinator",
    "ReviewRunnerFactory": "shinbot.agent.coordinators.review.factory",
    "ReviewRuntimeConfig": "shinbot.agent.coordinators.review.factory",
    "ReviewStageRuntimeConfig": "shinbot.agent.coordinators.review.factory",
    "ReviewWorkflowConfig": "shinbot.agent.coordinators.review.models",
    "ReviewWorkflowResult": "shinbot.agent.coordinators.review.models",
    "ReviewWorkflowExplanation": "shinbot.agent.coordinators.review.models",
    "build_review_workflow_explanation": "shinbot.agent.coordinators.review.models",
    "register_review_prompt_components": "shinbot.agent.coordinators.review.factory",
}


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value
