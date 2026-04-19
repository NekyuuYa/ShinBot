"""Runtime prompt helpers."""

from shinbot.agent.runtime.prompt_registration import register_runtime_prompt_components
from shinbot.agent.runtime.prompt_runtime import resolve_current_time_prompt

__all__ = [
    "register_runtime_prompt_components",
    "resolve_current_time_prompt",
]
