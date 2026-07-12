"""Shared chat action tool registration for Agent workflows."""

from shinbot.agent.workflows.chat_actions.intents import (
    EXTERNAL_ACTION_TOOL_NAMES,
    ExternalActionToolMode,
    collect_external_action_intent,
)
from shinbot.agent.workflows.chat_actions.tool_registration import (
    CHAT_ACTION_TOOL_TAG,
    register_chat_action_tools,
)

__all__ = [
    "CHAT_ACTION_TOOL_TAG",
    "EXTERNAL_ACTION_TOOL_NAMES",
    "ExternalActionToolMode",
    "collect_external_action_intent",
    "register_chat_action_tools",
]
