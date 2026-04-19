"""Identity mapping primitives for multi-user conversations."""

from shinbot.agent.identity.prompt_registration import register_identity_prompt_components
from shinbot.agent.identity.prompt_runtime import (
    inject_identity_layers_into_messages,
    resolve_identity_map_prompt,
)
from shinbot.agent.identity.store import IdentityStore

__all__ = [
    "IdentityStore",
    "inject_identity_layers_into_messages",
    "register_identity_prompt_components",
    "resolve_identity_map_prompt",
]
