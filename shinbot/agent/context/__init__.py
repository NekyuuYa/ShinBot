"""Context state management primitives."""

from shinbot.agent.context.alias_table import AliasEntry, SessionAliasTable
from shinbot.agent.context.context_stage_builder import ContextStageBuildConfig, ContextStageBuilder
from shinbot.agent.context.image_summary import ContextImageRegistry, ImageSummaryEntry
from shinbot.agent.context.instruction_stage_builder import (
    InstructionStageBuildConfig,
    InstructionStageBuilder,
)
from shinbot.agent.context.manager import ActiveContextPool, ContextManager
from shinbot.agent.context.message_parts import (
    NormalizedImagePart,
    NormalizedMessagePart,
    parse_message_parts,
)
from shinbot.agent.context.projection import PromptMemoryBundle, PromptMemoryProjectionRequest
from shinbot.agent.context.ring_buffer import StableRingIdAllocator
from shinbot.agent.context.state_store import (
    CompressedMemoryState,
    ContextBlockState,
    ContextSessionState,
    ContextStateStore,
)

__all__ = [
    "ActiveContextPool",
    "AliasEntry",
    "CompressedMemoryState",
    "ContextImageRegistry",
    "ContextManager",
    "ContextBlockState",
    "ContextStageBuildConfig",
    "ContextStageBuilder",
    "ContextSessionState",
    "ContextStateStore",
    "ImageSummaryEntry",
    "InstructionStageBuildConfig",
    "InstructionStageBuilder",
    "NormalizedImagePart",
    "NormalizedMessagePart",
    "PromptMemoryBundle",
    "PromptMemoryProjectionRequest",
    "SessionAliasTable",
    "StableRingIdAllocator",
    "parse_message_parts",
]
