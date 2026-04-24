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
from shinbot.agent.context.projection import (
    ContextProjectionState,
    ImageReferenceProjector,
    MessageIdProjector,
    PromptBlockProjection,
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
    make_record_key,
)
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
    "ContextProjectionState",
    "ContextStageBuildConfig",
    "ContextStageBuilder",
    "ContextSessionState",
    "ContextStateStore",
    "ImageSummaryEntry",
    "ImageReferenceProjector",
    "InstructionStageBuildConfig",
    "InstructionStageBuilder",
    "MessageIdProjector",
    "NormalizedImagePart",
    "NormalizedMessagePart",
    "PromptBlockProjection",
    "PromptMemoryBundle",
    "PromptMemoryProjectionRequest",
    "SessionAliasTable",
    "StableRingIdAllocator",
    "make_record_key",
    "parse_message_parts",
]
