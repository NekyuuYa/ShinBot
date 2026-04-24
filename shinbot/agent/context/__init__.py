"""Context state management primitives."""

from shinbot.agent.context.active_pool import ActiveContextPool
from shinbot.agent.context.alias_projector import AliasContextProjector
from shinbot.agent.context.alias_table import AliasEntry, SessionAliasTable
from shinbot.agent.context.compressed_memory_projector import CompressedMemoryProjector
from shinbot.agent.context.context_stage_builder import ContextStageBuildConfig, ContextStageBuilder
from shinbot.agent.context.image_summary import ContextImageRegistry, ImageSummaryEntry
from shinbot.agent.context.instruction_stage_builder import (
    InstructionStageBuildConfig,
    InstructionStageBuilder,
)
from shinbot.agent.context.long_term_memory import (
    LongTermMemoryItem,
    LongTermMemoryProjector,
    LongTermMemoryProvider,
    NoopLongTermMemoryProvider,
)
from shinbot.agent.context.manager import ContextManager
from shinbot.agent.context.message_parts import (
    NormalizedImagePart,
    NormalizedMessagePart,
    parse_message_parts,
)
from shinbot.agent.context.projection import (
    ContextProjectionState,
    ImageReferenceProjector,
    LegacyBlockAdapter,
    MessageIdProjector,
    PromptBlockProjection,
    PromptMemoryBundle,
    PromptMemoryProjectionRequest,
    block_content_blocks,
    block_text_parts,
    block_to_prompt_message,
    make_record_key,
    projection_to_context_block,
)
from shinbot.agent.context.prompt_memory_assembler import (
    PromptMemoryAssembler,
    PromptMemoryRuntime,
)
from shinbot.agent.context.ring_buffer import StableRingIdAllocator
from shinbot.agent.context.state_store import (
    CompressedMemoryState,
    ContextBlockState,
    ContextSessionState,
    ContextStateStore,
    OpenBlockState,
    SealedBlockDequeState,
    ShortTermMemoryState,
)
from shinbot.agent.context.timeline_runtime import ContextTimelineRuntime, TimelineRun

__all__ = [
    "ActiveContextPool",
    "AliasContextProjector",
    "AliasEntry",
    "CompressedMemoryProjector",
    "CompressedMemoryState",
    "ContextImageRegistry",
    "ContextManager",
    "ContextBlockState",
    "ContextProjectionState",
    "ContextStageBuildConfig",
    "ContextStageBuilder",
    "ContextSessionState",
    "ContextStateStore",
    "ContextTimelineRuntime",
    "ImageSummaryEntry",
    "ImageReferenceProjector",
    "InstructionStageBuildConfig",
    "InstructionStageBuilder",
    "LegacyBlockAdapter",
    "LongTermMemoryItem",
    "LongTermMemoryProjector",
    "LongTermMemoryProvider",
    "MessageIdProjector",
    "NoopLongTermMemoryProvider",
    "NormalizedImagePart",
    "NormalizedMessagePart",
    "OpenBlockState",
    "PromptBlockProjection",
    "PromptMemoryAssembler",
    "PromptMemoryBundle",
    "PromptMemoryProjectionRequest",
    "PromptMemoryRuntime",
    "SessionAliasTable",
    "SealedBlockDequeState",
    "ShortTermMemoryState",
    "StableRingIdAllocator",
    "TimelineRun",
    "block_content_blocks",
    "block_text_parts",
    "block_to_prompt_message",
    "make_record_key",
    "projection_to_context_block",
    "parse_message_parts",
]
