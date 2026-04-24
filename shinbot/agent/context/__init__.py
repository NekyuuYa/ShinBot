"""Context state management primitives."""

from shinbot.agent.context.builders.context_stage_builder import (
    ContextStageBuildConfig,
    ContextStageBuilder,
)
from shinbot.agent.context.builders.image_summary import ContextImageRegistry, ImageSummaryEntry
from shinbot.agent.context.builders.instruction_stage_builder import (
    InstructionStageBuildConfig,
    InstructionStageBuilder,
)
from shinbot.agent.context.builders.message_parts import (
    NormalizedImagePart,
    NormalizedMessagePart,
    parse_message_parts,
)
from shinbot.agent.context.manager import ContextManager
from shinbot.agent.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.context.projectors.compressed_memory_projector import CompressedMemoryProjector
from shinbot.agent.context.projectors.long_term_memory import (
    LongTermMemoryItem,
    LongTermMemoryProjector,
    LongTermMemoryProvider,
    NoopLongTermMemoryProvider,
)
from shinbot.agent.context.projectors.projection import (
    ContextBlockAdapter,
    ContextProjectionState,
    ImageReferenceProjector,
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
from shinbot.agent.context.runtime.alias_runtime import ContextAliasRuntime
from shinbot.agent.context.runtime.context_stage_runtime import ContextStageRuntime
from shinbot.agent.context.runtime.control_signals import CacheReleaseSignal
from shinbot.agent.context.runtime.eviction_runtime import ContextEvictionRuntime
from shinbot.agent.context.runtime.instruction_runtime import InstructionRuntime
from shinbot.agent.context.runtime.pool_runtime import ContextPoolRuntime
from shinbot.agent.context.runtime.prompt_memory_assembler import (
    PromptMemoryAssembler,
    PromptMemoryRuntime,
)
from shinbot.agent.context.runtime.prompt_runtime import ContextPromptRuntime
from shinbot.agent.context.runtime.session_runtime import ContextSessionRuntime
from shinbot.agent.context.runtime.timeline_runtime import ContextTimelineRuntime, TimelineRun
from shinbot.agent.context.state.active_pool import ActiveContextPool
from shinbot.agent.context.state.alias_table import AliasEntry, SessionAliasTable
from shinbot.agent.context.state.ring_buffer import StableRingIdAllocator
from shinbot.agent.context.state.state_store import (
    CompressedMemoryState,
    ContextBlockState,
    ContextSessionState,
    ContextStateStore,
    OpenBlockState,
    SealedBlockDequeState,
    ShortTermMemoryState,
)

__all__ = [
    "ActiveContextPool",
    "AliasContextProjector",
    "AliasEntry",
    "CacheReleaseSignal",
    "CompressedMemoryProjector",
    "CompressedMemoryState",
    "ContextImageRegistry",
    "ContextManager",
    "ContextBlockState",
    "ContextAliasRuntime",
    "ContextBlockAdapter",
    "ContextEvictionRuntime",
    "ContextProjectionState",
    "ContextStageBuildConfig",
    "ContextStageBuilder",
    "ContextStageRuntime",
    "ContextSessionState",
    "ContextStateStore",
    "ContextTimelineRuntime",
    "ImageSummaryEntry",
    "ImageReferenceProjector",
    "InstructionStageBuildConfig",
    "InstructionStageBuilder",
    "InstructionRuntime",
    "LongTermMemoryItem",
    "LongTermMemoryProjector",
    "LongTermMemoryProvider",
    "MessageIdProjector",
    "NoopLongTermMemoryProvider",
    "NormalizedImagePart",
    "NormalizedMessagePart",
    "OpenBlockState",
    "ContextPoolRuntime",
    "ContextSessionRuntime",
    "PromptBlockProjection",
    "ContextPromptRuntime",
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
