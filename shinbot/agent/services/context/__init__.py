"""Context state management primitives."""

from shinbot.agent.services.context.builders.context_stage_builder import (
    ContextStageBuildConfig,
    ContextStageBuilder,
)
from shinbot.agent.services.context.builders.image_summary import (
    ContextImageRegistry,
    ImageSummaryEntry,
)
from shinbot.agent.services.context.builders.message_parts import (
    NormalizedImagePart,
    NormalizedMessagePart,
    parse_message_parts,
)
from shinbot.agent.services.context.manager import ContextManager
from shinbot.agent.services.context.projectors.alias_projector import AliasContextProjector
from shinbot.agent.services.context.projectors.compressed_memory_projector import (
    CompressedMemoryProjector,
)
from shinbot.agent.services.context.projectors.long_term_memory import (
    LongTermMemoryItem,
    LongTermMemoryProjector,
    LongTermMemoryProvider,
    NoopLongTermMemoryProvider,
)
from shinbot.agent.services.context.projectors.projection import (
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
from shinbot.agent.services.context.prompt_registration import register_context_prompt_components
from shinbot.agent.services.context.runtime.alias_runtime import ContextAliasRuntime
from shinbot.agent.services.context.runtime.context_stage_runtime import ContextStageRuntime
from shinbot.agent.services.context.runtime.control_signals import CacheReleaseSignal
from shinbot.agent.services.context.runtime.eviction_runtime import ContextEvictionRuntime
from shinbot.agent.services.context.runtime.pool_runtime import ContextPoolRuntime
from shinbot.agent.services.context.runtime.prompt_memory_assembler import (
    PromptMemoryAssembler,
    PromptMemoryRuntime,
)
from shinbot.agent.services.context.runtime.prompt_runtime import ContextPromptRuntime
from shinbot.agent.services.context.runtime.session_runtime import ContextSessionRuntime
from shinbot.agent.services.context.runtime.timeline_runtime import (
    ContextTimelineRuntime,
    TimelineRun,
)
from shinbot.agent.services.context.state.active_pool import ActiveContextPool
from shinbot.agent.services.context.state.alias_table import AliasEntry, SessionAliasTable
from shinbot.agent.services.context.state.ring_buffer import StableRingIdAllocator
from shinbot.agent.services.context.state.state_store import (
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
    "register_context_prompt_components",
    "block_content_blocks",
    "block_text_parts",
    "block_to_prompt_message",
    "make_record_key",
    "projection_to_context_block",
    "parse_message_parts",
]
