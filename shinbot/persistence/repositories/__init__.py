"""Repository implementations grouped by persistence domain."""

from .admin import InstanceConfigRepository
from .agent_runtime_diagnostics import AgentRuntimeDiagnosticsRepository
from .agent_runtime_ownership import AgentRuntimeOwnershipRepository
from .ai import AIInteractionRepository, PromptSnapshotRepository
from .base import ContextProvider, Repository
from .durable_routing import DurableMessageRoutingRepository
from .media import (
    MediaAssetRepository,
    MediaSemanticRepository,
    MessageMediaLinkRepository,
    SessionMediaOccurrenceRepository,
)
from .messages import MessageLogRepository
from .model import ModelExecutionRepository, ModelRegistryRepository
from .sessions import AuditRepository, SessionRepository

__all__ = [
    "AIInteractionRepository",
    "AgentRuntimeDiagnosticsRepository",
    "AgentRuntimeOwnershipRepository",
    "AuditRepository",
    "ContextProvider",
    "DurableMessageRoutingRepository",
    "InstanceConfigRepository",
    "MediaAssetRepository",
    "MediaSemanticRepository",
    "MessageLogRepository",
    "MessageMediaLinkRepository",
    "ModelExecutionRepository",
    "ModelRegistryRepository",
    "PromptSnapshotRepository",
    "Repository",
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
