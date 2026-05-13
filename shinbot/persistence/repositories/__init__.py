"""Repository implementations grouped by persistence domain."""

from .admin import (
    ContextStrategyRepository,
    InstanceConfigRepository,
    PersonaRepository,
    PromptDefinitionRepository,
)
from .ai import AIInteractionRepository, PromptSnapshotRepository
from .base import ContextProvider, Repository
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
    "AuditRepository",
    "ContextProvider",
    "ContextStrategyRepository",
    "InstanceConfigRepository",
    "MediaAssetRepository",
    "MediaSemanticRepository",
    "MessageLogRepository",
    "MessageMediaLinkRepository",
    "ModelExecutionRepository",
    "ModelRegistryRepository",
    "PersonaRepository",
    "PromptDefinitionRepository",
    "PromptSnapshotRepository",
    "Repository",
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
