"""Repository implementations grouped by persistence domain."""

from .admin import (
    AgentRepository,
    BotConfigRepository,
    ContextStrategyRepository,
    PersonaRepository,
    PromptDefinitionRepository,
)
from .ai import AIInteractionRepository, PromptSnapshotRepository
from .base import ContextProvider
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
    "AgentRepository",
    "AIInteractionRepository",
    "AuditRepository",
    "BotConfigRepository",
    "ContextProvider",
    "ContextStrategyRepository",
    "MediaAssetRepository",
    "MediaSemanticRepository",
    "MessageLogRepository",
    "MessageMediaLinkRepository",
    "ModelExecutionRepository",
    "ModelRegistryRepository",
    "PersonaRepository",
    "PromptDefinitionRepository",
    "PromptSnapshotRepository",
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
