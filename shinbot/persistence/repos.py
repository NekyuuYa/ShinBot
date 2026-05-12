"""Compatibility facade for persistence repository classes.

The implementations live under ``shinbot.persistence.repositories`` grouped by
persistence domain.  This module remains for older imports.
"""

from .repositories import (
    AgentRepository,
    AIInteractionRepository,
    AuditRepository,
    ContextProvider,
    ContextStrategyRepository,
    InstanceConfigRepository,
    MediaAssetRepository,
    MediaSemanticRepository,
    MessageLogRepository,
    MessageMediaLinkRepository,
    ModelExecutionRepository,
    ModelRegistryRepository,
    PersonaRepository,
    PromptDefinitionRepository,
    PromptSnapshotRepository,
    SessionMediaOccurrenceRepository,
    SessionRepository,
)

__all__ = [
    "AgentRepository",
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
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
