"""Compatibility facade for persistence repository classes.

The implementations live under ``shinbot.persistence.repositories`` grouped by
persistence domain.  This module remains for older imports.
"""

from .repositories import (
    AIInteractionRepository,
    AuditRepository,
    ContextProvider,
    InstanceConfigRepository,
    MediaAssetRepository,
    MediaSemanticRepository,
    MessageLogRepository,
    MessageMediaLinkRepository,
    ModelExecutionRepository,
    ModelRegistryRepository,
    PromptDefinitionRepository,
    PromptSnapshotRepository,
    SessionMediaOccurrenceRepository,
    SessionRepository,
)

__all__ = [
    "AIInteractionRepository",
    "AuditRepository",
    "ContextProvider",
    "InstanceConfigRepository",
    "MediaAssetRepository",
    "MediaSemanticRepository",
    "MessageLogRepository",
    "MessageMediaLinkRepository",
    "ModelExecutionRepository",
    "ModelRegistryRepository",
    "PromptDefinitionRepository",
    "PromptSnapshotRepository",
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
