"""Compatibility facade for persistence repository classes.

The implementations live under ``shinbot.persistence.repositories`` grouped by
persistence domain.  This module remains for older imports.
"""

from .repositories import (
    AgentRuntimeDiagnosticsRepository,
    AgentRuntimeOwnershipRepository,
    AIInteractionRepository,
    AuditRepository,
    ContextProvider,
    DurableMessageRoutingRepository,
    InstanceConfigRepository,
    MediaAssetRepository,
    MediaSemanticRepository,
    MessageLogRepository,
    MessageMediaLinkRepository,
    ModelExecutionRepository,
    ModelRegistryRepository,
    PromptSnapshotRepository,
    SessionMediaOccurrenceRepository,
    SessionRepository,
)

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
    "SessionMediaOccurrenceRepository",
    "SessionRepository",
]
