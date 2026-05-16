"""Runtime persistence primitives for ShinBot."""

from shinbot.persistence.config import (
    DatabaseConfig,
    default_database_path,
    default_database_url,
    default_model_registry_path,
)
from shinbot.persistence.engine import DatabaseManager
from shinbot.persistence.records import (
    AIInteractionRecord,
    InstanceConfigRecord,
    MediaAssetRecord,
    MediaSemanticRecord,
    MessageLogRecord,
    MessageMediaLinkRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PromptSnapshotRecord,
    SessionMediaOccurrenceRecord,
)
from shinbot.persistence.repos import ContextProvider

__all__ = [
    "AIInteractionRecord",
    "DatabaseConfig",
    "DatabaseManager",
    "ContextProvider",
    "InstanceConfigRecord",
    "MediaAssetRecord",
    "MediaSemanticRecord",
    "MessageLogRecord",
    "MessageMediaLinkRecord",
    "ModelDefinitionRecord",
    "ModelExecutionRecord",
    "ModelProviderRecord",
    "ModelRouteMemberRecord",
    "ModelRouteRecord",
    "PromptSnapshotRecord",
    "SessionMediaOccurrenceRecord",
    "default_database_path",
    "default_database_url",
    "default_model_registry_path",
]
