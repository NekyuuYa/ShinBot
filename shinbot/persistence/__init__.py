"""Runtime persistence primitives for ShinBot."""

from shinbot.persistence.config import DatabaseConfig, default_database_path, default_database_url
from shinbot.persistence.engine import DatabaseManager
from shinbot.persistence.records import (
    AgentRecord,
    AIInteractionRecord,
    BotConfigRecord,
    ContextStrategyRecord,
    MediaAssetRecord,
    MediaSemanticRecord,
    MessageLogRecord,
    MessageMediaLinkRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PersonaRecord,
    PromptDefinitionRecord,
    PromptSnapshotRecord,
    SessionMediaOccurrenceRecord,
)
from shinbot.persistence.repos import ContextProvider

__all__ = [
    "AIInteractionRecord",
    "DatabaseConfig",
    "DatabaseManager",
    "AgentRecord",
    "BotConfigRecord",
    "ContextStrategyRecord",
    "ContextProvider",
    "MediaAssetRecord",
    "MediaSemanticRecord",
    "MessageLogRecord",
    "MessageMediaLinkRecord",
    "ModelDefinitionRecord",
    "ModelExecutionRecord",
    "ModelProviderRecord",
    "ModelRouteMemberRecord",
    "ModelRouteRecord",
    "PersonaRecord",
    "PromptDefinitionRecord",
    "PromptSnapshotRecord",
    "SessionMediaOccurrenceRecord",
    "default_database_path",
    "default_database_url",
]
