"""Runtime persistence primitives for ShinBot."""

from shinbot.persistence.config import DatabaseConfig, default_database_path, default_database_url
from shinbot.persistence.engine import DatabaseManager
from shinbot.persistence.records import (
    AgentRecord,
    AIInteractionRecord,
    BotConfigRecord,
    ContextStrategyRecord,
    MessageLogRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PersonaRecord,
    PromptDefinitionRecord,
    PromptSnapshotRecord,
)

__all__ = [
    "AIInteractionRecord",
    "DatabaseConfig",
    "DatabaseManager",
    "AgentRecord",
    "BotConfigRecord",
    "ContextStrategyRecord",
    "MessageLogRecord",
    "ModelDefinitionRecord",
    "ModelExecutionRecord",
    "ModelProviderRecord",
    "ModelRouteMemberRecord",
    "ModelRouteRecord",
    "PersonaRecord",
    "PromptDefinitionRecord",
    "PromptSnapshotRecord",
    "default_database_path",
    "default_database_url",
]
