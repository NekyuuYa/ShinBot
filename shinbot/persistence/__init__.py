"""Runtime persistence primitives for ShinBot."""

from shinbot.persistence.config import DatabaseConfig, default_database_path, default_database_url
from shinbot.persistence.engine import DatabaseManager
from shinbot.persistence.records import (
    AgentRecord,
    BotConfigRecord,
    ContextStrategyRecord,
    ModelDefinitionRecord,
    ModelExecutionRecord,
    ModelProviderRecord,
    ModelRouteMemberRecord,
    ModelRouteRecord,
    PersonaRecord,
    PromptDefinitionRecord,
)

__all__ = [
    "DatabaseConfig",
    "DatabaseManager",
    "AgentRecord",
    "BotConfigRecord",
    "ContextStrategyRecord",
    "ModelDefinitionRecord",
    "ModelExecutionRecord",
    "ModelProviderRecord",
    "ModelRouteMemberRecord",
    "ModelRouteRecord",
    "PersonaRecord",
    "PromptDefinitionRecord",
    "default_database_path",
    "default_database_url",
]
