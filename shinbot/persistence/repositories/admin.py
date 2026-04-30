"""Compatibility exports for administrative metadata repositories."""

from shinbot.persistence.repositories.admin_agents import AgentRepository
from shinbot.persistence.repositories.admin_bot_configs import BotConfigRepository
from shinbot.persistence.repositories.admin_context_strategies import ContextStrategyRepository
from shinbot.persistence.repositories.admin_personas import PersonaRepository
from shinbot.persistence.repositories.admin_prompt_definitions import PromptDefinitionRepository

__all__ = [
    "AgentRepository",
    "BotConfigRepository",
    "ContextStrategyRepository",
    "PersonaRepository",
    "PromptDefinitionRepository",
]
