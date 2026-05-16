"""Compatibility exports for administrative metadata repositories."""

from shinbot.persistence.repositories.admin_instance_configs import InstanceConfigRepository
from shinbot.persistence.repositories.admin_personas import PersonaRepository
from shinbot.persistence.repositories.admin_prompt_definitions import PromptDefinitionRepository

__all__ = [
    "InstanceConfigRepository",
    "PersonaRepository",
    "PromptDefinitionRepository",
]
