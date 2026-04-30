"""Compatibility exports for model persistence repositories."""

from shinbot.persistence.repositories.model_execution import ModelExecutionRepository
from shinbot.persistence.repositories.model_registry import ModelRegistryRepository

__all__ = [
    "ModelExecutionRepository",
    "ModelRegistryRepository",
]
