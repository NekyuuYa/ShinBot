"""Composed model registry repository."""

from __future__ import annotations

from .model_definitions import ModelDefinitionRepositoryMixin
from .model_providers import ModelProviderRepositoryMixin
from .model_routes import ModelRouteRepositoryMixin


class ModelRegistryRepository(
    ModelProviderRepositoryMixin,
    ModelDefinitionRepositoryMixin,
    ModelRouteRepositoryMixin,
):
    """Persistence adapter for provider/model/route metadata."""
