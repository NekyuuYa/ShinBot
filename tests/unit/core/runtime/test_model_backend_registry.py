from __future__ import annotations

from shinbot.core.runtime.model_backend import (
    DEFAULT_MODEL_BACKEND,
    SUPPORTED_MODEL_BACKENDS,
    create_model_backend,
    model_backend_name_from_config,
)


def test_model_backend_defaults_to_registered_default() -> None:
    backend = create_model_backend({})
    assert backend.name == DEFAULT_MODEL_BACKEND
    assert DEFAULT_MODEL_BACKEND in SUPPORTED_MODEL_BACKENDS


def test_model_backend_uses_registered_backend_name() -> None:
    backend = create_model_backend({"runtime": {"model_backend": {"type": "openai_compatible"}}})
    assert backend.name == "openai_compatible"


def test_model_backend_name_from_config_falls_back_to_default() -> None:
    assert model_backend_name_from_config(None) == DEFAULT_MODEL_BACKEND
    assert model_backend_name_from_config({"runtime": {}}) == DEFAULT_MODEL_BACKEND
