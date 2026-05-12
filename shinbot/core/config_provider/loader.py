"""Load provider-owned config schemas from TOML files."""

from __future__ import annotations

import tomllib
from importlib import import_module
from pathlib import Path
from types import ModuleType
from typing import Any

from shinbot.core.config_provider.schema import (
    ConfigFieldDefinition,
    ConfigFieldType,
    ConfigProviderDefinition,
    ConfigProviderKind,
)


class ConfigProviderLoadError(RuntimeError):
    """Raised when a provider config schema cannot be loaded."""


_FIELD_KEYS = {
    "path",
    "type",
    "required",
    "default",
    "choices",
    "min",
    "max",
    "secret",
    "env",
    "placeholder",
    "description",
    "visible_when",
    "advanced",
    "deprecated",
}


def load_provider_schema(
    path: Path | str,
    *,
    example_path: Path | str | None = None,
    owner_module: str = "",
) -> ConfigProviderDefinition:
    """Load one provider schema from ``config.schema.toml``."""

    schema_path = Path(path)
    try:
        payload = tomllib.loads(schema_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ConfigProviderLoadError(f"Failed to parse provider schema {schema_path}: {exc}") from exc

    provider = _mapping(payload.get("provider"), "provider", schema_path)
    fields = _field_definitions(payload.get("fields", []), schema_path)

    example_toml = ""
    resolved_example_path = Path(example_path) if example_path is not None else None
    if resolved_example_path is not None and resolved_example_path.exists():
        example_toml = resolved_example_path.read_text(encoding="utf-8")

    try:
        provider_kind = ConfigProviderKind(str(provider["kind"]).strip())
        provider_id = str(provider["id"]).strip()
    except KeyError as exc:
        raise ConfigProviderLoadError(
            f"Provider schema {schema_path} missing provider.{exc.args[0]}"
        ) from exc
    except ValueError as exc:
        raise ConfigProviderLoadError(f"Provider schema {schema_path} has invalid kind") from exc

    if not provider_id:
        raise ConfigProviderLoadError(f"Provider schema {schema_path} has empty provider.id")

    metadata = {key: value for key, value in provider.items() if key not in _provider_keys()}
    return ConfigProviderDefinition(
        kind=provider_kind,
        id=provider_id,
        display_name=str(provider.get("display_name") or provider_id),
        description=str(provider.get("description") or ""),
        config_version=str(provider.get("config_version") or "1.0.0"),
        fields=tuple(fields),
        example_toml=example_toml,
        owner_module=owner_module,
        source_path=str(schema_path),
        metadata=metadata,
    )


def load_provider_schema_from_module(
    module: str | ModuleType,
    *,
    schema_name: str = "config.schema.toml",
    example_name: str = "config.example.toml",
) -> ConfigProviderDefinition:
    """Load a provider schema placed next to a Python module/package."""

    module_obj = import_module(module) if isinstance(module, str) else module
    module_file = getattr(module_obj, "__file__", None)
    if not module_file:
        raise ConfigProviderLoadError(f"Module {module_obj!r} has no filesystem path")
    module_dir = Path(module_file).resolve().parent
    schema_path = module_dir / schema_name
    if not schema_path.exists():
        raise ConfigProviderLoadError(f"Provider schema not found: {schema_path}")
    example_path = module_dir / example_name
    return load_provider_schema(
        schema_path,
        example_path=example_path,
        owner_module=getattr(module_obj, "__name__", ""),
    )


def _field_definitions(value: Any, schema_path: Path) -> list[ConfigFieldDefinition]:
    if not isinstance(value, list):
        raise ConfigProviderLoadError(f"Provider schema {schema_path} fields must be a list")
    fields: list[ConfigFieldDefinition] = []
    seen: set[str] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise ConfigProviderLoadError(
                f"Provider schema {schema_path} fields[{index}] must be a mapping"
            )
        path = str(raw.get("path") or "").strip()
        if not path:
            raise ConfigProviderLoadError(
                f"Provider schema {schema_path} fields[{index}] missing path"
            )
        if path in seen:
            raise ConfigProviderLoadError(
                f"Provider schema {schema_path} has duplicate field path {path!r}"
            )
        seen.add(path)
        try:
            field_type = ConfigFieldType(str(raw["type"]).strip())
        except KeyError as exc:
            raise ConfigProviderLoadError(
                f"Provider schema {schema_path} field {path!r} missing type"
            ) from exc
        except ValueError as exc:
            raise ConfigProviderLoadError(
                f"Provider schema {schema_path} field {path!r} has invalid type"
            ) from exc

        fields.append(
            ConfigFieldDefinition(
                path=path,
                type=field_type,
                required=bool(raw.get("required", False)),
                default=raw.get("default"),
                has_default="default" in raw,
                choices=tuple(_list(raw.get("choices"))),
                min=_number_or_none(raw.get("min")),
                max=_number_or_none(raw.get("max")),
                secret=bool(raw.get("secret", False)),
                env=str(raw.get("env") or ""),
                placeholder=str(raw.get("placeholder") or ""),
                description=str(raw.get("description") or ""),
                visible_when=str(raw.get("visible_when") or ""),
                advanced=bool(raw.get("advanced", False)),
                deprecated=bool(raw.get("deprecated", False)),
                metadata={key: item for key, item in raw.items() if key not in _FIELD_KEYS},
            )
        )
    return fields


def _mapping(value: Any, name: str, schema_path: Path) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigProviderLoadError(f"Provider schema {schema_path} {name} must be a mapping")
    return value


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _number_or_none(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    return None


def _provider_keys() -> set[str]:
    return {
        "kind",
        "id",
        "display_name",
        "description",
        "config_version",
    }
