"""Load provider-owned config schemas from TOML files."""

from __future__ import annotations

import tomllib
from copy import deepcopy
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
    i18n_paths: list[Path | str] | None = None,
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
    provider_i18n, fields_i18n = _load_i18n_files(schema_path, i18n_paths)

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

    metadata = _merge_i18n_metadata(
        {key: value for key, value in provider.items() if key not in _provider_keys()},
        provider_i18n,
    )
    return ConfigProviderDefinition(
        kind=provider_kind,
        id=provider_id,
        display_name=str(provider.get("display_name") or provider_id),
        description=str(provider.get("description") or ""),
        config_version=str(provider.get("config_version") or "1.0.0"),
        fields=tuple(_merge_field_i18n(fields, fields_i18n)),
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


def _load_i18n_files(
    schema_path: Path,
    i18n_paths: list[Path | str] | None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, dict[str, Any]]]]:
    provider_i18n: dict[str, dict[str, Any]] = {}
    fields_i18n: dict[str, dict[str, dict[str, Any]]] = {}

    paths = (
        [Path(path) for path in i18n_paths]
        if i18n_paths is not None
        else _discover_i18n_paths(schema_path)
    )
    for path in paths:
        locale = _locale_from_i18n_path(path)
        if not locale:
            raise ConfigProviderLoadError(f"Provider i18n file has unsupported name: {path}")
        try:
            payload = tomllib.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ConfigProviderLoadError(f"Failed to parse provider i18n {path}: {exc}") from exc

        provider_payload = payload.get("provider", {})
        if provider_payload:
            provider_i18n[locale] = _copy_i18n_block(provider_payload, path, "provider")

        for field_path, field_payload in _iter_i18n_fields(payload.get("fields", []), path):
            fields_i18n.setdefault(field_path, {})[locale] = field_payload

    return provider_i18n, fields_i18n


def _discover_i18n_paths(schema_path: Path) -> list[Path]:
    file_paths = list(schema_path.parent.glob("config.i18n.*.toml"))
    dir_paths = []
    i18n_dir = schema_path.parent / "i18n"
    if i18n_dir.is_dir():
        dir_paths = list(i18n_dir.glob("*.toml"))
    return sorted((*file_paths, *dir_paths), key=lambda path: str(path))


def _locale_from_i18n_path(path: Path) -> str:
    name = path.name
    prefix = "config.i18n."
    suffix = ".toml"
    if name.startswith(prefix) and name.endswith(suffix):
        return name[len(prefix) : -len(suffix)].strip()
    if path.parent.name == "i18n" and name.endswith(suffix):
        return path.stem.strip()
    return ""


def _iter_i18n_fields(value: Any, path: Path) -> list[tuple[str, dict[str, Any]]]:
    if value in ({}, [], None):
        return []

    result: list[tuple[str, dict[str, Any]]] = []
    if isinstance(value, list):
        for index, raw in enumerate(value):
            if not isinstance(raw, dict):
                raise ConfigProviderLoadError(
                    f"Provider i18n {path} fields[{index}] must be a mapping"
                )
            field_path = str(raw.get("path") or "").strip()
            if not field_path:
                raise ConfigProviderLoadError(
                    f"Provider i18n {path} fields[{index}] missing path"
                )
            result.append(
                (field_path, {key: deepcopy(item) for key, item in raw.items() if key != "path"})
            )
        return result

    if isinstance(value, dict):
        for field_path, raw in value.items():
            if not isinstance(raw, dict):
                raise ConfigProviderLoadError(
                    f"Provider i18n {path} field {field_path!r} must be a mapping"
                )
            result.append((str(field_path), deepcopy(raw)))
        return result

    raise ConfigProviderLoadError(f"Provider i18n {path} fields must be a list or mapping")


def _copy_i18n_block(value: Any, path: Path, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigProviderLoadError(f"Provider i18n {path} {name} must be a mapping")
    return deepcopy(value)


def _merge_i18n_metadata(
    metadata: dict[str, Any],
    i18n_blocks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not i18n_blocks:
        return metadata
    result = deepcopy(metadata)
    existing_i18n = result.get("i18n", {})
    i18n = deepcopy(existing_i18n) if isinstance(existing_i18n, dict) else {}
    for locale, block in i18n_blocks.items():
        current = i18n.get(locale, {})
        merged = deepcopy(current) if isinstance(current, dict) else {}
        merged.update(deepcopy(block))
        i18n[locale] = merged
    result["i18n"] = i18n
    return result


def _merge_field_i18n(
    fields: list[ConfigFieldDefinition],
    fields_i18n: dict[str, dict[str, dict[str, Any]]],
) -> list[ConfigFieldDefinition]:
    if not fields_i18n:
        return fields

    return [
        ConfigFieldDefinition(
            path=field.path,
            type=field.type,
            required=field.required,
            default=field.default,
            has_default=field.has_default,
            choices=field.choices,
            min=field.min,
            max=field.max,
            secret=field.secret,
            env=field.env,
            placeholder=field.placeholder,
            description=field.description,
            visible_when=field.visible_when,
            advanced=field.advanced,
            deprecated=field.deprecated,
            metadata=_merge_i18n_metadata(field.metadata, fields_i18n.get(field.path, {})),
        )
        for field in fields
    ]


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
