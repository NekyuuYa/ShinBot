"""Registry and validation helpers for config providers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from shinbot.core.config_provider.loader import load_provider_schema_from_module
from shinbot.core.config_provider.schema import (
    ConfigFieldDefinition,
    ConfigFieldType,
    ConfigProviderDefinition,
    ConfigProviderKind,
    ConfigValidationIssue,
)

_MISSING = object()


class ConfigProviderRegistry:
    """In-memory registry for adapter/plugin/agent config schemas."""

    def __init__(self) -> None:
        self._providers: dict[tuple[ConfigProviderKind, str], ConfigProviderDefinition] = {}

    def register(self, provider: ConfigProviderDefinition) -> None:
        key = provider.key()
        if key in self._providers:
            raise ValueError(f"Config provider {provider.kind.value}:{provider.id} is registered")
        self._providers[key] = provider

    def upsert(self, provider: ConfigProviderDefinition) -> None:
        self._providers[provider.key()] = provider

    def load_from_module(self, module: str) -> ConfigProviderDefinition:
        provider = load_provider_schema_from_module(module)
        self.register(provider)
        return provider

    def get(
        self,
        kind: ConfigProviderKind | str,
        provider_id: str,
    ) -> ConfigProviderDefinition | None:
        return self._providers.get((_coerce_kind(kind), provider_id))

    def list(
        self,
        kind: ConfigProviderKind | str | None = None,
    ) -> list[ConfigProviderDefinition]:
        providers = list(self._providers.values())
        if kind is not None:
            coerced = _coerce_kind(kind)
            providers = [provider for provider in providers if provider.kind == coerced]
        return sorted(providers, key=lambda item: (item.kind.value, item.id))

    def catalog(self, kind: ConfigProviderKind | str | None = None) -> list[dict[str, Any]]:
        return [provider.to_dict() for provider in self.list(kind)]

    def default_config(
        self,
        kind: ConfigProviderKind | str,
        provider_id: str,
    ) -> dict[str, Any]:
        provider = self._require(kind, provider_id)
        result: dict[str, Any] = {}
        for field in provider.fields:
            if field.has_default:
                _set_path(result, field.path, deepcopy(field.default))
        return result

    def validate(
        self,
        kind: ConfigProviderKind | str,
        provider_id: str,
        config: dict[str, Any] | None,
        *,
        path_prefix: str = "",
        strict: bool = False,
    ) -> list[ConfigValidationIssue]:
        provider = self._require(kind, provider_id)
        payload = config or {}
        issues: list[ConfigValidationIssue] = []
        if strict:
            issues.extend(_validate_unknown_fields(provider.fields, payload, path_prefix))
        for field in provider.fields:
            value = _get_path(payload, field.path)
            issue_path = f"{path_prefix}.{field.path}" if path_prefix else field.path
            if value is _MISSING:
                if field.required and not field.has_default:
                    issues.append(
                        ConfigValidationIssue(
                            path=issue_path,
                            message="field is required",
                            code="required",
                        )
                    )
                continue
            issues.extend(_validate_field(field, value, issue_path))
        return issues

    def _require(
        self,
        kind: ConfigProviderKind | str,
        provider_id: str,
    ) -> ConfigProviderDefinition:
        provider = self.get(kind, provider_id)
        if provider is None:
            coerced = _coerce_kind(kind)
            raise KeyError(f"Config provider {coerced.value}:{provider_id} is not registered")
        return provider


def _validate_field(
    field: ConfigFieldDefinition,
    value: Any,
    path: str,
) -> list[ConfigValidationIssue]:
    issues: list[ConfigValidationIssue] = []
    if not _matches_type(field.type, value):
        issues.append(
            ConfigValidationIssue(
                path=path,
                message=f"expected {field.type.value}",
                code="type",
            )
        )
        return issues

    if field.choices and value not in field.choices:
        issues.append(
            ConfigValidationIssue(
                path=path,
                message=f"expected one of: {', '.join(str(item) for item in field.choices)}",
                code="choices",
            )
        )

    if isinstance(value, int | float) and not isinstance(value, bool):
        if field.min is not None and value < field.min:
            issues.append(
                ConfigValidationIssue(
                    path=path,
                    message=f"must be >= {field.min}",
                    code="min",
                )
            )
        if field.max is not None and value > field.max:
            issues.append(
                ConfigValidationIssue(
                    path=path,
                    message=f"must be <= {field.max}",
                    code="max",
                )
            )
    return issues


def _validate_unknown_fields(
    fields: tuple[ConfigFieldDefinition, ...],
    payload: dict[str, Any],
    path_prefix: str,
) -> list[ConfigValidationIssue]:
    allowed_paths = {tuple(field.path.split(".")): field for field in fields}
    allowed_prefixes: set[tuple[str, ...]] = set()
    for path in allowed_paths:
        for index in range(1, len(path)):
            allowed_prefixes.add(path[:index])
    opaque_paths = {
        path
        for path, field in allowed_paths.items()
        if field.type in (ConfigFieldType.OBJECT, ConfigFieldType.ARRAY_OBJECT)
        and path not in allowed_prefixes
    }

    issues: list[ConfigValidationIssue] = []

    def walk(value: Any, path: tuple[str, ...]) -> None:
        if path in opaque_paths:
            return
        if not isinstance(value, dict):
            return

        for key, child_value in value.items():
            child_path = (*path, str(key))
            if child_path not in allowed_paths and child_path not in allowed_prefixes:
                issue_path = ".".join(child_path)
                issues.append(
                    ConfigValidationIssue(
                        path=f"{path_prefix}.{issue_path}" if path_prefix else issue_path,
                        message="unknown field",
                        code="unknown",
                    )
                )
                continue
            if child_path in allowed_paths and child_path not in allowed_prefixes:
                continue
            walk(child_value, child_path)

    walk(payload, ())
    return issues


def _matches_type(field_type: ConfigFieldType, value: Any) -> bool:
    if field_type in (ConfigFieldType.STRING, ConfigFieldType.ENUM, ConfigFieldType.PATH):
        return isinstance(value, str)
    if field_type == ConfigFieldType.INTEGER:
        return isinstance(value, int) and not isinstance(value, bool)
    if field_type in (ConfigFieldType.FLOAT, ConfigFieldType.DURATION):
        return isinstance(value, int | float) and not isinstance(value, bool)
    if field_type == ConfigFieldType.BOOLEAN:
        return isinstance(value, bool)
    if field_type == ConfigFieldType.STRING_LIST:
        return isinstance(value, list) and all(isinstance(item, str) for item in value)
    if field_type == ConfigFieldType.INTEGER_LIST:
        return isinstance(value, list) and all(
            isinstance(item, int) and not isinstance(item, bool) for item in value
        )
    if field_type == ConfigFieldType.OBJECT:
        return isinstance(value, dict)
    if field_type == ConfigFieldType.ARRAY_OBJECT:
        return isinstance(value, list) and all(isinstance(item, dict) for item in value)
    return False


def _get_path(payload: dict[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return _MISSING
        current = current[part]
    return current


def _set_path(payload: dict[str, Any], path: str, value: Any) -> None:
    current = payload
    parts = path.split(".")
    for part in parts[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            current[part] = next_value
        current = next_value
    current[parts[-1]] = value


def _coerce_kind(kind: ConfigProviderKind | str) -> ConfigProviderKind:
    if isinstance(kind, ConfigProviderKind):
        return kind
    return ConfigProviderKind(str(kind))
