"""Schema types for provider-owned configuration definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class ConfigProviderKind(StrEnum):
    """Known provider categories."""

    ADAPTER = "adapter"
    PLUGIN = "plugin"
    AGENT = "agent"
    MODEL_PROVIDER = "model_provider"
    TOOL_PROVIDER = "tool_provider"
    MEMORY_PROVIDER = "memory_provider"


class ConfigFieldType(StrEnum):
    """Primitive field types supported by config provider schemas."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    ENUM = "enum"
    STRING_LIST = "string_list"
    INTEGER_LIST = "integer_list"
    OBJECT = "object"
    ARRAY_OBJECT = "array_object"
    PATH = "path"
    DURATION = "duration"


@dataclass(slots=True, frozen=True)
class ConfigFieldDefinition:
    """One configurable field exposed by a provider."""

    path: str
    type: ConfigFieldType
    required: bool = False
    default: Any = None
    has_default: bool = False
    choices: tuple[Any, ...] = ()
    min: int | float | None = None
    max: int | float | None = None
    secret: bool = False
    env: str = ""
    placeholder: str = ""
    description: str = ""
    visible_when: str = ""
    advanced: bool = False
    deprecated: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        payload: dict[str, Any] = {
            "path": self.path,
            "type": self.type.value,
            "required": self.required,
            "choices": list(self.choices),
            "secret": self.secret,
            "env": self.env,
            "placeholder": self.placeholder,
            "description": self.description,
            "visible_when": self.visible_when,
            "advanced": self.advanced,
            "deprecated": self.deprecated,
            "metadata": dict(self.metadata),
        }
        if self.has_default:
            payload["default"] = self.default
        if self.min is not None:
            payload["min"] = self.min
        if self.max is not None:
            payload["max"] = self.max
        return payload


@dataclass(slots=True, frozen=True)
class ConfigProviderDefinition:
    """Config schema owned by one adapter/plugin/subsystem provider."""

    kind: ConfigProviderKind
    id: str
    display_name: str = ""
    description: str = ""
    config_version: str = "1.0.0"
    fields: tuple[ConfigFieldDefinition, ...] = ()
    example_toml: str = ""
    owner_module: str = ""
    source_path: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def key(self) -> tuple[ConfigProviderKind, str]:
        return self.kind, self.id

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "kind": self.kind.value,
            "id": self.id,
            "display_name": self.display_name,
            "description": self.description,
            "config_version": self.config_version,
            "fields": [field_def.to_dict() for field_def in self.fields],
            "example_toml": self.example_toml,
            "owner_module": self.owner_module,
            "source_path": self.source_path,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True, frozen=True)
class ConfigValidationIssue:
    """One provider config validation issue."""

    path: str
    message: str
    code: str = "invalid"

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "message": self.message, "code": self.code}
