"""Build model request tool schemas from registered tool definitions."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from .registry import ToolRegistry
from .schema import ToolDefinition, ToolVisibility

ToolPermissionChecker = Callable[[ToolDefinition, str, str, str], bool]


class ToolSchemaBuilder:
    """Projects visible tool definitions into Chat Completions tool schemas."""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        permission_checker: ToolPermissionChecker | None = None,
    ) -> None:
        self._registry = registry
        self._permission_checker = permission_checker
        self._schema_cache: dict[tuple[Any, ...], dict[str, Any]] = {}

    def list_visible_tools(
        self,
        *,
        caller: str,
        instance_id: str = "",
        session_id: str = "",
        user_id: str = "",
        include_private: bool = False,
        tags: set[str] | None = None,
    ) -> list[ToolDefinition]:
        definitions = self._registry.list_tools(enabled=True, tags=tags)
        visible: list[ToolDefinition] = []
        for definition in definitions:
            if definition.visibility == ToolVisibility.PRIVATE and not include_private:
                continue
            if self._permission_checker is not None and not self._permission_checker(
                definition,
                instance_id,
                session_id,
                user_id,
            ):
                continue
            visible.append(definition)
        return visible

    def export_model_tools(
        self,
        *,
        caller: str,
        instance_id: str = "",
        session_id: str = "",
        user_id: str = "",
        tags: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            self._schema_for_definition(definition)
            for definition in self.list_visible_tools(
                caller=caller,
                instance_id=instance_id,
                session_id=session_id,
                user_id=user_id,
                tags=tags,
            )
            if definition.visibility != ToolVisibility.PRIVATE
        ]

    def build_request_tools(
        self,
        tool_names: list[str],
        *,
        caller: str = "",
        instance_id: str = "",
        session_id: str = "",
        user_id: str = "",
        tags: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Build Chat Completions tool schemas for specific tool names."""
        requested_names = [name for name in tool_names if name]
        schema_by_name = {
            str(schema.get("function", {}).get("name") or ""): schema
            for schema in self.export_model_tools(
                caller=caller,
                instance_id=instance_id,
                session_id=session_id,
                user_id=user_id,
                tags=tags,
            )
        }
        return [
            schema_by_name[name]
            for name in requested_names
            if name in schema_by_name
        ]

    def _schema_for_definition(self, definition: ToolDefinition) -> dict[str, Any]:
        cache_key = _definition_cache_key(definition)
        cached = self._schema_cache.get(cache_key)
        if cached is None:
            cached = {
                "type": "function",
                "function": {
                    "name": definition.name,
                    "description": definition.description,
                    "parameters": definition.input_schema,
                },
            }
            self._schema_cache[cache_key] = cached
        return {
            "type": str(cached["type"]),
            "function": dict(cached["function"]),
        }


def _definition_cache_key(definition: ToolDefinition) -> tuple[Any, ...]:
    return (
        definition.id,
        definition.name,
        definition.description,
        json.dumps(definition.input_schema, ensure_ascii=False, sort_keys=True),
        definition.visibility,
        definition.enabled,
    )


__all__ = ["ToolPermissionChecker", "ToolSchemaBuilder"]
