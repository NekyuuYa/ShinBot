"""In-memory Tool registry."""

from __future__ import annotations

from typing import Any

from .schema import ToolDefinition, ToolVisibility


class ToolRegistry:
    """Central registry for tool definitions."""

    def __init__(self) -> None:
        self._tools_by_id: dict[str, ToolDefinition] = {}
        self._tool_id_by_name: dict[str, str] = {}

    def register_tool(self, definition: ToolDefinition) -> None:
        if not definition.id.strip():
            raise ValueError("Tool id must be a non-empty string")
        if not definition.name.strip():
            raise ValueError("Tool name must be a non-empty string")
        if not definition.owner_id.strip():
            raise ValueError("Tool owner_id must be a non-empty string")
        if definition.id in self._tools_by_id:
            raise ValueError(f"Tool id {definition.id!r} is already registered")
        if definition.name in self._tool_id_by_name:
            raise ValueError(f"Tool name {definition.name!r} is already registered")
        self._tools_by_id[definition.id] = definition
        self._tool_id_by_name[definition.name] = definition.id

    def unregister_tool(self, tool_id: str) -> None:
        definition = self._tools_by_id.pop(tool_id, None)
        if definition is not None:
            self._tool_id_by_name.pop(definition.name, None)

    def unregister_owner(self, owner_type: str, owner_id: str) -> int:
        removed = [
            tool_id
            for tool_id, definition in self._tools_by_id.items()
            if definition.owner_type == owner_type and definition.owner_id == owner_id
        ]
        for tool_id in removed:
            self.unregister_tool(tool_id)
        return len(removed)

    def get_tool(self, tool_id: str) -> ToolDefinition | None:
        return self._tools_by_id.get(tool_id)

    def get_tool_by_name(self, name: str) -> ToolDefinition | None:
        tool_id = self._tool_id_by_name.get(name)
        if tool_id is None:
            return None
        return self._tools_by_id.get(tool_id)

    def list_owner_tools(self, owner_type: str, owner_id: str) -> list[ToolDefinition]:
        return [
            definition
            for definition in self._tools_by_id.values()
            if definition.owner_type == owner_type and definition.owner_id == owner_id
        ]

    def list_tools(
        self,
        *,
        enabled: bool | None = None,
        visibility: ToolVisibility | None = None,
        owner_type: str | None = None,
        owner_id: str | None = None,
        tags: set[str] | None = None,
    ) -> list[ToolDefinition]:
        definitions = list(self._tools_by_id.values())
        if enabled is not None:
            definitions = [tool for tool in definitions if tool.enabled is enabled]
        if visibility is not None:
            definitions = [tool for tool in definitions if tool.visibility == visibility]
        if owner_type is not None:
            definitions = [tool for tool in definitions if tool.owner_type == owner_type]
        if owner_id is not None:
            definitions = [tool for tool in definitions if tool.owner_id == owner_id]
        if tags:
            definitions = [tool for tool in definitions if tags.issubset(set(tool.tags))]
        return sorted(definitions, key=lambda item: item.name)

    def export_model_tools(self, **filters: Any) -> list[dict[str, Any]]:
        definitions = self.list_tools(**filters)
        return [
            {
                "type": "function",
                "function": {
                    "name": definition.name,
                    "description": definition.description,
                    "parameters": definition.input_schema,
                },
            }
            for definition in definitions
            if definition.visibility != ToolVisibility.PRIVATE
        ]
