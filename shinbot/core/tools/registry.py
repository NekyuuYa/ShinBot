"""In-memory tool registry."""

from __future__ import annotations

from typing import Any

from shinbot.core.tools.schema import ToolDefinition, ToolVisibility


class ToolRegistry:
    """Central registry for tool definitions."""

    def __init__(self) -> None:
        """Initialize an empty tool registry."""
        self._tools_by_id: dict[str, ToolDefinition] = {}
        self._tool_id_by_name: dict[str, str] = {}
        self._revision = 0

    @property
    def revision(self) -> int:
        """Monotonic revision for consumers that cache registry projections."""
        return self._revision

    def touch(self) -> int:
        """Mark registry contents as changed after in-place tool edits."""
        self._revision += 1
        return self._revision

    def register_tool(self, definition: ToolDefinition) -> None:
        """Register a new tool definition.

        Args:
            definition: The tool definition to register.

        Raises:
            ValueError: If the tool id or name is already registered,
                or if the definition fails validation.
        """
        self._validate_definition(definition)
        if definition.id in self._tools_by_id:
            raise ValueError(f"Tool id {definition.id!r} is already registered")
        if definition.name in self._tool_id_by_name:
            raise ValueError(f"Tool name {definition.name!r} is already registered")
        self._tools_by_id[definition.id] = definition
        self._tool_id_by_name[definition.name] = definition.id
        self.touch()

    def replace_tool(self, definition: ToolDefinition) -> None:
        """Replace an existing tool definition and update name indexes."""
        self._validate_definition(definition)
        current = self._tools_by_id.get(definition.id)
        if current is None:
            raise ValueError(f"Tool id {definition.id!r} is not registered")
        existing_name_owner = self._tool_id_by_name.get(definition.name)
        if existing_name_owner is not None and existing_name_owner != definition.id:
            raise ValueError(f"Tool name {definition.name!r} is already registered")
        if current.name != definition.name:
            self._tool_id_by_name.pop(current.name, None)
        self._tools_by_id[definition.id] = definition
        self._tool_id_by_name[definition.name] = definition.id
        self.touch()

    def unregister_tool(self, tool_id: str) -> None:
        """Remove a tool by its id.

        Args:
            tool_id: The unique identifier of the tool to remove.
        """
        definition = self._tools_by_id.pop(tool_id, None)
        if definition is not None:
            self._tool_id_by_name.pop(definition.name, None)
            self.touch()

    def unregister_owner(self, owner_type: str, owner_id: str) -> int:
        """Remove all tools belonging to a specific owner.

        Args:
            owner_type: The owner type to match (e.g. 'plugin', 'builtin_module').
            owner_id: The owner id to match.

        Returns:
            The number of tools removed.
        """
        removed = [
            tool_id
            for tool_id, definition in self._tools_by_id.items()
            if definition.owner_type == owner_type and definition.owner_id == owner_id
        ]
        for tool_id in removed:
            self.unregister_tool(tool_id)
        return len(removed)

    def get_tool(self, tool_id: str) -> ToolDefinition | None:
        """Look up a tool by its unique id.

        Args:
            tool_id: The unique identifier of the tool.

        Returns:
            The tool definition if found, otherwise None.
        """
        return self._tools_by_id.get(tool_id)

    def get_tool_by_name(self, name: str) -> ToolDefinition | None:
        """Look up a tool by its human-readable name.

        Args:
            name: The tool name to search for.

        Returns:
            The tool definition if found, otherwise None.
        """
        tool_id = self._tool_id_by_name.get(name)
        if tool_id is None:
            return None
        return self._tools_by_id.get(tool_id)

    def list_owner_tools(self, owner_type: str, owner_id: str) -> list[ToolDefinition]:
        """List all tools owned by a specific owner.

        Args:
            owner_type: The owner type to filter by.
            owner_id: The owner id to filter by.

        Returns:
            A list of tool definitions belonging to the specified owner.
        """
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
        """List registered tools with optional filtering.

        Args:
            enabled: Filter by enabled/disabled state. None returns all.
            visibility: Filter by visibility level. None returns all.
            owner_type: Filter by owner type. None returns all.
            owner_id: Filter by owner id. None returns all.
            tags: Filter to tools containing all of these tags.

        Returns:
            A sorted list of matching tool definitions.
        """
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
        """Export tools in the format expected by LLM function-calling APIs.

        Filters tools using the same arguments as ``list_tools``, then
        converts each non-private tool into an OpenAI-compatible tool dict.

        Args:
            **filters: Keyword arguments forwarded to ``list_tools``
                (e.g. ``enabled=True``, ``visibility=ToolVisibility.PUBLIC``).

        Returns:
            A list of dicts with ``type`` and ``function`` keys suitable for
            inclusion in a model API tools payload.
        """
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

    def _validate_definition(self, definition: ToolDefinition) -> None:
        if not definition.id.strip():
            raise ValueError("Tool id must be a non-empty string")
        if not definition.name.strip():
            raise ValueError("Tool name must be a non-empty string")
        if not definition.owner_id.strip():
            raise ValueError("Tool owner_id must be a non-empty string")
