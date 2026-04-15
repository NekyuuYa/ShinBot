from __future__ import annotations

import pytest

from shinbot.agent.tools import (
    ToolCallRequest,
    ToolDefinition,
    ToolManager,
    ToolOwnerType,
    ToolRegistry,
    ToolVisibility,
)
from shinbot.core.security.permission import PermissionEngine


def _tool_definition(
    *,
    tool_id: str = "builtin.weather_query",
    name: str = "weather_query",
    permission: str = "tools.weather.query",
    enabled: bool = True,
    visibility: ToolVisibility = ToolVisibility.SCOPED,
    handler=None,
):
    async def default_handler(arguments, runtime):
        return {"city": arguments["city"], "caller": runtime.caller}

    return ToolDefinition(
        id=tool_id,
        name=name,
        display_name=name,
        description="query weather",
        input_schema={
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
        handler=handler or default_handler,
        owner_type=ToolOwnerType.BUILTIN_MODULE,
        owner_id="builtin.weather",
        permission=permission,
        enabled=enabled,
        visibility=visibility,
    )


class TestToolRegistry:
    def test_register_and_lookup_tool(self):
        registry = ToolRegistry()
        definition = _tool_definition()
        registry.register_tool(definition)

        assert registry.get_tool(definition.id) is definition
        assert registry.get_tool_by_name(definition.name) is definition

    def test_duplicate_tool_name_is_rejected(self):
        registry = ToolRegistry()
        registry.register_tool(_tool_definition())

        with pytest.raises(ValueError, match="already registered"):
            registry.register_tool(
                _tool_definition(tool_id="builtin.weather_query_v2", name="weather_query")
            )

    def test_unregister_owner_removes_all_owner_tools(self):
        registry = ToolRegistry()
        registry.register_tool(_tool_definition(tool_id="plugin.demo.one", name="one"))
        registry.register_tool(_tool_definition(tool_id="plugin.demo.two", name="two"))

        removed = registry.unregister_owner(ToolOwnerType.BUILTIN_MODULE, "builtin.weather")

        assert removed == 2
        assert registry.list_tools() == []


class TestToolManager:
    def setup_method(self):
        self.registry = ToolRegistry()
        self.permissions = PermissionEngine()
        self.manager = ToolManager(self.registry, permission_engine=self.permissions)
        self.registry.register_tool(_tool_definition())

    def test_export_model_tools_filters_by_permission(self):
        tools = self.manager.export_model_tools(
            caller="agent.runtime",
            instance_id="inst1",
            session_id="inst1:group:g1",
            user_id="user1",
        )
        assert tools == []

        self.permissions.bind("inst1:user1", "admin")
        tools = self.manager.export_model_tools(
            caller="agent.runtime",
            instance_id="inst1",
            session_id="inst1:group:g1",
            user_id="user1",
        )
        assert [item["function"]["name"] for item in tools] == ["weather_query"]

    @pytest.mark.asyncio
    async def test_execute_tool_success(self):
        self.permissions.bind("inst1:user1", "admin")

        result = await self.manager.execute(
            ToolCallRequest(
                tool_name="weather_query",
                arguments={"city": "Shanghai"},
                caller="agent.runtime",
                instance_id="inst1",
                session_id="inst1:group:g1",
                user_id="user1",
            )
        )

        assert result.success is True
        assert result.output["city"] == "Shanghai"

    @pytest.mark.asyncio
    async def test_execute_tool_rejects_invalid_arguments(self):
        self.permissions.bind("inst1:user1", "admin")

        result = await self.manager.execute(
            ToolCallRequest(
                tool_name="weather_query",
                arguments={},
                caller="agent.runtime",
                instance_id="inst1",
                session_id="inst1:group:g1",
                user_id="user1",
            )
        )

        assert result.success is False
        assert result.error_code == "invalid_arguments"

    @pytest.mark.asyncio
    async def test_execute_tool_rejects_permission_denied(self):
        result = await self.manager.execute(
            ToolCallRequest(
                tool_name="weather_query",
                arguments={"city": "Shanghai"},
                caller="agent.runtime",
                instance_id="inst1",
                session_id="inst1:group:g1",
                user_id="user1",
            )
        )

        assert result.success is False
        assert result.error_code == "permission_denied"
