from __future__ import annotations

from dataclasses import replace

import pytest

from shinbot.agent.services.tools import (
    ToolCallRequest,
    ToolDefinition,
    ToolManager,
    ToolOwnerType,
    ToolRegistry,
    ToolSchemaBuilder,
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
    tags: list[str] | None = None,
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
        tags=tags or [],
    )


class TestToolRegistry:
    def test_register_and_lookup_tool(self):
        registry = ToolRegistry()
        definition = _tool_definition()
        before_revision = registry.revision
        registry.register_tool(definition)

        assert registry.get_tool(definition.id) is definition
        assert registry.get_tool_by_name(definition.name) is definition
        assert registry.revision == before_revision + 1

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
        assert registry.revision == 4

    def test_replace_tool_updates_name_index_and_revision(self):
        registry = ToolRegistry()
        definition = _tool_definition()
        registry.register_tool(definition)

        registry.replace_tool(
            replace(
                definition,
                name="weather_query_v2",
                description="new description",
            )
        )

        assert registry.revision == 2
        assert registry.get_tool_by_name("weather_query") is None
        assert registry.get_tool_by_name("weather_query_v2").description == "new description"


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

        self.permissions.bind("inst1:user1", "owner")
        tools = self.manager.export_model_tools(
            caller="agent.runtime",
            instance_id="inst1",
            session_id="inst1:group:g1",
            user_id="user1",
        )
        assert [item["function"]["name"] for item in tools] == ["weather_query"]

    def test_build_request_tools_filters_by_name_tags_and_preserves_requested_order(self):
        self.registry.register_tool(
            _tool_definition(
                tool_id="builtin.no_reply",
                name="no_reply",
                permission="tools.chat.no_reply",
                tags=["chat_action"],
            )
        )
        self.registry.register_tool(
            _tool_definition(
                tool_id="builtin.send_reply",
                name="send_reply",
                permission="tools.chat.send_reply",
                tags=["chat_action"],
            )
        )
        self.permissions.bind("inst1:user1", "owner")

        tools = self.manager.build_request_tools(
            ["send_reply", "missing", "no_reply"],
            caller="agent.runtime",
            instance_id="inst1",
            session_id="inst1:group:g1",
            user_id="user1",
            tags={"chat_action"},
        )

        assert [item["function"]["name"] for item in tools] == ["send_reply", "no_reply"]

    @pytest.mark.asyncio
    async def test_execute_tool_success(self):
        self.permissions.bind("inst1:user1", "owner")

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
        self.permissions.bind("inst1:user1", "owner")

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

    def test_invalidate_tool_schema_cache_after_in_place_tool_update(self):
        self.permissions.bind("inst1:user1", "owner")

        first = self.manager.build_request_tools(
            ["weather_query"],
            caller="agent.runtime",
            instance_id="inst1",
            session_id="inst1:group:g1",
            user_id="user1",
        )
        definition = self.registry.get_tool("builtin.weather_query")
        assert definition is not None
        definition.description = "updated weather query"

        self.manager.invalidate_tool_schema_cache("builtin.weather_query")
        second = self.manager.build_request_tools(
            ["weather_query"],
            caller="agent.runtime",
            instance_id="inst1",
            session_id="inst1:group:g1",
            user_id="user1",
        )

        assert first[0]["function"]["description"] == "query weather"
        assert second[0]["function"]["description"] == "updated weather query"


class TestToolSchemaBuilder:
    def test_build_request_tools_filters_and_preserves_requested_order(self):
        registry = ToolRegistry()
        registry.register_tool(
            _tool_definition(
                tool_id="builtin.no_reply",
                name="no_reply",
                permission="",
                tags=["chat_action"],
            )
        )
        registry.register_tool(
            _tool_definition(
                tool_id="builtin.send_reply",
                name="send_reply",
                permission="",
                tags=["chat_action"],
            )
        )
        builder = ToolSchemaBuilder(registry)

        tools = builder.build_request_tools(
            ["send_reply", "missing", "no_reply"],
            caller="agent.review",
            tags={"chat_action"},
        )

        assert [item["function"]["name"] for item in tools] == ["send_reply", "no_reply"]

    def test_export_model_tools_hides_private_and_uses_permission_checker(self):
        registry = ToolRegistry()
        registry.register_tool(
            _tool_definition(
                tool_id="builtin.public",
                name="public_tool",
                permission="tools.public",
                visibility=ToolVisibility.PUBLIC,
            )
        )
        registry.register_tool(
            _tool_definition(
                tool_id="builtin.private",
                name="private_tool",
                permission="",
                visibility=ToolVisibility.PRIVATE,
            )
        )
        allowed = False

        def permission_checker(*_args):
            return allowed

        builder = ToolSchemaBuilder(registry, permission_checker=permission_checker)

        assert builder.export_model_tools(caller="agent.review") == []
        allowed = True
        tools = builder.export_model_tools(caller="agent.review")
        assert [item["function"]["name"] for item in tools] == ["public_tool"]

    def test_schema_cache_refreshes_when_registry_revision_changes(self):
        registry = ToolRegistry()
        registry.register_tool(
            _tool_definition(
                tool_id="builtin.one",
                name="one",
                permission="",
                visibility=ToolVisibility.PUBLIC,
            )
        )
        builder = ToolSchemaBuilder(registry)
        assert [item["function"]["name"] for item in builder.export_model_tools(caller="x")] == [
            "one"
        ]

        registry.register_tool(
            _tool_definition(
                tool_id="builtin.two",
                name="two",
                permission="",
                visibility=ToolVisibility.PUBLIC,
            )
        )

        assert [item["function"]["name"] for item in builder.export_model_tools(caller="x")] == [
            "one",
            "two",
        ]

    def test_schema_cache_refreshes_when_tool_is_replaced(self):
        registry = ToolRegistry()
        definition = _tool_definition(
            tool_id="builtin.one",
            name="one",
            permission="",
            visibility=ToolVisibility.PUBLIC,
        )
        registry.register_tool(definition)
        builder = ToolSchemaBuilder(registry)

        first = builder.export_model_tools(caller="x")
        registry.replace_tool(replace(definition, description="updated one"))
        second = builder.export_model_tools(caller="x")

        assert first[0]["function"]["description"] == "query weather"
        assert second[0]["function"]["description"] == "updated one"

    def test_exported_schema_is_not_mutated_by_later_definition_changes(self):
        registry = ToolRegistry()
        definition = _tool_definition(
            tool_id="builtin.one",
            name="one",
            permission="",
            visibility=ToolVisibility.PUBLIC,
        )
        registry.register_tool(definition)
        builder = ToolSchemaBuilder(registry)

        schema = builder.export_model_tools(caller="x")[0]
        definition.input_schema["properties"]["city"]["description"] = "changed"

        assert "description" not in schema["function"]["parameters"]["properties"]["city"]
