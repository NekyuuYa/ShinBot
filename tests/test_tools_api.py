from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.agent.tools import ToolDefinition, ToolOwnerType, ToolRiskLevel, ToolVisibility
from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot


class _BootStub:
    def __init__(self, data_dir: Path) -> None:
        self.config = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            }
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None

    def save_config(self) -> bool:
        return True


async def _tool_handler(arguments, runtime):
    return {"arguments": arguments, "caller": runtime.caller}


def test_tools_list_route_returns_registered_tools(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.tool_registry.register_tool(
        ToolDefinition(
            id="builtin.weather.query",
            name="weather_query",
            display_name="Weather Query",
            description="Query weather information",
            input_schema={
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
            handler=_tool_handler,
            owner_type=ToolOwnerType.BUILTIN_MODULE,
            owner_id="builtin.weather",
            owner_module="shinbot.builtin.weather",
            permission="tools.weather.query",
            enabled=True,
            visibility=ToolVisibility.PUBLIC,
            timeout_seconds=12.5,
            risk_level=ToolRiskLevel.MEDIUM,
            tags=["weather", "network"],
            metadata={"category": "utility"},
        )
    )
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    with TestClient(app) as client:
        response = client.get("/api/v1/tools", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    weather_tool = next(item for item in payload if item["id"] == "builtin.weather.query")
    assert weather_tool == {
        "id": "builtin.weather.query",
        "name": "weather_query",
        "displayName": "Weather Query",
        "description": "Query weather information",
        "inputSchema": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
        "outputSchema": None,
        "ownerType": "builtin_module",
        "ownerId": "builtin.weather",
        "ownerModule": "shinbot.builtin.weather",
        "permission": "tools.weather.query",
        "enabled": True,
        "visibility": "public",
        "timeoutSeconds": 12.5,
        "riskLevel": "medium",
        "tags": ["weather", "network"],
        "metadata": {"category": "utility"},
    }
