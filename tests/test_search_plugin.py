from __future__ import annotations

from pathlib import Path

import pytest

import shinbot.builtin_plugins.shinbot_plugin_search as search_plugin
from shinbot.agent.tools import ToolCallRequest, ToolManager, ToolRegistry
from shinbot.core.dispatch.command import CommandRegistry
from shinbot.core.dispatch.event_bus import EventBus
from shinbot.core.plugins.context import Plugin


def _build_plugin(tool_registry: ToolRegistry) -> Plugin:
    return Plugin(
        "shinbot_plugin_search",
        CommandRegistry(),
        EventBus(),
        tool_registry=tool_registry,
    )


def test_resolve_config_path_supports_cli_flag_styles():
    assert search_plugin._resolve_config_path(["--config", "custom.toml"]) == Path("custom.toml")
    assert search_plugin._resolve_config_path(["--config=runtime.toml"]) == Path("runtime.toml")
    assert search_plugin._resolve_config_path([]) == Path("config.toml")


def test_load_plugin_config_reads_plugin_configs_block(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[plugin_configs.shinbot_plugin_search]
tavily_api_key = "secret-key"
default_max_results = 7
timeout_seconds = 22.5
""".strip(),
        encoding="utf-8",
    )

    config = search_plugin._load_plugin_config(
        "shinbot_plugin_search",
        config_path=config_path,
    )

    assert config.tavily_api_key == "secret-key"
    assert config.default_max_results == 7
    assert config.timeout_seconds == 22.5


def test_setup_registers_tavily_search_tool():
    registry = ToolRegistry()
    plugin = _build_plugin(registry)

    search_plugin.setup(plugin)

    definition = registry.get_tool_by_name("tavily_search")
    assert definition is not None
    assert definition.owner_id == "shinbot_plugin_search"
    assert definition.metadata.get("provider") == "tavily"
    assert "attention" in definition.tags


@pytest.mark.asyncio
async def test_tavily_search_uses_bearer_authorization_header(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    class _FakeResponse:
        text = ""
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"results": []}

    class _FakeAsyncClient:
        def __init__(self, *, timeout: float):
            captured["timeout"] = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, url: str, *, headers=None, json=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            return _FakeResponse()

    monkeypatch.setattr(search_plugin.httpx, "AsyncClient", _FakeAsyncClient)

    await search_plugin._tavily_search(
        api_key="token-123",
        query="Leo Messi",
        max_results=1,
        timeout_seconds=12.0,
        search_depth="basic",
        include_answer=False,
        include_raw_content=False,
    )

    assert captured["url"] == "https://api.tavily.com/search"
    headers = captured["headers"]
    assert isinstance(headers, dict)
    assert headers.get("Authorization") == "Bearer token-123"
    assert headers.get("Content-Type") == "application/json"
    payload = captured["json"]
    assert isinstance(payload, dict)
    assert "api_key" not in payload


@pytest.mark.asyncio
async def test_tavily_search_tool_executes_with_tavily(monkeypatch: pytest.MonkeyPatch):
    registry = ToolRegistry()
    manager = ToolManager(registry)
    plugin = _build_plugin(registry)

    monkeypatch.setattr(
        search_plugin,
        "_load_plugin_config",
        lambda plugin_id, config_path=None, argv=None: search_plugin.SearchPluginConfig(
            tavily_api_key="test-key",
            default_max_results=4,
            include_answer=True,
        ),
    )

    async def _fake_tavily_search(**kwargs):
        assert kwargs["api_key"] == "test-key"
        assert kwargs["query"] == "ShinBot"
        assert kwargs["max_results"] == 4
        return {
            "answer": "ShinBot summary",
            "response_time": 0.42,
            "results": [
                {
                    "title": "ShinBot Docs",
                    "url": "https://example.com/docs",
                    "content": "ShinBot plugin docs",
                    "score": 0.95,
                }
            ],
        }

    monkeypatch.setattr(search_plugin, "_tavily_search", _fake_tavily_search)

    search_plugin.setup(plugin)
    result = await manager.execute(
        ToolCallRequest(
            tool_name="tavily_search",
            arguments={"query": "ShinBot"},
            caller="attention.workflow_runner",
        )
    )

    assert result.success is True
    assert result.output["provider"] == "tavily"
    assert result.output["query"] == "ShinBot"
    assert result.output["answer"] == "ShinBot summary"
    assert len(result.output["results"]) == 1
    assert result.output["results"][0]["title"] == "ShinBot Docs"


@pytest.mark.asyncio
async def test_tavily_search_tool_requires_api_key(monkeypatch: pytest.MonkeyPatch):
    registry = ToolRegistry()
    manager = ToolManager(registry)
    plugin = _build_plugin(registry)

    monkeypatch.delenv("TAVILY_API_KEY", raising=False)
    monkeypatch.setattr(
        search_plugin,
        "_load_plugin_config",
        lambda plugin_id, config_path=None, argv=None: search_plugin.SearchPluginConfig(),
    )

    search_plugin.setup(plugin)
    result = await manager.execute(
        ToolCallRequest(
            tool_name="tavily_search",
            arguments={"query": "ShinBot"},
            caller="attention.workflow_runner",
        )
    )

    assert result.success is False
    assert result.error_code == "tool_execution_failed"
    assert "Missing Tavily API key" in (result.error_message or "")
