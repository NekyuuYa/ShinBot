from __future__ import annotations

import sys
import types
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import BaseModel

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
        self.save_config_calls = 0

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return True


def _make_plugin_module(name: str):
    mod = types.ModuleType(name)

    def setup(ctx):
        @ctx.on_command("demo")
        async def demo(c, args):
            return None

    mod.setup = setup  # type: ignore[attr-defined]
    sys.modules[name] = mod
    return mod


def _make_configurable_plugin_module(name: str):
    mod = types.ModuleType(name)

    class RetryConfig(BaseModel):
        timeout: int = 5

    class DemoPluginConfig(BaseModel):
        api_key: str = ""
        retry: RetryConfig = RetryConfig()

    def setup(ctx):
        @ctx.on_command("demo-config")
        async def demo(c, args):
            return None

    mod.setup = setup  # type: ignore[attr-defined]
    mod.__plugin_config_class__ = DemoPluginConfig  # type: ignore[attr-defined]
    sys.modules[name] = mod
    return mod


def test_plugins_enable_disable_routes(tmp_path: Path):
    module_name = "test_api_plugin_toggle"
    sys.modules.pop(module_name, None)
    _make_plugin_module(module_name)

    bot = ShinBot(data_dir=tmp_path)
    bot.load_plugin("demo-plugin", module_name)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    try:
        with TestClient(app) as client:
            disable_resp = client.post("/api/v1/plugins/demo-plugin/disable", headers=headers)
            assert disable_resp.status_code == 200
            assert disable_resp.json()["data"]["status"] == "disabled"

            list_resp = client.get("/api/v1/plugins", headers=headers)
            assert list_resp.status_code == 200
            statuses = {item["id"]: item["status"] for item in list_resp.json()["data"]}
            assert statuses["demo-plugin"] == "disabled"

            enable_resp = client.post("/api/v1/plugins/demo-plugin/enable", headers=headers)
            assert enable_resp.status_code == 200
            assert enable_resp.json()["data"]["status"] == "enabled"
    finally:
        sys.modules.pop(module_name, None)


def test_adapter_plugin_schema_endpoint_is_hidden_from_plugin_management(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.load_plugin(
        "shinbot_adapter_onebot_v11",
        "shinbot.builtin_plugins.shinbot_adapter_onebot_v11",
    )
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/plugins/shinbot_adapter_onebot_v11/schema",
            headers=headers,
        )

    assert response.status_code == 404
    assert response.json()["error"]["message"].endswith(
        "does not expose plugin-level configuration"
    )


def test_plugin_config_route_persists_config_instead_of_reloading(tmp_path: Path):
    module_name = "test_api_plugin_config"
    sys.modules.pop(module_name, None)
    _make_configurable_plugin_module(module_name)

    bot = ShinBot(data_dir=tmp_path)
    bot.load_plugin("demo-config-plugin", module_name)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}

    try:
        with TestClient(app) as client:
            response = client.patch(
                "/api/v1/plugins/demo-config-plugin/config",
                headers=headers,
                json={"api_key": "secret", "retry.timeout": 12},
            )

            assert response.status_code == 200
            assert response.json()["data"]["metadata"]["config"] == {
                "api_key": "secret",
                "retry": {"timeout": 12},
            }
            assert boot.config["plugin_configs"]["demo-config-plugin"] == {
                "api_key": "secret",
                "retry": {"timeout": 12},
            }
            assert boot.save_config_calls == 1
    finally:
        sys.modules.pop(module_name, None)
