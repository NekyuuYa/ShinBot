from __future__ import annotations

import sys
import types
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import BaseModel

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.plugins.config import normalize_plugin_config, translate_plugin_schema


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


def test_plugins_list_and_schema_apply_locale_translations(tmp_path: Path):
    module_name = "test_api_plugin_i18n"
    sys.modules.pop(module_name, None)

    mod = types.ModuleType(module_name)

    class DemoPluginConfig(BaseModel):
        api_key: str = ""

    def setup(ctx):
        @ctx.on_command("demo-i18n")
        async def demo(c, args):
            return None

    mod.setup = setup  # type: ignore[attr-defined]
    mod.__plugin_name__ = "Demo Plugin"  # type: ignore[attr-defined]
    mod.__plugin_description__ = "Demo description"  # type: ignore[attr-defined]
    mod.__plugin_config_class__ = DemoPluginConfig  # type: ignore[attr-defined]
    mod.__plugin_locales__ = {  # type: ignore[attr-defined]
        "zh-CN": {
            "meta.name": "演示插件",
            "meta.description": "演示描述",
            "config.title": "演示配置",
            "config.fields.api_key.label": "接口密钥",
            "config.fields.api_key.description": "用于访问服务的密钥",
        }
    }
    sys.modules[module_name] = mod

    bot = ShinBot(data_dir=tmp_path)
    bot.load_plugin("demo-i18n-plugin", module_name)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    try:
        with TestClient(app) as client:
            list_resp = client.get("/api/v1/plugins", headers=headers)
            assert list_resp.status_code == 200
            payload = list_resp.json()["data"][0]
            assert payload["name"] == "演示插件"
            assert payload["description"] == "演示描述"
            assert payload["metadata"]["config_schema"]["title"] == "演示配置"
            assert payload["metadata"]["config_schema"]["properties"]["api_key"]["title"] == "接口密钥"

            schema_resp = client.get("/api/v1/plugins/demo-i18n-plugin/schema", headers=headers)
            assert schema_resp.status_code == 200
            schema = schema_resp.json()["data"]
            assert schema["title"] == "演示配置"
            assert schema["properties"]["api_key"]["title"] == "接口密钥"
            assert schema["properties"]["api_key"]["description"] == "用于访问服务的密钥"
    finally:
        sys.modules.pop(module_name, None)


def test_plugins_list_and_schema_load_locale_files(tmp_path: Path):
    plugins_root = tmp_path / "plugins"
    plugin_dir = plugins_root / "demo_file_i18n"
    locales_dir = plugin_dir / "locales"
    locales_dir.mkdir(parents=True)

    (plugin_dir / "__init__.py").write_text(
        "\n".join(
            [
                "from pydantic import BaseModel",
                "",
                '__plugin_name__ = "Demo File Plugin"',
                '__plugin_description__ = "Demo file description"',
                "",
                "class DemoPluginConfig(BaseModel):",
                '    api_key: str = ""',
                "",
                "__plugin_config_class__ = DemoPluginConfig",
                "",
                "def setup(ctx):",
                '    @ctx.on_command("demo-file")',
                "    async def demo(c, args):",
                "        return None",
            ]
        ),
        encoding="utf-8",
    )
    (locales_dir / "zh-CN.json").write_text(
        """
{
  "meta.name": "文件插件",
  "meta.description": "来自 locale 文件的描述",
  "config.title": "文件配置",
  "config.fields.api_key.label": "接口密钥"
}
""".strip(),
        encoding="utf-8",
    )

    bot = ShinBot(data_dir=tmp_path)
    bot.plugin_manager.load_plugins_from_dir(plugins_root)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "zh-CN,zh;q=0.9",
    }

    with TestClient(app) as client:
        list_resp = client.get("/api/v1/plugins", headers=headers)
        assert list_resp.status_code == 200
        payload = list_resp.json()["data"][0]
        assert payload["name"] == "文件插件"
        assert payload["description"] == "来自 locale 文件的描述"
        assert payload["metadata"]["config_schema"]["title"] == "文件配置"
        assert payload["metadata"]["config_schema"]["properties"]["api_key"]["title"] == "接口密钥"

        schema_resp = client.get("/api/v1/plugins/demo_file_i18n/schema", headers=headers)
        assert schema_resp.status_code == 200
        schema = schema_resp.json()["data"]
        assert schema["title"] == "文件配置"
        assert schema["properties"]["api_key"]["title"] == "接口密钥"


def test_plugin_config_helpers_are_reusable_after_module_split(tmp_path: Path):
    module_name = "test_api_plugin_config_helpers"
    sys.modules.pop(module_name, None)
    _make_configurable_plugin_module(module_name)

    bot = ShinBot(data_dir=tmp_path)
    bot.load_plugin("demo-config-plugin", module_name)

    try:
        normalized = normalize_plugin_config(
            bot.plugin_manager,
            "demo-config-plugin",
            {"api_key": "secret", "retry.timeout": 8},
        )
        translated = translate_plugin_schema(
            {"type": "object", "properties": {"api_key": {"type": "string"}}},
            {"config.fields.api_key.label": "接口密钥"},
        )

        assert normalized == {"api_key": "secret", "retry": {"timeout": 8}}
        assert translated is not None
        assert translated["properties"]["api_key"]["title"] == "接口密钥"
    finally:
        sys.modules.pop(module_name, None)
