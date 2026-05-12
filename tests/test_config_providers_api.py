from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot
from shinbot.core.config_provider import load_provider_schema


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


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def _schema_path(relative: str) -> Path:
    return Path(__file__).resolve().parents[1] / relative


def test_config_provider_catalog_and_details_api(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.config_provider_registry.register(
        load_provider_schema(
            _schema_path("shinbot/builtin_plugins/shinbot_adapter_qqofficial/config.schema.toml"),
            example_path=_schema_path(
                "shinbot/builtin_plugins/shinbot_adapter_qqofficial/config.example.toml"
            ),
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        catalog = client.get(
            "/api/v1/config-providers?kind=adapter",
            headers=_auth_headers(app),
        )
        detail = client.get(
            "/api/v1/config-providers/adapter/qqofficial",
            headers=_auth_headers(app),
        )
        defaults = client.get(
            "/api/v1/config-providers/adapter/qqofficial/defaults",
            headers=_auth_headers(app),
        )

    assert catalog.status_code == 200
    assert [item["id"] for item in catalog.json()["data"]] == ["qqofficial"]
    assert detail.status_code == 200
    assert detail.json()["data"]["id"] == "qqofficial"
    assert defaults.status_code == 200
    assert defaults.json()["data"]["app_secret"] == ""


def test_config_provider_validate_api_reports_field_issues(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    bot.config_provider_registry.register(
        load_provider_schema(
            _schema_path("shinbot/builtin_plugins/shinbot_adapter_onebot_v11/config.schema.toml")
        )
    )
    app = create_api_app(bot, _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/config-providers/adapter/onebot_v11/validate",
            headers=_auth_headers(app),
            json={
                "config": {"mode": "bad", "reverse_port": 70000},
                "pathPrefix": "adapter_instances[0].config",
            },
        )

    assert response.status_code == 200
    issues = response.json()["data"]["issues"]
    assert [issue["code"] for issue in issues] == ["choices", "max"]
    assert issues[0]["path"] == "adapter_instances[0].config.mode"
