from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from shinbot.admin.persona_files import render_persona_markdown
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


def _auth_headers(app) -> dict[str, str]:
    token = app.state.auth_config.create_token()
    return {"Authorization": f"Bearer {token}"}


def test_agent_configs_api_lists_data_agent_profiles(tmp_path: Path):
    personas_dir = tmp_path / "personas"
    personas_dir.mkdir()
    (personas_dir / "companion.md").write_text(
        render_persona_markdown(
            persona_id="companion",
            name="Companion",
            prompt_text="You are a helpful companion.",
            tags=[],
            enabled=True,
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
        ),
        encoding="utf-8",
    )
    agents_dir = tmp_path / "agents"
    agents_dir.mkdir()
    (agents_dir / "full-agent.toml").write_text(
        "[agent]\nid = \"full-agent\"\nmode = \"full\"\npersona_id = \"companion\"\n",
        encoding="utf-8",
    )
    app = create_api_app(ShinBot(data_dir=tmp_path), _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.get("/api/v1/agent-configs", headers=_auth_headers(app))

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload[0]["fileName"] == "full-agent.toml"
    assert payload[0]["path"] == "agents/full-agent.toml"
    assert payload[0]["agentId"] == "full-agent"
    assert payload[0]["issues"] == []


def test_agent_configs_api_creates_profile_from_agent_id(tmp_path: Path):
    app = create_api_app(ShinBot(data_dir=tmp_path), _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/agent-configs",
            headers=_auth_headers(app),
            json={"config": {"agent": {"id": "main-agent", "mode": "full"}}},
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["fileName"] == "main-agent.toml"
    assert (tmp_path / "agents" / "main-agent.toml").is_file()


def test_agent_configs_api_rejects_invalid_profile(tmp_path: Path):
    app = create_api_app(ShinBot(data_dir=tmp_path), _BootStub(tmp_path))

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/agent-configs",
            headers=_auth_headers(app),
            json={"fileName": "broken.toml", "config": {"agent": {"mode": "full"}}},
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "CONFIG_VALIDATION_FAILED"
    assert response.json()["data"]["issues"][0]["path"] == "agent.id"
    assert not (tmp_path / "agents" / "broken.toml").exists()
