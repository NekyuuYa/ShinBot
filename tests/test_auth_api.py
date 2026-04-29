from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

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
            },
            "instances": [],
        }
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.save_config_calls = 0
        self.save_config_result = True

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return self.save_config_result


def _login(
    client: TestClient,
    username: str = "admin",
    password: str = "admin",
):
    return client.post(
        "/api/v1/auth/login",
        json={"username": username, "password": password},
    )


def test_login_marks_default_credentials_for_immediate_change(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        response = _login(client)

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["username"] == "admin"
    assert payload["must_change_credentials"] is True
    assert payload["expires_in_hours"] == 24
    assert "token" not in payload

    set_cookie = response.headers.get("set-cookie", "").lower()
    assert "shinbot_session=" in set_cookie
    assert "httponly" in set_cookie
    assert "samesite=strict" in set_cookie


def test_profile_update_requires_non_default_username_and_password(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        login = _login(client)
        response = client.patch(
            "/api/v1/auth/profile",
            json={
                "username": "admin",
                "current_password": "admin",
                "new_password": "new-password",
            },
        )

    assert login.status_code == 200
    assert response.status_code == 400
    assert response.json()["success"] is False


def test_profile_update_persists_credentials_and_returns_refreshed_token(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        login = _login(client)
        profile_before = client.get("/api/v1/auth/profile")
        update = client.patch(
            "/api/v1/auth/profile",
            json={
                "username": "owner",
                "current_password": "admin",
                "new_password": "strong-password",
            },
        )
        profile_after = client.get("/api/v1/auth/profile")
        login_old = _login(client)
        login_new = _login(client, username="owner", password="strong-password")

    assert login.status_code == 200
    assert profile_before.status_code == 200
    assert profile_before.json()["data"] == {
        "username": "admin",
        "must_change_credentials": True,
    }

    assert update.status_code == 200
    update_data = update.json()["data"]
    assert update_data["username"] == "owner"
    assert update_data["must_change_credentials"] is False
    assert "token" not in update_data

    assert profile_after.status_code == 200
    assert profile_after.json()["data"] == {
        "username": "owner",
        "must_change_credentials": False,
    }

    assert boot.save_config_calls == 1
    assert boot.config["admin"]["username"] == "owner"
    assert boot.config["admin"]["password"] == "strong-password"

    assert login_old.status_code == 401
    assert login_new.status_code == 200


def test_profile_update_rollback_when_config_persist_fails(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    boot.save_config_result = False
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        login = _login(client)
        response = client.patch(
            "/api/v1/auth/profile",
            json={
                "username": "owner",
                "current_password": "admin",
                "new_password": "strong-password",
            },
        )

    assert login.status_code == 200
    assert response.status_code == 500
    assert boot.config["admin"]["username"] == "admin"
    assert boot.config["admin"]["password"] == "admin"


def test_logout_clears_session_cookie(tmp_path: Path):
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)

    with TestClient(app) as client:
        login = _login(client)
        logout = client.post("/api/v1/auth/logout")
        profile = client.get("/api/v1/auth/profile")

        assert client.cookies.get(app.state.auth_config.session_cookie_name) is None

    assert login.status_code == 200
    assert logout.status_code == 200
    assert logout.json()["data"] == {"logged_out": True}
    assert profile.status_code == 401
