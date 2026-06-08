from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from shinbot.api.app import create_api_app
from shinbot.core.application.app import ShinBot


@dataclass(slots=True)
class RouterApiHarness:
    """Test API app bundle with authenticated request headers."""

    data_dir: Path
    bot: ShinBot
    boot: RouterBootStub
    app: Any
    headers: dict[str, str]


class RouterBootStub:
    """Boot controller stub for router tests."""

    def __init__(self, data_dir: Path, admin_config: dict[str, Any]) -> None:
        self.config = {"admin": dict(admin_config)}
        self.data_dir = data_dir
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None


@pytest.fixture(scope="session")
def router_admin_config() -> dict[str, Any]:
    """Return shared admin auth config for router API tests."""

    return {
        "username": "admin",
        "password": "admin",
        "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
        "jwt_expire_hours": 24,
    }


@pytest.fixture
def router_api(tmp_path: Path, router_admin_config: dict[str, Any]) -> RouterApiHarness:
    """Create an authenticated API app harness for one isolated data dir."""

    bot = ShinBot(data_dir=tmp_path)
    boot = RouterBootStub(tmp_path, router_admin_config)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    return RouterApiHarness(
        data_dir=tmp_path,
        bot=bot,
        boot=boot,
        app=app,
        headers={"Authorization": f"Bearer {token}"},
    )
