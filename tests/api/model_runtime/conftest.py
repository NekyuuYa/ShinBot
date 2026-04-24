from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest


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


@pytest.fixture
def make_boot_stub() -> Callable[[Path], _BootStub]:
    def _make(data_dir: Path) -> _BootStub:
        return _BootStub(data_dir)

    return _make


@pytest.fixture
def make_auth_headers() -> Callable[[Any], dict[str, str]]:
    def _make(app: Any) -> dict[str, str]:
        token = app.state.auth_config.create_token()
        return {"Authorization": f"Bearer {token}"}

    return _make
