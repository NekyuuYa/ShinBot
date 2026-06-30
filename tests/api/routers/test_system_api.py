"""Tests for the system management API router."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from shinbot.api.app import create_api_app
from shinbot.api.models import EC
from shinbot.core.application.runtime_control import RuntimeControl
from shinbot.core.application.system_update.common import SystemUpdateError

# ── Helpers ──────────────────────────────────────────────────────────────────


class _BootStub:
    """Minimal boot controller stub for system router tests."""

    def __init__(self, data_dir):
        from pathlib import Path

        self.config: dict[str, Any] = {
            "admin": {
                "username": "admin",
                "password": "admin",
                "jwt_secret": "test-secret-that-is-long-enough-for-hs256",
                "jwt_expire_hours": 24,
            },
        }
        self.data_dir = Path(data_dir)
        self.dashboard_dist_dir = None
        self.dashboard_index_file = None
        self.save_config_calls = 0
        self.save_config_result = True

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return self.save_config_result


def _make_app(tmp_path, *, non_default_creds: bool = False):
    """Create a test app with authenticated client and runtime control."""
    from shinbot.core.application.app import ShinBot

    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    if non_default_creds:
        boot.config["admin"]["username"] = "owner"
        boot.config["admin"]["password"] = "admin"
        from shinbot.api.auth import AuthConfig

        ac = AuthConfig(boot.config, boot.data_dir)
        boot.config["admin"]["password_hash"] = ac._hash_password("admin")
        del boot.config["admin"]["password"]
    runtime_control = RuntimeControl()
    app = create_api_app(bot, boot, runtime_control=runtime_control)
    token = app.state.auth_config.create_token()
    return app, token, boot, runtime_control


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ── Public endpoints ─────────────────────────────────────────────────────────


class TestHealthEndpoint:
    """GET /api/v1/system/health — public, no auth required."""

    def test_health_returns_healthy_status(self, tmp_path):
        from shinbot.core.application.app import ShinBot

        bot = ShinBot(data_dir=tmp_path)
        boot = _BootStub(tmp_path)
        app = create_api_app(bot, boot)

        with TestClient(app) as client:
            response = client.get("/api/v1/system/health")

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["status"] == "healthy"

    def test_health_does_not_require_auth(self, tmp_path):
        """Health endpoint should work without any auth cookie or header."""
        from shinbot.core.application.app import ShinBot

        bot = ShinBot(data_dir=tmp_path)
        boot = _BootStub(tmp_path)
        app = create_api_app(bot, boot)

        with TestClient(app) as client:
            response = client.get("/api/v1/system/health")

        assert response.status_code == 200


# ── Runtime state ────────────────────────────────────────────────────────────


class TestRuntimeState:
    """GET /api/v1/system/runtime — authenticated."""

    def test_runtime_state_no_restart_pending(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.get(
                "/api/v1/system/runtime", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["restartRequested"] is False
        assert body["data"]["restartRequest"] is None

    def test_runtime_state_with_restart_pending(self, tmp_path):
        app, token, _, rc = _make_app(tmp_path)
        from shinbot.core.application.runtime_control import RestartReason

        rc.request_restart(reason=RestartReason.MANUAL, requested_by="test")

        with TestClient(app) as client:
            response = client.get(
                "/api/v1/system/runtime", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["restartRequested"] is True
        assert body["data"]["restartRequest"] is not None
        assert body["data"]["restartRequest"]["reason"] == "manual"

    def test_runtime_state_requires_auth(self, tmp_path):
        app, _, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.get("/api/v1/system/runtime")

        assert response.status_code == 401


# ── Restart ──────────────────────────────────────────────────────────────────


class TestRestart:
    """POST /api/v1/system/restart — authenticated."""

    def test_restart_manual(self, tmp_path):
        app, token, _, rc = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/restart",
                json={"reason": "manual", "requestedBy": "test-user"},
                headers=_auth_headers(token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["accepted"] is True
        assert body["data"]["restartRequested"] is True
        assert body["data"]["restartRequest"]["reason"] == "manual"
        assert rc.restart_requested is True

    def test_restart_update_reason(self, tmp_path):
        app, token, _, rc = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/restart",
                json={"reason": "update"},
                headers=_auth_headers(token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["data"]["restartRequest"]["reason"] == "update"

    def test_restart_default_reason_is_manual(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/restart",
                json={},
                headers=_auth_headers(token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["data"]["restartRequest"]["reason"] == "manual"

    def test_restart_conflict_when_already_requested(self, tmp_path):
        app, token, _, rc = _make_app(tmp_path)
        from shinbot.core.application.runtime_control import RestartReason

        rc.request_restart(reason=RestartReason.MANUAL, requested_by="first")

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/restart",
                json={"reason": "manual"},
                headers=_auth_headers(token),
            )

        assert response.status_code == 409
        body = response.json()
        assert body["success"] is False
        assert body["error"]["code"] == EC.RESTART_ALREADY_REQUESTED

    def test_restart_requires_auth(self, tmp_path):
        app, _, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/restart", json={"reason": "manual"}
            )

        assert response.status_code == 401


# ── Logging ──────────────────────────────────────────────────────────────────


class TestLoggingState:
    """GET /api/v1/system/logging and PATCH /api/v1/system/logging."""

    def test_get_logging_state(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.get(
                "/api/v1/system/logging", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        data = body["data"]
        assert "level" in data
        assert "effectiveLevel" in data
        assert "handlers" in data
        assert "sources" in data

    def test_patch_logging_level(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.patch(
                "/api/v1/system/logging",
                json={"level": "DEBUG"},
                headers=_auth_headers(token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["level"] == "DEBUG"

    def test_patch_logging_invalid_level(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.patch(
                "/api/v1/system/logging",
                json={"level": "INVALID_LEVEL_XYZ"},
                headers=_auth_headers(token),
            )

        assert response.status_code == 400
        body = response.json()
        assert body["success"] is False
        assert body["error"]["code"] == EC.INVALID_ACTION

    def test_patch_logging_persist(self, tmp_path):
        app, token, boot, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.patch(
                "/api/v1/system/logging",
                json={"level": "WARNING", "persist": True},
                headers=_auth_headers(token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["data"]["level"] == "WARNING"
        assert boot.save_config_calls == 1
        assert boot.config["logging"]["level"] == "WARNING"

    def test_patch_logging_persist_failure(self, tmp_path):
        app, token, boot, _ = _make_app(tmp_path)
        boot.save_config_result = False

        with TestClient(app) as client:
            response = client.patch(
                "/api/v1/system/logging",
                json={"level": "ERROR", "persist": True},
                headers=_auth_headers(token),
            )

        assert response.status_code == 500
        body = response.json()
        assert body["success"] is False
        assert body["error"]["code"] == EC.CONFIG_WRITE_FAILED

    def test_patch_logging_third_party_noise(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.patch(
                "/api/v1/system/logging",
                json={"thirdPartyNoise": "debug"},
                headers=_auth_headers(token),
            )

        assert response.status_code == 200
        body = response.json()
        assert body["data"]["thirdPartyNoise"] == "debug"

    def test_logging_requires_auth(self, tmp_path):
        app, _, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.get("/api/v1/system/logging")

        assert response.status_code == 401


# ── Update state (GET) ──────────────────────────────────────────────────────


class TestUpdateState:
    """GET /api/v1/system/update — authenticated."""

    def test_update_state_returns_guarded_status(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        mock_status = {
            "canUpdate": True,
            "currentVersion": "1.0.0",
            "latestVersion": "1.1.0",
        }
        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "inspect",
                new_callable=AsyncMock,
                return_value=mock_status,
            ),
        ):
            response = client.get(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        data = body["data"]
        assert data["credentialsChangeRequired"] is False
        assert data["restartRequested"] is False
        assert data["canUpdate"] is True

    def test_update_state_blocked_by_default_credentials(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        mock_status = {"canUpdate": True}
        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "inspect",
                new_callable=AsyncMock,
                return_value=mock_status,
            ),
        ):
            response = client.get(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        data = body["data"]
        assert data["credentialsChangeRequired"] is True
        assert data["canUpdate"] is False
        assert data["blockCode"] == "default_credentials"

    def test_update_state_blocked_by_pending_restart(self, tmp_path):
        app, token, _, rc = _make_app(tmp_path, non_default_creds=True)
        from shinbot.core.application.runtime_control import RestartReason

        rc.request_restart(reason=RestartReason.MANUAL, requested_by="test")

        mock_status = {"canUpdate": True}
        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "inspect",
                new_callable=AsyncMock,
                return_value=mock_status,
            ),
        ):
            response = client.get(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        data = body["data"]
        assert data["restartRequested"] is True
        assert data["canUpdate"] is False
        assert data["blockCode"] == "restart_pending"

    def test_update_state_inspect_error(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "inspect",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Git not available",
                    status_code=500,
                ),
            ),
        ):
            response = client.get(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 500
        body = response.json()
        assert body["success"] is False
        assert body["error"]["code"] == EC.UPDATE_FAILED


# ── Update apply (POST) ─────────────────────────────────────────────────────


class TestApplyUpdate:
    """POST /api/v1/system/update — authenticated."""

    def test_apply_update_success(self, tmp_path):
        app, token, _, rc = _make_app(tmp_path, non_default_creds=True)

        result = {
            "accepted": True,
            "updated": True,
            "restartRequested": True,
        }
        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "run_and_request_restart",
                new_callable=AsyncMock,
                return_value=result,
            ),
        ):
            response = client.post(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["updated"] is True

    def test_apply_update_blocked_by_default_credentials(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 403
        body = response.json()
        assert body["success"] is False
        assert body["error"]["code"] == EC.UPDATE_NOT_ALLOWED

    def test_apply_update_already_running(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "run_and_request_restart",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="UPDATE_ALREADY_RUNNING",
                    message="Update already in progress",
                    status_code=409,
                ),
            ),
        ):
            response = client.post(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 409
        body = response.json()
        assert body["error"]["code"] == EC.UPDATE_ALREADY_RUNNING

    def test_apply_update_command_failure_with_output(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "run_and_request_restart",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Command failed",
                    status_code=500,
                    output="fatal: not a git repository",
                ),
            ),
        ):
            response = client.post(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 500
        body = response.json()
        assert body["error"]["code"] == EC.UPDATE_FAILED
        assert "fatal: not a git repository" in body["error"]["message"]

    def test_apply_update_restart_already_requested(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.framework_update_service,
                "run_and_request_restart",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="RESTART_ALREADY_REQUESTED",
                    message="Restart already pending",
                    status_code=409,
                ),
            ),
        ):
            response = client.post(
                "/api/v1/system/update", headers=_auth_headers(token)
            )

        assert response.status_code == 409
        body = response.json()
        assert body["error"]["code"] == EC.RESTART_ALREADY_REQUESTED

    def test_apply_update_requires_auth(self, tmp_path):
        app, _, _, _ = _make_app(tmp_path, non_default_creds=True)

        with TestClient(app) as client:
            response = client.post("/api/v1/system/update")

        assert response.status_code == 401


# ── Dashboard build state (GET) ─────────────────────────────────────────────


class TestDashboardBuildState:
    """GET /api/v1/system/dashboard-build — authenticated."""

    def test_dashboard_build_state_returns_status(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        mock_status = {
            "canBuild": True,
            "dashboardDir": "/some/path",
            "buildInProgress": False,
        }
        with (
            TestClient(app) as client,
            patch.object(
                app.state.dashboard_build_service,
                "inspect",
                new_callable=AsyncMock,
                return_value=mock_status,
            ),
        ):
            response = client.get(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        data = body["data"]
        assert data["credentialsChangeRequired"] is False
        assert data["canBuild"] is True

    def test_dashboard_build_state_blocked_by_default_credentials(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        mock_status = {"canBuild": True}
        with (
            TestClient(app) as client,
            patch.object(
                app.state.dashboard_build_service,
                "inspect",
                new_callable=AsyncMock,
                return_value=mock_status,
            ),
        ):
            response = client.get(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        data = body["data"]
        assert data["credentialsChangeRequired"] is True
        assert data["canBuild"] is False
        assert data["blockCode"] == "default_credentials"


# ── Dashboard build (POST) ──────────────────────────────────────────────────


class TestBuildDashboard:
    """POST /api/v1/system/dashboard-build — authenticated."""

    def test_build_dashboard_success(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        result = {
            "accepted": True,
            "built": True,
            "distPath": "/some/dist",
        }
        with (
            TestClient(app) as client,
            patch.object(
                app.state.dashboard_build_service,
                "build",
                new_callable=AsyncMock,
                return_value=result,
            ),
        ):
            response = client.post(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 200
        body = response.json()
        assert body["success"] is True
        assert body["data"]["built"] is True

    def test_build_dashboard_blocked_by_default_credentials(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path)

        with TestClient(app) as client:
            response = client.post(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 403
        body = response.json()
        assert body["success"] is False
        assert body["error"]["code"] == EC.UPDATE_NOT_ALLOWED

    def test_build_dashboard_already_running(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.dashboard_build_service,
                "build",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="UPDATE_ALREADY_RUNNING",
                    message="Build already in progress",
                    status_code=409,
                ),
            ),
        ):
            response = client.post(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 409
        body = response.json()
        assert body["error"]["code"] == EC.UPDATE_ALREADY_RUNNING

    def test_build_dashboard_command_failure_with_output(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.dashboard_build_service,
                "build",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Build failed",
                    status_code=500,
                    output="ERR pnpm not found",
                ),
            ),
        ):
            response = client.post(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 500
        body = response.json()
        assert body["error"]["code"] == EC.UPDATE_FAILED
        assert "ERR pnpm not found" in body["error"]["message"]

    def test_build_dashboard_update_not_allowed(self, tmp_path):
        app, token, _, _ = _make_app(tmp_path, non_default_creds=True)

        with (
            TestClient(app) as client,
            patch.object(
                app.state.dashboard_build_service,
                "build",
                new_callable=AsyncMock,
                side_effect=SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message="Missing prerequisites",
                    status_code=403,
                ),
            ),
        ):
            response = client.post(
                "/api/v1/system/dashboard-build", headers=_auth_headers(token)
            )

        assert response.status_code == 403
        body = response.json()
        assert body["error"]["code"] == EC.UPDATE_NOT_ALLOWED

    def test_build_dashboard_requires_auth(self, tmp_path):
        app, _, _, _ = _make_app(tmp_path, non_default_creds=True)

        with TestClient(app) as client:
            response = client.post("/api/v1/system/dashboard-build")

        assert response.status_code == 401
