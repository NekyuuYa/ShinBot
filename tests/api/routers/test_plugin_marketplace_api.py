from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient

import shinbot.admin.plugin_marketplace as plugin_marketplace
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


def _client(tmp_path: Path) -> tuple[TestClient, ShinBot, _BootStub, dict[str, str]]:
    bot = ShinBot(data_dir=tmp_path)
    boot = _BootStub(tmp_path)
    app = create_api_app(bot, boot)
    token = app.state.auth_config.create_token()
    headers = {"Authorization": f"Bearer {token}"}
    return TestClient(app), bot, boot, headers


def _metadata_archive_zip(
    *,
    version: str = "0.1.0",
    required_dependencies: list[str] | None = None,
) -> bytes:
    demo_metadata = {
        "id": "shinbot_plugin_market_demo",
        "name": "Market Demo",
        "version": version,
        "description": "Demo plugin from marketplace",
        "author": "Tests",
        "role": "logic",
        "entry": "shinbot_plugin_market_demo/__init__.py",
        "permissions": ["send_message"],
        "required_dependencies": required_dependencies or [],
        "optional_dependencies": ["shinbot_plugin_optional_demo"],
        "tags": ["demo"],
    }
    other_metadata = {
        "id": "shinbot_plugin_other_demo",
        "name": "Other Demo",
        "version": "0.1.0",
        "role": "logic",
        "entry": "shinbot_plugin_other_demo/__init__.py",
        "permissions": [],
    }
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        root = "repo"
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_market_demo/metadata.json",
            json.dumps(demo_metadata),
        )
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_other_demo/metadata.json",
            json.dumps(other_metadata),
        )
    return stream.getvalue()


def _plugin_archive_zip(*, version: str = "0.1.0") -> bytes:
    metadata = {
        "id": "shinbot_plugin_market_demo",
        "name": "Market Demo",
        "version": version,
        "description": "Demo plugin from marketplace",
        "author": "Tests",
        "role": "logic",
        "entry": "shinbot_plugin_market_demo/__init__.py",
        "permissions": ["send_message"],
        "optional_dependencies": ["shinbot_plugin_optional_demo"],
        "tags": ["demo"],
    }
    setup_body = "\n".join(
        [
            "def setup(plg):",
            "    @plg.on_command('market-demo')",
            "    async def demo(c, args):",
            "        return None",
            "",
        ]
    )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        root = "repo"
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_market_demo/metadata.json",
            json.dumps(metadata),
        )
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_market_demo/shinbot_plugin_market_demo/__init__.py",
            setup_body,
        )
    return stream.getvalue()


def _patch_sparse_archives(
    monkeypatch,
    archives_by_scope: dict[str, list[bytes]],
) -> list[tuple[str, list[str]]]:
    calls: list[tuple[str, list[str]]] = []

    async def fake_create_sparse_archive(self, source, *, sparse_paths):
        scope = "metadata" if sparse_paths == ["plugins/*/metadata.json"] else "plugin"
        calls.append((scope, sparse_paths))
        archives = archives_by_scope[scope]
        return plugin_marketplace.PluginMarketplaceArchive(
            content=archives.pop(0),
            resolved_ref=f"resolved-{len(calls)}",
        )

    monkeypatch.setattr(
        plugin_marketplace.PluginMarketplaceService,
        "_create_sparse_archive",
        fake_create_sparse_archive,
    )
    return calls


def test_plugin_marketplace_sources_returns_official_source(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.get("/api/v1/plugin-marketplace/sources", headers=headers)

    assert response.status_code == 200
    source = response.json()["data"]["sources"][0]
    assert source["id"] == "official"
    assert source["repository_url"] == "https://github.com/NekyuuYa/shinbot-plugins"
    assert source["ref"] == "main"


def test_plugin_marketplace_lists_monorepo_plugins(tmp_path: Path, monkeypatch):
    calls = _patch_sparse_archives(
        monkeypatch,
        {"metadata": [_metadata_archive_zip()], "plugin": []},
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        response = client.get("/api/v1/plugin-marketplace", headers=headers)

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["source"]["id"] == "official"
    plugins = {item["plugin_id"]: item for item in payload["plugins"]}
    demo = plugins["shinbot_plugin_market_demo"]
    assert demo["plugin_path"] == "plugins/shinbot_plugin_market_demo"
    assert demo["repository_url"] == "https://github.com/NekyuuYa/shinbot-plugins"
    assert demo["installed"] is False
    assert demo["can_install"] is True
    assert demo["missing_optional_dependencies"] == ["shinbot_plugin_optional_demo"]
    assert payload["cache"]["cached"] is False
    assert calls == [("metadata", ["plugins/*/metadata.json"])]


def test_plugin_marketplace_reuses_cached_archive(tmp_path: Path, monkeypatch):
    calls = _patch_sparse_archives(
        monkeypatch,
        {
            "metadata": [_metadata_archive_zip()],
            "plugin": [_plugin_archive_zip()],
        },
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        first = client.get("/api/v1/plugin-marketplace", headers=headers)
        second = client.get("/api/v1/plugin-marketplace", headers=headers)
        preview = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/preview",
            headers=headers,
            json={"source": "official"},
        )

    assert first.status_code == 200
    assert first.json()["data"]["cache"]["cached"] is False
    assert second.status_code == 200
    assert second.json()["data"]["cache"]["cached"] is True
    assert preview.status_code == 200
    assert calls == [
        ("metadata", ["plugins/*/metadata.json"]),
        ("plugin", ["plugins/shinbot_plugin_market_demo/**"]),
    ]


def test_plugin_marketplace_refresh_bypasses_cached_archive(tmp_path: Path, monkeypatch):
    calls = _patch_sparse_archives(
        monkeypatch,
        {
            "metadata": [
                _metadata_archive_zip(version="0.1.0"),
                _metadata_archive_zip(version="0.2.0"),
            ],
            "plugin": [],
        },
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        first = client.get("/api/v1/plugin-marketplace", headers=headers)
        refreshed = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"refresh": "true"},
        )

    assert first.status_code == 200
    assert refreshed.status_code == 200
    demo = {
        item["plugin_id"]: item for item in refreshed.json()["data"]["plugins"]
    }["shinbot_plugin_market_demo"]
    assert demo["version"] == "0.2.0"
    assert refreshed.json()["data"]["cache"]["cached"] is False
    assert len(calls) == 2


def test_plugin_marketplace_marks_missing_required_dependency(tmp_path: Path, monkeypatch):
    _patch_sparse_archives(
        monkeypatch,
        {
            "metadata": [
                _metadata_archive_zip(
                    required_dependencies=["shinbot_plugin_missing_required"]
                )
            ],
            "plugin": [],
        },
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        response = client.get("/api/v1/plugin-marketplace", headers=headers)

    assert response.status_code == 200
    demo = {
        item["plugin_id"]: item for item in response.json()["data"]["plugins"]
    }["shinbot_plugin_market_demo"]
    assert demo["can_install"] is False
    assert demo["missing_required_dependencies"] == ["shinbot_plugin_missing_required"]


def test_plugin_marketplace_installs_selected_monorepo_plugin(tmp_path: Path, monkeypatch):
    calls = _patch_sparse_archives(
        monkeypatch,
        {
            "metadata": [_metadata_archive_zip()],
            "plugin": [_plugin_archive_zip()],
        },
    )
    client, bot, _boot, headers = _client(tmp_path)

    with client:
        install = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/install",
            headers=headers,
            json={"enable_after_install": True},
        )
        listed = client.get("/api/v1/plugin-marketplace", headers=headers)

    assert install.status_code == 200
    assert install.json()["data"]["status"] == "succeeded"
    assert bot.plugin_manager.get_plugin("shinbot_plugin_market_demo") is not None
    assert bot.plugin_manager.get_plugin("shinbot_plugin_other_demo") is None
    demo = {
        item["plugin_id"]: item for item in listed.json()["data"]["plugins"]
    }["shinbot_plugin_market_demo"]
    assert demo["installed"] is True
    assert demo["managed_by_webui"] is True
    assert demo["can_install"] is False
    assert demo["installed_source"]["plugin_path"] == "plugins/shinbot_plugin_market_demo"
    assert calls == [
        ("metadata", ["plugins/*/metadata.json"]),
        ("plugin", ["plugins/shinbot_plugin_market_demo/**"]),
    ]


def test_plugin_marketplace_preview_uses_selected_plugin_path(tmp_path: Path, monkeypatch):
    calls = _patch_sparse_archives(
        monkeypatch,
        {
            "metadata": [_metadata_archive_zip()],
            "plugin": [_plugin_archive_zip()],
        },
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        response = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/preview",
            headers=headers,
            json={"source": "official"},
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["plugin_id"] == "shinbot_plugin_market_demo"
    assert payload["plugin_path"] == "plugins/shinbot_plugin_market_demo"
    assert calls == [
        ("metadata", ["plugins/*/metadata.json"]),
        ("plugin", ["plugins/shinbot_plugin_market_demo/**"]),
    ]


def test_plugin_marketplace_blocks_unmanaged_existing_plugin(tmp_path: Path, monkeypatch):
    plugin_root = tmp_path / "plugins" / "shinbot_plugin_market_demo"
    package_root = plugin_root / "shinbot_plugin_market_demo"
    package_root.mkdir(parents=True)
    (plugin_root / "metadata.json").write_text(
        json.dumps(
            {
                "id": "shinbot_plugin_market_demo",
                "name": "Local Demo",
                "version": "0.0.1",
                "role": "logic",
                "entry": "shinbot_plugin_market_demo/__init__.py",
            }
        ),
        encoding="utf-8",
    )
    (package_root / "__init__.py").write_text("def setup(plg):\n    pass\n", encoding="utf-8")
    _patch_sparse_archives(
        monkeypatch,
        {"metadata": [_metadata_archive_zip()], "plugin": []},
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        response = client.get("/api/v1/plugin-marketplace", headers=headers)

    assert response.status_code == 200
    demo = {
        item["plugin_id"]: item for item in response.json()["data"]["plugins"]
    }["shinbot_plugin_market_demo"]
    assert demo["installed"] is True
    assert demo["managed_by_webui"] is False
    assert demo["can_install"] is False
    assert demo["can_update"] is False
    assert any("not WebUI-managed" in warning for warning in demo["warnings"])


def test_plugin_marketplace_reports_update_available(tmp_path: Path, monkeypatch):
    _patch_sparse_archives(
        monkeypatch,
        {
            "metadata": [_metadata_archive_zip(version="0.1.0")],
            "plugin": [_plugin_archive_zip(version="0.1.0")],
        },
    )
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        install = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/install",
            headers=headers,
            json={"enable_after_install": True},
        )
    assert install.status_code == 200

    _patch_sparse_archives(
        monkeypatch,
        {"metadata": [_metadata_archive_zip(version="0.2.0")], "plugin": []},
    )
    with client:
        listed = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"refresh": "true"},
        )

    demo = {
        item["plugin_id"]: item for item in listed.json()["data"]["plugins"]
    }["shinbot_plugin_market_demo"]
    assert demo["installed"] is True
    assert demo["installed_version"] == "0.1.0"
    assert demo["update_available"] is True
    assert demo["can_update"] is True


def test_plugin_marketplace_get_unknown_plugin_returns_404(tmp_path: Path, monkeypatch):
    _patch_sparse_archives(
        monkeypatch,
        {"metadata": [_metadata_archive_zip()], "plugin": []},
    )
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        response = client.get("/api/v1/plugin-marketplace/shinbot_plugin_missing", headers=headers)

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "PLUGIN_MARKETPLACE_ITEM_NOT_FOUND"


def test_plugin_marketplace_unknown_source_returns_404(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)

    with client:
        response = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "unknown"},
        )

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "PLUGIN_MARKETPLACE_SOURCE_NOT_FOUND"
