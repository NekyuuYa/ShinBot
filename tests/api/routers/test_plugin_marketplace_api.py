from __future__ import annotations

import io
import json
import shutil
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


def _register_monorepo_source(bot: ShinBot, boot: _BootStub, source_id: str = "monorepo") -> None:
    service = plugin_marketplace.build_plugin_marketplace_service(bot, boot)
    service.register_source(
        source_id=source_id,
        name="Monorepo Test Source",
        source_type="github_monorepo",
        repository_url="https://github.com/NekyuuYa/shinbot-plugins",
        ref="main",
        plugin_root="plugins",
    )


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


def _custom_metadata_archive_zip(*, version: str = "1.0.0") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "repo/cool_plugin/metadata.yaml",
            "\n".join(
                [
                    "name: cool_plugin",
                    f"version: {version}",
                    "desc: Custom marketplace plugin",
                    "author: Tests",
                    "",
                ]
            ),
        )
    return stream.getvalue()


def _custom_plugin_archive_zip(*, version: str = "1.0.0") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "repo/cool_plugin/metadata.yaml",
            "\n".join(
                [
                    "name: cool_plugin",
                    f"version: {version}",
                    "desc: Custom marketplace plugin",
                    "author: Tests",
                    "",
                ]
            ),
        )
        archive.writestr("repo/cool_plugin/main.py", "# custom plugin\n")
    return stream.getvalue()


def _custom_plugin_repo_zip(*, version: str = "1.0.0") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "cool-plugin-main/metadata.yaml",
            "\n".join(
                [
                    "name: cool_plugin",
                    f"version: {version}",
                    "desc: Custom marketplace plugin",
                    "author: Tests",
                    "",
                ]
            ),
        )
        archive.writestr("cool-plugin-main/main.py", "# custom plugin\n")
    return stream.getvalue()


def _parse_simple_yaml(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        values[key.strip()] = value.strip()
    return values


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


def test_plugin_marketplace_unregisters_plugin_owned_sources(tmp_path: Path):
    client, bot, boot, headers = _client(tmp_path)
    service = plugin_marketplace.build_plugin_marketplace_service(bot, boot)

    async def install_fn(plugin_path: Path) -> bool:
        return plugin_path.exists()

    service.register_installer(
        "custom",
        owner_plugin_id="owner_plugin",
        install_fn=install_fn,
    )
    service.register_source(
        source_id="custom-source",
        name="Custom Source",
        repository_url="https://github.com/example/custom-source",
        installer_type="custom",
        owner_plugin_id="owner_plugin",
    )

    with client:
        listed = client.get("/api/v1/plugin-marketplace/sources", headers=headers)

    assert listed.status_code == 200
    assert {item["id"] for item in listed.json()["data"]["sources"]} == {
        "official",
        "custom-source",
    }

    service.unregister_owner("owner_plugin")

    with client:
        listed_after = client.get("/api/v1/plugin-marketplace/sources", headers=headers)

    assert listed_after.status_code == 200
    assert [item["id"] for item in listed_after.json()["data"]["sources"]] == ["official"]


def test_plugin_marketplace_custom_source_uses_registered_installer(
    tmp_path: Path,
    monkeypatch,
):
    expected_metadata_paths = ["*/metadata.json", "*/metadata.yaml", "*/metadata.yml"]
    calls: list[tuple[str, list[str]]] = []

    async def fake_create_sparse_archive(self, source, *, sparse_paths):
        if sparse_paths == expected_metadata_paths:
            calls.append(("metadata", sparse_paths))
            return plugin_marketplace.PluginMarketplaceArchive(
                content=_custom_metadata_archive_zip(),
                resolved_ref="custom-resolved",
            )
        calls.append(("plugin", sparse_paths))
        return plugin_marketplace.PluginMarketplaceArchive(
            content=_custom_plugin_archive_zip(),
            resolved_ref="custom-resolved",
        )

    monkeypatch.setattr(
        plugin_marketplace.PluginMarketplaceService,
        "_create_sparse_archive",
        fake_create_sparse_archive,
    )

    target_dir = tmp_path / "custom_plugins"
    install_calls: list[dict[str, object]] = []

    def validate_fn(plugin_path: Path) -> dict[str, object] | None:
        metadata_path = plugin_path / "metadata.yaml"
        if not metadata_path.is_file():
            return None
        raw = _parse_simple_yaml(metadata_path)
        return {
            "id": raw["name"],
            "name": "Cool Plugin",
            "version": raw["version"],
            "description": raw["desc"],
            "author": raw["author"],
        }

    async def install_fn(
        plugin_path: Path,
        *,
        target_dir: Path,
        source_info: dict[str, object] | None = None,
    ) -> bool:
        install_calls.append(source_info or {})
        target = target_dir / plugin_path.name
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(plugin_path, target)
        return True

    client, bot, boot, headers = _client(tmp_path)
    service = plugin_marketplace.build_plugin_marketplace_service(bot, boot)
    service.register_installer(
        "custom",
        owner_plugin_id="owner_plugin",
        install_fn=install_fn,
        validate_fn=validate_fn,
        target_dir=target_dir,
    )
    service.register_source(
        source_id="custom-source",
        name="Custom Source",
        repository_url="https://github.com/example/custom-source",
        plugin_root=".",
        installer_type="custom",
        owner_plugin_id="owner_plugin",
    )

    with client:
        listed = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "custom-source"},
        )
        install = client.post(
            "/api/v1/plugin-marketplace/cool_plugin/install",
            headers=headers,
            json={"source": "custom-source", "enable_after_install": True},
        )
        listed_after = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "custom-source"},
        )

    assert listed.status_code == 200
    first_item = listed.json()["data"]["plugins"][0]
    assert first_item["plugin_id"] == "cool_plugin"
    assert first_item["name"] == "Cool Plugin"
    assert first_item["plugin_path"] == "cool_plugin"
    assert first_item["installed"] is False

    assert install.status_code == 200
    assert install.json()["data"]["status"] == "succeeded"
    assert install.json()["data"]["plugin_id"] == "cool_plugin"
    assert (target_dir / "cool_plugin" / "main.py").is_file()
    assert install_calls[0]["source"]["id"] == "custom-source"

    installed_item = listed_after.json()["data"]["plugins"][0]
    assert installed_item["installed"] is True
    assert installed_item["managed_by_webui"] is True
    assert installed_item["installed_version"] == "1.0.0"
    assert installed_item["can_install"] is False
    assert installed_item["installed_source"]["source_type"] == "marketplace"
    assert installed_item["installed_source"]["installer_type"] == "custom"
    assert calls == [
        ("metadata", expected_metadata_paths),
        ("plugin", ["cool_plugin/**"]),
    ]


def test_plugin_marketplace_github_index_source_downloads_selected_repo(
    tmp_path: Path,
    monkeypatch,
):
    async def fail_sparse_archive(self, source, *, sparse_paths):
        raise AssertionError("github_index sources must not use sparse checkout")

    async def fake_download_github_file(self, source, index_path):
        assert source.repository_url == "https://github.com/example/plugin-index"
        assert index_path == "plugins.json"
        payload = {
            "cool_plugin": {
                "display_name": "Cool Plugin",
                "desc": "Custom marketplace plugin",
                "author": "Tests",
                "repo": "https://github.com/example/cool-plugin",
                "tags": ["custom"],
            }
        }
        return json.dumps(payload).encode("utf-8"), "index-sha"

    zip_downloads: list[tuple[str, str]] = []

    async def fake_download_github_zip_archive(self, source):
        zip_downloads.append((source.repository_url, source.ref))
        return plugin_marketplace.PluginMarketplaceArchive(
            content=_custom_plugin_repo_zip(),
            resolved_ref="plugin-sha",
        )

    monkeypatch.setattr(
        plugin_marketplace.PluginMarketplaceService,
        "_create_sparse_archive",
        fail_sparse_archive,
    )
    monkeypatch.setattr(
        plugin_marketplace.PluginMarketplaceService,
        "_download_github_file",
        fake_download_github_file,
    )
    monkeypatch.setattr(
        plugin_marketplace.PluginMarketplaceService,
        "_download_github_zip_archive",
        fake_download_github_zip_archive,
    )

    target_dir = tmp_path / "custom_plugins"

    def validate_fn(plugin_path: Path) -> dict[str, object] | None:
        metadata_path = plugin_path / "metadata.yaml"
        if not metadata_path.is_file():
            return None
        raw = _parse_simple_yaml(metadata_path)
        return {
            "id": raw["name"],
            "name": "Cool Plugin",
            "version": raw["version"],
            "description": raw["desc"],
            "author": raw["author"],
        }

    async def install_fn(plugin_path: Path, *, target_dir: Path) -> bool:
        metadata = _parse_simple_yaml(plugin_path / "metadata.yaml")
        target = target_dir / metadata["name"]
        if target.exists():
            shutil.rmtree(target)
        shutil.copytree(plugin_path, target)
        return True

    client, bot, boot, headers = _client(tmp_path)
    service = plugin_marketplace.build_plugin_marketplace_service(bot, boot)
    service.register_installer(
        "custom",
        owner_plugin_id="owner_plugin",
        install_fn=install_fn,
        validate_fn=validate_fn,
        target_dir=target_dir,
    )
    service.register_source(
        source_id="custom-index",
        name="Custom Index",
        source_type="github_index",
        repository_url="https://github.com/example/plugin-index",
        plugin_root="plugins.json",
        installer_type="custom",
        owner_plugin_id="owner_plugin",
    )

    with client:
        listed = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "custom-index"},
        )
        install = client.post(
            "/api/v1/plugin-marketplace/cool_plugin/install",
            headers=headers,
            json={"source": "custom-index", "enable_after_install": True},
        )
        listed_after = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "custom-index"},
        )

    assert listed.status_code == 200
    item = listed.json()["data"]["plugins"][0]
    assert item["plugin_id"] == "cool_plugin"
    assert item["repository"] == "https://github.com/example/cool-plugin"
    assert item["ref"] == "HEAD"
    assert item["plugin_path"] == ""

    assert install.status_code == 200
    assert install.json()["data"]["status"] == "succeeded"
    assert (target_dir / "cool_plugin" / "main.py").is_file()
    assert zip_downloads == [("https://github.com/example/cool-plugin", "HEAD")]

    installed_item = listed_after.json()["data"]["plugins"][0]
    assert installed_item["installed"] is True
    assert installed_item["installed_source"]["source_url"] == "https://github.com/example/cool-plugin"
    assert installed_item["installed_source"]["ref"] == "HEAD"
    assert installed_item["installed_source"]["marketplace_source_id"] == "custom-index"

def test_plugin_marketplace_lists_monorepo_plugins(tmp_path: Path, monkeypatch):
    calls = _patch_sparse_archives(
        monkeypatch,
        {"metadata": [_metadata_archive_zip()], "plugin": []},
    )
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        response = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["source"]["id"] == "monorepo"
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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        first = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )
        second = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )
        preview = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/preview",
            headers=headers,
            json={"source": "monorepo"},
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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        first = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )
        refreshed = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo", "refresh": "true"},
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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        response = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )

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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        install = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/install",
            headers=headers,
            json={"source": "monorepo", "enable_after_install": True},
        )
        listed = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )

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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        response = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/preview",
            headers=headers,
            json={"source": "monorepo"},
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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        response = client.get(
            "/api/v1/plugin-marketplace",
            headers=headers,
            params={"source": "monorepo"},
        )

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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)
    with client:
        install = client.post(
            "/api/v1/plugin-marketplace/shinbot_plugin_market_demo/install",
            headers=headers,
            json={"source": "monorepo", "enable_after_install": True},
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
            params={"source": "monorepo", "refresh": "true"},
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
    client, bot, boot, headers = _client(tmp_path)
    _register_monorepo_source(bot, boot)

    with client:
        response = client.get(
            "/api/v1/plugin-marketplace/shinbot_plugin_missing",
            headers=headers,
            params={"source": "monorepo"},
        )

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
