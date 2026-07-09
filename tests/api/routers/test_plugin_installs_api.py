from __future__ import annotations

import importlib
import io
import json
import shutil
import stat
import sys
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient

import shinbot.admin.plugin_install as plugin_install
import shinbot.admin.plugin_marketplace as plugin_marketplace
import shinbot.core.plugins.dependencies as plugin_dependencies
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


def _plugin_zip(
    plugin_id: str = "shinbot_plugin_install_demo",
    *,
    version: str = "0.1.0",
    required_dependencies: list[str] | None = None,
    optional_dependencies: list[str] | None = None,
    dependencies: list[str] | None = None,
    pyproject_dependencies: list[str] | None = None,
    command: str = "install-demo",
    setup_body: str | None = None,
) -> bytes:
    metadata = {
        "id": plugin_id,
        "name": "Install Demo",
        "version": version,
        "description": "Demo plugin",
        "author": "Tests",
        "role": "logic",
        "entry": "__init__.py",
        "permissions": [],
        "required_dependencies": required_dependencies or [],
        "optional_dependencies": optional_dependencies or [],
        "dependencies": dependencies or [],
    }
    if setup_body is None:
        setup_body = "\n".join(
            [
                "def setup(plg):",
                f"    @plg.on_command({command!r})",
                "    async def demo(c, args):",
                "        return None",
                "",
            ]
        )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(f"{plugin_id}/metadata.json", json.dumps(metadata))
        archive.writestr(f"{plugin_id}/__init__.py", setup_body)
        if pyproject_dependencies is not None:
            dependency_lines = "\n".join(
                f'    "{dependency}",' for dependency in pyproject_dependencies
            )
            archive.writestr(
                f"{plugin_id}/pyproject.toml",
                "\n".join(
                    [
                        "[project]",
                        f'name = "{plugin_id.replace("_", "-")}"',
                        'version = "0.1.0"',
                        "dependencies = [",
                        dependency_lines,
                        "]",
                        "",
                    ]
                ),
            )
    return stream.getvalue()


def _monorepo_zip(*, version: str = "0.1.0", command: str = "market-demo") -> bytes:
    metadata = {
        "id": "shinbot_plugin_market_demo",
        "name": "Market Demo",
        "version": version,
        "description": "Demo plugin in a monorepo",
        "author": "Tests",
        "role": "logic",
        "entry": "shinbot_plugin_market_demo/__init__.py",
        "permissions": [],
    }
    other_metadata = {
        "id": "shinbot_plugin_other_demo",
        "name": "Other Demo",
        "version": "0.1.0",
        "role": "logic",
        "entry": "shinbot_plugin_other_demo/__init__.py",
        "permissions": [],
    }
    setup_body = "\n".join(
        [
            "def setup(plg):",
            f"    @plg.on_command({command!r})",
            "    async def demo(c, args):",
            "        return None",
            "",
        ]
    )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        root = "shinbot-plugins-main"
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_market_demo/metadata.json",
            json.dumps(metadata),
        )
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_market_demo/shinbot_plugin_market_demo/__init__.py",
            setup_body,
        )
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_other_demo/metadata.json",
            json.dumps(other_metadata),
        )
        archive.writestr(
            f"{root}/plugins/shinbot_plugin_other_demo/shinbot_plugin_other_demo/__init__.py",
            "def setup(plg):\n    pass\n",
        )
    return stream.getvalue()


def _unsafe_zip(name: str, content: str = "x") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(name, content)
    return stream.getvalue()


def _symlink_zip() -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        info = zipfile.ZipInfo("shinbot_plugin_link/link")
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        archive.writestr(info, "target")
    return stream.getvalue()


def _custom_plugin_zip(*, name: str = "cool_plugin", version: str = "1.0.0") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr(
            "cool-plugin-main/metadata.yaml",
            "\n".join(
                [
                    f"name: {name}",
                    f"version: {version}",
                    "desc: Custom plugin",
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


def _register_custom_installer(bot: ShinBot, boot: _BootStub, target_dir: Path) -> None:
    def validate_fn(plugin_path: Path) -> dict[str, object] | None:
        metadata_path = plugin_path / "metadata.yaml"
        if not metadata_path.is_file():
            return None
        raw = _parse_simple_yaml(metadata_path)
        return {
            "id": raw["name"],
            "name": raw["name"],
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

    service = plugin_marketplace.build_plugin_marketplace_service(bot, boot)
    service.register_installer(
        "astrbot",
        owner_plugin_id="shinbot_converter_astrbot",
        install_fn=install_fn,
        validate_fn=validate_fn,
        target_dir=target_dir,
    )


def test_archive_preview_validates_dependencies_without_installing(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive/preview",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(
                required_dependencies=["shinbot_plugin_missing_required"],
                optional_dependencies=["shinbot_plugin_missing_optional"],
                dependencies=["shinbot_plugin_legacy_soft"],
            ),
        )

    assert response.status_code == 200
    payload = response.json()["data"]
    assert payload["can_install"] is False
    assert payload["missing_required_dependencies"] == ["shinbot_plugin_missing_required"]
    assert payload["missing_optional_dependencies"] == ["shinbot_plugin_missing_optional"]
    assert any("Legacy dependency" in item for item in payload["warnings"])
    assert not (tmp_path / "plugins" / "shinbot_plugin_install_demo").exists()


def test_archive_install_rejects_missing_required_dependency(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(required_dependencies=["shinbot_plugin_missing_required"]),
        )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PLUGIN_INSTALL_REQUIRED_DEPENDENCY_MISSING"


def test_archive_preview_rejects_unsafe_zip_paths_and_symlinks(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        traversal = client.post(
            "/api/v1/plugin-installs/archive/preview",
            headers={**headers, "Content-Type": "application/zip"},
            content=_unsafe_zip("../escape.txt"),
        )
        symlink = client.post(
            "/api/v1/plugin-installs/archive/preview",
            headers={**headers, "Content-Type": "application/zip"},
            content=_symlink_zip(),
        )

    assert traversal.status_code == 409
    assert traversal.json()["error"]["code"] == "PLUGIN_INSTALL_ARCHIVE_INVALID"
    assert symlink.status_code == 409
    assert symlink.json()["error"]["code"] == "PLUGIN_INSTALL_ARCHIVE_INVALID"


def test_archive_install_loads_plugin_and_writes_manifest(tmp_path: Path):
    client, bot, boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive?filename=demo.zip",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(),
        )
        task_id = response.json()["data"]["task_id"]
        task_response = client.get(f"/api/v1/plugin-installs/tasks/{task_id}", headers=headers)
        sources_response = client.get("/api/v1/plugin-installs", headers=headers)
        plugins_response = client.get("/api/v1/plugins", headers=headers)

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "succeeded"
    assert task_response.json()["data"]["status"] == "succeeded"
    assert bot.plugin_manager.get_plugin("shinbot_plugin_install_demo") is not None
    assert boot.config["plugins"] == [
        {"id": "shinbot_plugin_install_demo", "enabled": True, "config": {}}
    ]
    assert (tmp_path / "plugin_install_manifest.json").is_file()
    source = sources_response.json()["data"]["plugins"][0]
    assert source["plugin_id"] == "shinbot_plugin_install_demo"
    assert source["source_type"] == "archive"
    plugin_payload = plugins_response.json()["data"][0]
    assert plugin_payload["metadata"]["install_source"]["can_uninstall"] is True


def test_custom_archive_install_uses_registered_installer(tmp_path: Path):
    client, bot, boot, headers = _client(tmp_path)
    target_dir = tmp_path / "custom_plugins"
    _register_custom_installer(bot, boot, target_dir)

    with client:
        sources = client.get("/api/v1/plugin-installs", headers=headers)
        preview = client.post(
            "/api/v1/plugin-installs/archive/preview?filename=cool.zip&installer_type=astrbot",
            headers={**headers, "Content-Type": "application/zip"},
            content=_custom_plugin_zip(),
        )
        install = client.post(
            "/api/v1/plugin-installs/archive?filename=cool.zip&installer_type=astrbot",
            headers={**headers, "Content-Type": "application/zip"},
            content=_custom_plugin_zip(),
        )
        records = client.get("/api/v1/plugin-installs", headers=headers)

    assert sources.status_code == 200
    installers = {item["type"] for item in sources.json()["data"]["installers"]}
    assert installers == {"shinbot", "astrbot"}

    assert preview.status_code == 200
    preview_payload = preview.json()["data"]
    assert preview_payload["plugin_id"] == "cool_plugin"
    assert preview_payload["source_type"] == "marketplace"
    assert preview_payload["installer_type"] == "astrbot"
    assert preview_payload["can_install"] is True

    assert install.status_code == 200
    assert install.json()["data"]["status"] == "succeeded"
    assert (target_dir / "cool_plugin" / "main.py").is_file()

    source = records.json()["data"]["plugins"][0]
    assert source["plugin_id"] == "cool_plugin"
    assert source["source_type"] == "marketplace"
    assert source["source_url"] == "cool.zip"
    assert source["installer_type"] == "astrbot"


def test_archive_install_installs_pyproject_dependencies(tmp_path: Path, monkeypatch):
    captured: list[tuple[str, ...]] = []

    class _FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"installed", b""

    async def fake_create_subprocess_exec(*args, **kwargs):
        captured.append(tuple(str(arg) for arg in args))
        return _FakeProcess()

    monkeypatch.setattr(
        plugin_dependencies.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    client, bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive?filename=demo.zip",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(
                pyproject_dependencies=[
                    "cairosvg>=2.7.0",
                    "jinja2>=3.1.0",
                ],
            ),
        )

    assert response.status_code == 200
    assert bot.plugin_manager.get_plugin("shinbot_plugin_install_demo") is not None
    assert captured == [
        (
            sys.executable,
            "-m",
            "pip",
            "install",
            "cairosvg>=2.7.0",
            "jinja2>=3.1.0",
        )
    ]


def test_archive_install_falls_back_to_uv_when_pip_module_is_missing(
    tmp_path: Path,
    monkeypatch,
):
    captured: list[tuple[str, ...]] = []

    class _FakeProcess:
        def __init__(self, returncode: int, stderr: bytes = b"") -> None:
            self.returncode = returncode
            self.stderr = stderr

        async def communicate(self):
            return b"", self.stderr

    async def fake_create_subprocess_exec(*args, **kwargs):
        command = tuple(str(arg) for arg in args)
        captured.append(command)
        if command[:4] == (sys.executable, "-m", "pip", "install"):
            return _FakeProcess(1, b"No module named pip")
        return _FakeProcess(0)

    monkeypatch.setattr(
        plugin_dependencies.asyncio,
        "create_subprocess_exec",
        fake_create_subprocess_exec,
    )

    client, bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive?filename=demo.zip",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(pyproject_dependencies=["cairosvg>=2.7.0"]),
        )

    assert response.status_code == 200
    assert bot.plugin_manager.get_plugin("shinbot_plugin_install_demo") is not None
    assert captured == [
        (
            sys.executable,
            "-m",
            "pip",
            "install",
            "cairosvg>=2.7.0",
        ),
        (
            "uv",
            "pip",
            "install",
            "--python",
            sys.executable,
            "cairosvg>=2.7.0",
        ),
    ]


def test_github_install_downloads_archive_and_records_source(
    tmp_path: Path,
    monkeypatch,
):
    archive_bytes = _plugin_zip(plugin_id="shinbot_plugin_github_demo", command="github-demo")
    captured: dict[str, object] = {}

    class _FakeResponse:
        content = archive_bytes
        headers = {"x-github-request-id": "resolved-ref"}

        def raise_for_status(self) -> None:
            return None

    class _FakeAsyncClient:
        def __init__(self, **kwargs):
            captured["client_kwargs"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            captured["url"] = url
            return _FakeResponse()

    monkeypatch.setattr(plugin_install.httpx, "AsyncClient", _FakeAsyncClient)

    client, bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/github",
            headers=headers,
            json={
                "url": "https://github.com/NekyuuYa/shinbot-plugin-demo",
                "ref": "v0.1.0",
            },
        )
        sources_response = client.get("/api/v1/plugin-installs", headers=headers)

    assert response.status_code == 200
    assert bot.plugin_manager.get_plugin("shinbot_plugin_github_demo") is not None
    assert str(captured["url"]).endswith("/NekyuuYa/shinbot-plugin-demo/zip/v0.1.0")
    source = sources_response.json()["data"]["plugins"][0]
    assert source["source_type"] == "github"
    assert source["source_url"] == "https://github.com/NekyuuYa/shinbot-plugin-demo"
    assert source["ref"] == "v0.1.0"
    assert source["resolved_ref"] == ""

def test_custom_github_install_uses_registered_installer(
    tmp_path: Path,
    monkeypatch,
):
    downloads: list[tuple[str, str]] = []

    async def fake_download_github_zip_archive(self, source):
        downloads.append((source.repository_url, source.ref))
        return plugin_marketplace.PluginMarketplaceArchive(
            content=_custom_plugin_zip(version="2.0.0"),
            resolved_ref="custom-ref",
        )

    monkeypatch.setattr(
        plugin_marketplace.PluginMarketplaceService,
        "_download_github_zip_archive",
        fake_download_github_zip_archive,
    )

    client, bot, boot, headers = _client(tmp_path)
    target_dir = tmp_path / "custom_plugins"
    _register_custom_installer(bot, boot, target_dir)

    with client:
        response = client.post(
            "/api/v1/plugin-installs/github",
            headers=headers,
            json={
                "url": "https://github.com/example/cool-plugin",
                "ref": "v2.0.0",
                "installer_type": "astrbot",
            },
        )
        sources_response = client.get("/api/v1/plugin-installs", headers=headers)

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "succeeded"
    assert downloads == [("https://github.com/example/cool-plugin", "v2.0.0")]
    assert (target_dir / "cool_plugin" / "main.py").is_file()

    source = sources_response.json()["data"]["plugins"][0]
    assert source["plugin_id"] == "cool_plugin"
    assert source["source_type"] == "marketplace"
    assert source["source_url"] == "https://github.com/example/cool-plugin"
    assert source["ref"] == "v2.0.0"
    assert source["resolved_ref"] == "custom-ref"
    assert source["installed_version"] == "2.0.0"
    assert source["installer_type"] == "astrbot"


def test_github_managed_plugin_can_update_from_manifest_source(tmp_path: Path, monkeypatch):
    archive_versions = [
        _plugin_zip(
            plugin_id="shinbot_plugin_github_demo",
            version="0.1.0",
            command="github-demo",
        ),
        _plugin_zip(
            plugin_id="shinbot_plugin_github_demo",
            version="0.2.0",
            command="github-demo",
        ),
    ]
    calls: list[str] = []

    class _FakeResponse:
        def __init__(self, content: bytes, index: int) -> None:
            self.content = content
            self.headers = {"x-github-request-id": f"resolved-{index}"}

        def raise_for_status(self) -> None:
            return None

    class _FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            calls.append(url)
            index = len(calls) - 1
            return _FakeResponse(archive_versions[index], index)

    monkeypatch.setattr(plugin_install.httpx, "AsyncClient", _FakeAsyncClient)

    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        install = client.post(
            "/api/v1/plugin-installs/github",
            headers=headers,
            json={
                "url": "https://github.com/NekyuuYa/shinbot-plugin-demo",
                "ref": "main",
            },
        )
        update = client.post(
            "/api/v1/plugin-installs/shinbot_plugin_github_demo/update",
            headers=headers,
        )
        sources_response = client.get("/api/v1/plugin-installs", headers=headers)

    assert install.status_code == 200
    assert update.status_code == 200
    assert len(calls) == 2
    metadata = json.loads(
        (tmp_path / "plugins" / "shinbot_plugin_github_demo" / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["version"] == "0.2.0"
    source = sources_response.json()["data"]["plugins"][0]
    assert source["installed_version"] == "0.2.0"
    assert source["resolved_ref"] == ""


def test_github_install_can_select_plugin_path_from_monorepo(tmp_path: Path, monkeypatch):
    archive_versions = [
        _monorepo_zip(version="0.1.0", command="market-demo"),
        _monorepo_zip(version="0.1.0", command="market-demo"),
        _monorepo_zip(version="0.2.0", command="market-demo"),
    ]
    calls: list[str] = []

    class _FakeResponse:
        def __init__(self, content: bytes) -> None:
            self.content = content
            self.headers = {}

        def raise_for_status(self) -> None:
            return None

    class _FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            calls.append(url)
            return _FakeResponse(archive_versions[len(calls) - 1])

    monkeypatch.setattr(plugin_install.httpx, "AsyncClient", _FakeAsyncClient)

    client, bot, _boot, headers = _client(tmp_path)
    with client:
        preview = client.post(
            "/api/v1/plugin-installs/github/preview",
            headers=headers,
            json={
                "url": "https://github.com/NekyuuYa/shinbot-plugins",
                "ref": "main",
                "plugin_path": "plugins/shinbot_plugin_market_demo",
            },
        )
        install = client.post(
            "/api/v1/plugin-installs/github",
            headers=headers,
            json={
                "url": "https://github.com/NekyuuYa/shinbot-plugins",
                "ref": "main",
                "plugin_path": "plugins/shinbot_plugin_market_demo",
            },
        )
        update = client.post(
            "/api/v1/plugin-installs/shinbot_plugin_market_demo/update",
            headers=headers,
        )
        sources_response = client.get("/api/v1/plugin-installs", headers=headers)
        plugins_response = client.get("/api/v1/plugins", headers=headers)

    assert preview.status_code == 200
    assert preview.json()["data"]["plugin_path"] == "plugins/shinbot_plugin_market_demo"
    assert install.status_code == 200
    assert update.status_code == 200
    assert len(calls) == 3
    meta = bot.plugin_manager.get_plugin("shinbot_plugin_market_demo")
    assert meta is not None
    assert meta.module_path == "shinbot_plugin_market_demo"
    assert importlib.import_module("shinbot_plugin_market_demo") is not None
    assert bot.plugin_manager.get_plugin("shinbot_plugin_other_demo") is None
    metadata = json.loads(
        (tmp_path / "plugins" / "shinbot_plugin_market_demo" / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["version"] == "0.2.0"
    source = sources_response.json()["data"]["plugins"][0]
    assert source["plugin_id"] == "shinbot_plugin_market_demo"
    assert source["source_url"] == "https://github.com/NekyuuYa/shinbot-plugins"
    assert source["plugin_path"] == "plugins/shinbot_plugin_market_demo"
    assert source["installed_version"] == "0.2.0"
    plugin_payload = {
        item["id"]: item for item in plugins_response.json()["data"]
    }["shinbot_plugin_market_demo"]
    assert plugin_payload["metadata"]["source"] == "github"
    sys.modules.pop("shinbot_plugin_market_demo", None)


def test_github_preview_rejects_invalid_plugin_path(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/github/preview",
            headers=headers,
            json={
                "url": "https://github.com/NekyuuYa/shinbot-plugins",
                "ref": "main",
                "plugin_path": "../shinbot_plugin_market_demo",
            },
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "PLUGIN_INSTALL_INVALID_PLUGIN_PATH"


def test_archive_install_can_persist_disabled_state(tmp_path: Path):
    client, bot, boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive?enable_after_install=false",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(),
        )

    assert response.status_code == 200
    meta = bot.plugin_manager.get_plugin("shinbot_plugin_install_demo")
    assert meta is not None
    assert meta.state.value == "disabled"
    assert boot.config["plugins"] == [
        {"id": "shinbot_plugin_install_demo", "enabled": False, "config": {}}
    ]


def test_archive_preview_rejects_declared_oversized_body_before_reading(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive/preview",
            headers={
                **headers,
                "Content-Type": "application/zip",
                "Content-Length": str(plugin_install.PLUGIN_INSTALL_MAX_ARCHIVE_BYTES + 1),
            },
            content=b"",
        )

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "PLUGIN_INSTALL_ARCHIVE_INVALID"


def test_archive_install_refuses_unmanaged_overwrite(tmp_path: Path):
    existing = tmp_path / "plugins" / "shinbot_plugin_install_demo"
    existing.mkdir(parents=True)
    (existing / "metadata.json").write_text(
        json.dumps({"id": "shinbot_plugin_install_demo", "entry": "__init__.py"}),
        encoding="utf-8",
    )
    (existing / "__init__.py").write_text("def setup(plg):\n    pass\n", encoding="utf-8")

    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        response = client.post(
            "/api/v1/plugin-installs/archive?allow_overwrite=true",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(version="0.2.0"),
        )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "PLUGIN_INSTALL_TARGET_UNMANAGED"
    assert json.loads((existing / "metadata.json").read_text(encoding="utf-8"))["entry"] == "__init__.py"


def test_webui_managed_overwrite_rolls_back_when_new_plugin_fails_to_load(tmp_path: Path):
    client, bot, _boot, headers = _client(tmp_path)
    with client:
        first = client.post(
            "/api/v1/plugin-installs/archive",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(version="0.1.0"),
        )
        failing = client.post(
            "/api/v1/plugin-installs/archive?allow_overwrite=true",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(
                version="0.2.0",
                setup_body="def setup(plg):\n    raise RuntimeError('boom')\n",
            ),
        )

    assert first.status_code == 200
    assert failing.status_code == 500
    assert failing.json()["error"]["code"] == "PLUGIN_INSTALL_LOAD_FAILED"
    metadata = json.loads(
        (tmp_path / "plugins" / "shinbot_plugin_install_demo" / "metadata.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["version"] == "0.1.0"
    assert bot.plugin_manager.get_plugin("shinbot_plugin_install_demo") is not None


def test_failed_overwrite_restores_previous_enabled_config(tmp_path: Path):
    client, _bot, boot, headers = _client(tmp_path)
    with client:
        install = client.post(
            "/api/v1/plugin-installs/archive?enable_after_install=false",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(version="0.1.0"),
        )
        failing = client.post(
            "/api/v1/plugin-installs/archive?allow_overwrite=true&enable_after_install=true",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(
                version="0.2.0",
                setup_body="def setup(plg):\n    raise RuntimeError('boom')\n",
            ),
        )

    assert install.status_code == 200
    assert failing.status_code == 500
    assert boot.config["plugins"] == [
        {"id": "shinbot_plugin_install_demo", "enabled": False, "config": {}}
    ]


def test_webui_managed_plugin_can_be_uninstalled_without_plugin_data_loss(tmp_path: Path):
    client, _bot, _boot, headers = _client(tmp_path)
    with client:
        install = client.post(
            "/api/v1/plugin-installs/archive",
            headers={**headers, "Content-Type": "application/zip"},
            content=_plugin_zip(),
        )
        data_dir = tmp_path / "plugin_data" / "shinbot_plugin_install_demo"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "state.txt").write_text("keep", encoding="utf-8")
        uninstall = client.delete(
            "/api/v1/plugin-installs/shinbot_plugin_install_demo",
            headers=headers,
        )

    assert install.status_code == 200
    assert uninstall.status_code == 200
    assert not (tmp_path / "plugins" / "shinbot_plugin_install_demo").exists()
    assert (data_dir / "state.txt").read_text(encoding="utf-8") == "keep"
