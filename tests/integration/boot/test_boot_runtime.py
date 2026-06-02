from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot import BootController, BootState
from shinbot.core.application.boot_preflight import BootPreflightError, run_boot_preflight
from shinbot.core.application.data_initializer import DataInitializer
from shinbot.core.application.runtime_control import RuntimeControl
from shinbot.core.plugins.types import PluginState
from tests.conftest import MockAdapter


def _write_config(path: Path, *, extra_config: str = "") -> None:
    path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                extra_config,
            ]
        ),
        encoding="utf-8",
    )


def _sqlite_tables(path: Path) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
    finally:
        conn.close()


def test_setup_instances_reads_normalized_adapter_instances(tmp_path: Path):
    boot = BootController(config_path=tmp_path / "config.toml", data_dir=tmp_path / "data")
    boot.config = {
        "adapter_instances": [
            {
                "id": "mock-main",
                "adapter": "mock",
                "enabled": True,
                "config": {},
            },
            {
                "id": "mock-disabled",
                "adapter": "mock",
                "enabled": False,
                "config": {},
            },
        ]
    }
    bot = ShinBot(data_dir=tmp_path / "data")
    bot.adapter_manager.register_adapter("mock", MockAdapter)
    boot.bot = bot

    boot._setup_instances()

    assert bot.adapter_manager.get_instance("mock-main") is not None
    assert bot.adapter_manager.get_instance("mock-disabled") is None


def test_data_initializer_creates_required_dirs_and_cleans_temp(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    stale_dir = data_dir / "temp" / "context_state"
    stale_dir.mkdir(parents=True)
    stale_file = stale_dir / "old.json"
    stale_file.write_text("{}", encoding="utf-8")

    result = DataInitializer(data_dir).initialize()

    assert (data_dir / "db").is_dir()
    assert (data_dir / "plugins").is_dir()
    assert (data_dir / "plugin_data").is_dir()
    assert (data_dir / "sessions").is_dir()
    assert (data_dir / "audit").is_dir()
    assert (data_dir / "agents").is_dir()
    assert (data_dir / "personas").is_dir()
    assert (data_dir / "personas" / "default.md").is_file()
    assert (data_dir / "models.json").is_file()
    assert (data_dir / "instance-configs.json").is_file()
    assert (data_dir / "temp").is_dir()
    assert not stale_dir.exists()
    assert result.cleaned_temp_entries == (stale_dir,)


def test_boot_preflight_reports_static_config_issues(tmp_path: Path) -> None:
    config = {
        "runtime": {"model": "yes"},
        "database": {"url": "postgresql://example/db"},
        "adapter_instances": [{"id": "main"}],
    }

    result = run_boot_preflight(
        config,
        data_dir=tmp_path / "data",
        raise_on_error=False,
    )

    assert {(issue.path, issue.code) for issue in result.issues} == {
        ("runtime.model", "type"),
        ("database.url", "database_url"),
        ("adapter_instances[0].adapter", "required"),
    }


def test_boot_phase1_rejects_invalid_admin_section_before_defaults(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text('admin = "bad"\n', encoding="utf-8")
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    with pytest.raises(BootPreflightError) as exc_info:
        boot._phase1_environment()

    assert [(issue.path, issue.code) for issue in exc_info.value.issues] == [
        ("admin", "type"),
    ]
    assert boot.state == BootState.DEGRADED


def test_boot_preflight_skips_agent_file_check_when_agent_runtime_disabled(
    tmp_path: Path,
) -> None:
    config = {
        "runtime": {"agent": False},
        "bots": [
            {
                "id": "full-agent",
                "enabled": True,
                "agent": {"mode": "full", "config": "agents/missing.toml"},
            }
        ],
    }

    result = run_boot_preflight(
        config,
        data_dir=tmp_path / "data",
        raise_on_error=False,
    )

    assert result.issues == ()


def test_boot_preflight_skips_agent_file_check_when_model_runtime_disabled(
    tmp_path: Path,
) -> None:
    config = {
        "runtime": {"model": False},
        "bots": [
            {
                "id": "full-agent",
                "enabled": True,
                "agent": {"mode": "full", "config": "agents/missing.toml"},
            }
        ],
    }

    result = run_boot_preflight(
        config,
        data_dir=tmp_path / "data",
        raise_on_error=False,
    )

    assert result.issues == ()


@pytest.mark.asyncio
async def test_boot_does_not_mount_model_or_agent_without_agent_bots(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path)
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is None
        assert bot.agent_runtime is None
        assert (tmp_path / "data" / "agents").is_dir()
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_clean_boot_initializes_file_configs_without_config_tables(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    data_dir = tmp_path / "data"
    _write_config(config_path)
    boot = BootController(config_path=config_path, data_dir=data_dir)

    try:
        bot = await boot.boot()

        assert boot.state == BootState.RUNNING
        assert bot.database is not None
        assert (data_dir / "models.json").is_file()
        assert (data_dir / "instance-configs.json").is_file()
        assert (data_dir / "personas" / "default.md").is_file()
        assert (data_dir / "prompts" / "custom").is_dir()
        assert json.loads((data_dir / "models.json").read_text(encoding="utf-8")) == {
            "version": 2,
            "providers": [],
            "models": [],
            "routes": [],
        }
        assert json.loads((data_dir / "instance-configs.json").read_text(encoding="utf-8")) == {
            "version": 1,
            "configs": [],
        }

        tables = _sqlite_tables(data_dir / "db" / "shinbot.sqlite3")
        assert {
            "model_providers",
            "model_definitions",
            "model_routes",
            "model_route_members",
            "bot_configs",
            "agents",
            "context_strategies",
            "personas",
            "prompt_definitions",
        }.isdisjoint(tables)
        assert {"sessions", "message_logs", "model_execution_records", "audit_logs"} <= tables
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_migrates_legacy_media_schema_before_startup_indexes(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    data_dir = tmp_path / "data"
    db_path = data_dir / "db" / "shinbot.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _write_config(config_path)

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE media_semantics (
                raw_hash TEXT PRIMARY KEY,
                kind TEXT NOT NULL DEFAULT '',
                digest TEXT NOT NULL DEFAULT '',
                verified_by_model INTEGER NOT NULL DEFAULT 0,
                inspection_agent_ref TEXT NOT NULL DEFAULT '',
                inspection_llm_ref TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                first_seen_at REAL NOT NULL,
                last_seen_at REAL NOT NULL,
                expire_at REAL NOT NULL
            );
            INSERT INTO media_semantics (
                raw_hash, kind, digest, first_seen_at, last_seen_at, expire_at
            ) VALUES ('raw-boot', 'image', 'boot legacy digest', 1, 2, 3);
            """
        )
        conn.commit()
    finally:
        conn.close()

    boot = BootController(config_path=config_path, data_dir=data_dir)

    try:
        bot = await boot.boot()
        assert boot.state == BootState.RUNNING
        assert bot.database is not None
        with bot.database.connect() as conn:
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(media_semantics)").fetchall()
            }
            row = conn.execute(
                "SELECT raw_hash, strict_dhash, digest FROM media_semantics"
            ).fetchone()
            index_row = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'index' AND name = 'idx_media_semantics_strict_dhash'
                """
            ).fetchone()

        assert "strict_dhash" in columns
        assert row["raw_hash"] == "raw-boot"
        assert row["strict_dhash"] == ""
        assert row["digest"] == "boot legacy digest"
        assert index_row is not None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_api_creation_does_not_mount_model_runtime_without_runtime_config(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path)
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        boot.create_api_app(RuntimeControl())

        assert bot.model_runtime is None
        assert bot.agent_runtime is None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_clean_boot_keeps_debug_plugins_disabled_by_default(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path)
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        states = {meta.id: meta.state for meta in bot.plugin_manager.all_plugins}

        assert states["shinbot_debug_message"] == PluginState.DISABLED
        assert states["shinbot_debug_model"] == PluginState.DISABLED
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_clean_boot_can_enable_debug_plugin_from_config(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(
        config_path,
        extra_config="\n".join(
            [
                "[[plugins]]",
                'id = "shinbot_debug_model"',
                'module = "shinbot.builtin_plugins.shinbot_debug_model"',
                "enabled = true",
            ]
        ),
    )
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        states = {meta.id: meta.state for meta in bot.plugin_manager.all_plugins}

        assert states["shinbot_debug_model"] == PluginState.ACTIVE
        assert states["shinbot_debug_message"] == PluginState.DISABLED
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_loads_agent_config_for_full_bot(tmp_path: Path):
    data_dir = tmp_path / "data"
    agent_config = data_dir / "agents" / "full-agent.toml"
    agent_config.parent.mkdir(parents=True)
    agent_config.write_text(
        "\n".join(
            [
                "[agent]",
                'id = "full-agent-profile"',
                "",
                "[agent.review]",
                "scan_batch_size = 9",
                "",
                "[agent.active_chat]",
                "initial_interest = 42",
            ]
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[[bots]]",
                'id = "full-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/full-agent.toml"',
            ]
        ),
        encoding="utf-8",
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)

    try:
        bot = await boot.boot()
        assert bot.model_runtime is not None
        assert bot.agent_runtime is not None
        assert bot.agent_runtime.model_runtime is bot.model_runtime
        profile = bot.agent_runtime.agent_profile_for_bot("full-agent")
        assert profile.profile_id == "full-agent-profile"
        assert profile.config.review_workflow_config.review_scan_batch_size == 9
        assert profile.config.active_chat_policy_config.initial_interest_value == 42
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_fails_when_full_bot_agent_config_is_missing(tmp_path: Path):
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[[bots]]",
                'id = "full-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/missing.toml"',
            ]
        ),
        encoding="utf-8",
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)

    with pytest.raises(BootPreflightError) as exc_info:
        await boot.boot()
    assert [(issue.path, issue.code) for issue in exc_info.value.issues] == [
        ("bots[0].agent.config", "not_found"),
    ]
    assert boot.state == BootState.DEGRADED


@pytest.mark.asyncio
async def test_boot_skips_full_agent_when_agent_runtime_disabled(tmp_path: Path):
    data_dir = tmp_path / "data"
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        "\n".join(
            [
                "[admin]",
                'username = "admin"',
                'password = "admin"',
                "jwt_expire_hours = 24",
                "",
                "[runtime]",
                "agent = false",
                "",
                "[[bots]]",
                'id = "full-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/missing.toml"',
            ]
        ),
        encoding="utf-8",
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)

    try:
        bot = await boot.boot()
        assert bot.model_runtime is None
        assert bot.agent_runtime is None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_can_mount_model_without_agent(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path, extra_config="[runtime]\nmodel = true\nagent = false")
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is not None
        assert bot.agent_runtime is None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_mounts_configured_model_backend(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(
        config_path,
        extra_config="\n".join(
            [
                "[runtime]",
                "model = true",
                "agent = false",
                "",
                "[runtime.model_backend]",
                'type = "openai_compatible"',
            ]
        ),
    )
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is not None
        assert bot.model_runtime._backend.name == "openai_compatible"
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_preregisters_plugin_model_runtime_extensions_before_mount(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    builtin_root = tmp_path / "empty_builtin_plugins"
    builtin_root.mkdir()
    monkeypatch.setattr("shinbot.core.plugins.manager._BUILTIN_PLUGINS_DIR", builtin_root)

    data_dir = tmp_path / "data"
    plugin_id = "demo_runtime_bootstrap"
    plugin_dir = data_dir / "plugins" / plugin_id
    plugin_dir.mkdir(parents=True)
    (data_dir / "plugins" / "__init__.py").write_text("", encoding="utf-8")
    (plugin_dir / "metadata.json").write_text(
        json.dumps(
            {
                "id": plugin_id,
                "name": "Runtime Bootstrap",
                "version": "1.0.0",
                "author": "test",
                "description": "",
                "entry": "__init__.py",
                "permissions": [],
            }
        ),
        encoding="utf-8",
    )
    (plugin_dir / "__init__.py").write_text(
        "\n".join(
            [
                "from shinbot.agent.services.model_runtime.backends.protocol import BackendRequestPlan",
                "from shinbot.agent.services.model_runtime.providers import ModelProviderDescriptor",
                "",
                "class DemoBootBackend:",
                '    name = "demo_boot_backend"',
                "",
                "    def plan_request(self, *, provider, model, call, timeout_override, operation):",
                "        return BackendRequestPlan(",
                "            operation=operation,",
                '            payload={"model": model["backend_model"]},',
                '            safe_payload={"model": model["backend_model"]},',
                "            backend_name=self.name,",
                '            backend_model=str(model["backend_model"]),',
                "        )",
                "",
                "    def invoke(self, plan):",
                '        return {"choices": [{"message": {"content": "boot"}}], "usage": {}}',
                "",
                "    def normalize_response(self, *, operation, response, usage):",
                '        return {"text": "boot", "tool_calls": []}',
                "",
                "def register_model_runtime_extensions(registrar):",
                '    registrar.register_backend_factory("demo_boot_backend", DemoBootBackend)',
                "    registrar.register_provider_descriptor(",
                "        ModelProviderDescriptor(",
                '            provider_type="demo_boot_provider",',
                '            supported_backends=frozenset({"demo_boot_backend"}),',
                '            auth_strategy="none",',
                '            catalog_path=None,',
                "        )",
                "    )",
                "",
                "def setup(plg):",
                "    pass",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "config.toml"
    _write_config(
        config_path,
        extra_config="\n".join(
            [
                "[runtime]",
                "model = true",
                "agent = false",
                "",
                "[runtime.model_backend]",
                'type = "demo_boot_backend"',
            ]
        ),
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)

    try:
        bot = await boot.boot()
        assert bot.model_runtime is not None
        assert bot.model_runtime._backend.name == "demo_boot_backend"
        from shinbot.agent.services.model_runtime.providers import require_provider_descriptor

        descriptor = require_provider_descriptor("demo_boot_provider")
        assert descriptor.supports_backend("demo_boot_backend")
    finally:
        await boot.shutdown()
        sys.modules.pop(f"plugins.{plugin_id}", None)
        sys.modules.pop("plugins", None)


@pytest.mark.asyncio
async def test_boot_can_disable_model_when_agent_is_disabled(tmp_path: Path):
    config_path = tmp_path / "config.toml"
    _write_config(config_path, extra_config="[runtime]\nmodel = false\nagent = false")
    boot = BootController(config_path=config_path, data_dir=tmp_path / "data")

    try:
        bot = await boot.boot()
        assert bot.model_runtime is None
        assert bot.agent_runtime is None
    finally:
        await boot.shutdown()


@pytest.mark.asyncio
async def test_boot_shutdown_closes_agent_runtime(tmp_path: Path):
    data_dir = tmp_path / "data"
    agent_config = data_dir / "agents" / "full-agent.toml"
    agent_config.parent.mkdir(parents=True)
    agent_config.write_text("[agent]\nid = \"full-agent-profile\"\n", encoding="utf-8")

    config_path = tmp_path / "config.toml"
    _write_config(
        config_path,
        extra_config="\n".join(
            [
                "[[bots]]",
                'id = "full-agent"',
                "enabled = true",
                "",
                "[bots.agent]",
                'mode = "full"',
                'config = "agents/full-agent.toml"',
            ]
        ),
    )
    boot = BootController(config_path=config_path, data_dir=data_dir)
    bot = await boot.boot()
    assert bot.agent_runtime is not None

    closed = False
    original_shutdown = bot.agent_runtime.shutdown

    async def shutdown_probe() -> None:
        nonlocal closed
        closed = True
        await original_shutdown()

    bot.agent_runtime.shutdown = shutdown_probe
    await boot.shutdown()

    assert closed is True
