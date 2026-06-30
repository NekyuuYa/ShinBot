"""System boot controller implementing the 5-phase startup lifecycle."""

from __future__ import annotations

import secrets
import sys
import tomllib
from enum import Enum
from pathlib import Path
from typing import Any

from shinbot.admin.command_admin import apply_command_enabled_overrides
from shinbot.core.application.app import ShinBot
from shinbot.core.application.boot_preflight import (
    BootPreflightError,
    run_boot_preflight,
)
from shinbot.core.application.bot_permissions import apply_bot_admin_bindings
from shinbot.core.application.bots_config import BotServiceConfig
from shinbot.core.application.config_sections import (
    iter_adapter_instance_records,
    normalize_adapter_instance_record,
)
from shinbot.core.application.data_initializer import DataInitializer
from shinbot.core.application.paths import resolve_project_path
from shinbot.core.application.provider_config_validation import (
    ProviderConfigValidationError,
    validate_adapter_instance_configs,
    validate_plugin_configs,
)
from shinbot.core.application.runtime_control import RuntimeControl
from shinbot.core.plugins.config import plugin_saved_enabled
from shinbot.core.plugins.types import PluginState
from shinbot.core.security.permission_service import (
    BUILTIN_GROUP_IDS,
    RUNTIME_MANAGED_BINDING_GROUP_IDS,
)
from shinbot.core.security.permission_toml import (
    bindings_from_config,
    command_overrides_from_config,
    groups_from_config,
)
from shinbot.utils.log_file import parse_file_log_config
from shinbot.utils.logger import DEFAULT_THIRD_PARTY_NOISE_POLICY, get_logger, setup_logging

logger = get_logger(__name__, source="boot", color="cyan")


class BootState(Enum):
    UNINITIALIZED = "UNINITIALIZED"
    BOOTING = "BOOTING"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"


class BootController:
    """Coordinates deterministic boot/shutdown lifecycle."""

    def __init__(
        self,
        *,
        config_path: Path | str,
        data_dir: Path | str = "data",
        log_level: str = "INFO",
    ) -> None:
        self.config_path = Path(config_path)
        self.data_dir = Path(data_dir)
        self.log_level = log_level
        self.state = BootState.UNINITIALIZED
        self.config: dict[str, Any] = {}
        self.bot_service_configs: tuple[BotServiceConfig, ...] = ()
        self.bot: ShinBot | None = None
        self.dashboard_dist_dir: Path | None = None
        self.dashboard_index_file: Path | None = None
        self.dashboard_build_service: Any | None = None
        self.framework_update_service: Any | None = None

    async def boot(self) -> ShinBot:
        """Execute phase 1-5 startup sequence."""
        self.state = BootState.BOOTING

        self._phase1_environment()
        await self._phase2_infrastructure()
        self._phase3_kernel_load()
        await self._phase4_plugin_loading()
        await self._phase5_adapter_activation()

        self.state = BootState.RUNNING
        assert self.bot is not None
        return self.bot

    async def shutdown(self) -> None:
        """Shutdown in reverse order of startup."""
        if self.bot is None:
            return

        self.state = BootState.STOPPING

        # 1) Stop adapters first to stop incoming traffic.
        await self.bot.adapter_manager.shutdown_all()

        # 2) Shut down Agent-owned timers and background tasks.
        if self.bot.agent_runtime is not None:
            shutdown = getattr(self.bot.agent_runtime, "shutdown", None)
            if shutdown is not None:
                await shutdown()

        # 3) Notify plugins and free plugin resources.
        await self.bot.plugin_manager.unload_all_plugins_async()

        # 3b) Shut down the plugin cron scheduler.
        if self.bot.cron_manager is not None:
            self.bot.cron_manager.shutdown()

        # 4) Persist session state.
        for session in self.bot.session_manager.all_sessions:
            self.bot.session_manager.update(session)

        # 5) Infrastructure teardown placeholder (DB pool not present yet).
        self.state = BootState.UNINITIALIZED

    def _phase1_environment(self) -> None:
        self._configure_logging(self.log_level)
        logger.info("Boot Phase 1/5: environment")

        self.config = self._load_config(self.config_path)
        try:
            preflight = run_boot_preflight(self.config, data_dir=self.data_dir)
        except BootPreflightError:
            self.state = BootState.DEGRADED
            raise
        logging_cfg = self.config.get("logging", {})
        cfg_level = logging_cfg.get("level", self.log_level)
        third_party_noise = logging_cfg.get("third_party_noise", DEFAULT_THIRD_PARTY_NOISE_POLICY)
        self._configure_logging(
            cfg_level,
            third_party_noise=third_party_noise,
            file_config_raw=logging_cfg.get("file"),
        )
        self.bot_service_configs = preflight.bot_service_configs
        self._ensure_admin_defaults()
        DataInitializer(self.data_dir).initialize()

    async def _phase2_infrastructure(self) -> None:
        logger.info("Boot Phase 2/5: infrastructure")
        self._init_dashboard_static_config()
        try:
            self.bot = self._create_core_application()
            self.bot.plugin_manager._boot = self
            await self._preregister_model_runtime_extensions()
            self._mount_model_runtime()
            self._mount_agent_runtime()
        except Exception:
            self.state = BootState.DEGRADED
            raise

    def _create_core_application(self) -> ShinBot:
        db_cfg = self.config.get("database", {})
        database_url = db_cfg.get("url")
        snapshot_ttl = db_cfg.get("snapshot_ttl")
        bot = ShinBot(
            data_dir=self.data_dir,
            database_url=database_url,
            database_snapshot_ttl=snapshot_ttl,
        )
        bot.configure_bot_service_configs(self.bot_service_configs)
        return bot

    async def _preregister_model_runtime_extensions(self) -> None:
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        user_plugins_dir = self.data_dir / "plugins"
        await self.bot.plugin_manager.preregister_model_runtime_extensions(user_plugins_dir)

    def _mount_model_runtime(self) -> None:
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        if not self._should_mount_model_runtime():
            logger.info("Model runtime not requested by current config")
            return
        if not self._runtime_feature_enabled("model", default=True):
            logger.info("Model runtime disabled by [runtime].model=false")
            return

        from shinbot.core.runtime import install_model_runtime

        self.bot.config = self.config
        install_model_runtime(self.bot)

    def _mount_agent_runtime(self) -> None:
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        if not self._agent_runtime_requested_by_bots():
            logger.info("Agent runtime not requested by any enabled bot")
            return
        if not self._runtime_feature_enabled("agent", default=True):
            logger.info("Agent runtime disabled by [runtime].agent=false")
            return
        if not self._runtime_feature_enabled("model", default=True):
            logger.info("Agent runtime disabled because [runtime].model=false")
            return

        if self.bot.model_runtime is None:
            logger.info("Mounting model runtime because Agent runtime depends on it")
            from shinbot.core.runtime import install_model_runtime

            self.bot.config = self.config
            install_model_runtime(self.bot)

        from shinbot.agent.runtime import install_agent_runtime

        install_agent_runtime(
            self.bot,
            agent_configs_by_bot_id=self._load_agent_runtime_configs_by_bot_id(),
        )

    def _should_mount_model_runtime(self) -> bool:
        if self._runtime_feature_enabled("model", default=False):
            return True
        return self._agent_runtime_requested_by_bots() and self._runtime_feature_enabled(
            "agent", default=True
        )

    def _agent_runtime_requested_by_bots(self) -> bool:
        return any(
            bot_config.enabled and bot_config.agent.mode != "none"
            for bot_config in self.bot_service_configs
        )

    def _load_agent_runtime_configs_by_bot_id(self) -> dict[str, Any]:
        from shinbot.agent.runtime.config import (
            AgentRuntimeConfigError,
            load_agent_runtime_config,
        )

        configs: dict[str, Any] = {}
        for bot_config in self.bot_service_configs:
            if not bot_config.enabled:
                continue
            if bot_config.agent.mode == "none" or not bot_config.agent.config:
                continue

            config_path = self.data_dir / bot_config.agent.config
            try:
                configs[bot_config.id] = load_agent_runtime_config(
                    config_path,
                    data_dir=self.data_dir,
                )
            except AgentRuntimeConfigError as exc:
                raise AgentRuntimeConfigError(
                    f"Invalid agent config for bot {bot_config.id!r}: {exc}"
                ) from exc
        return configs

    def _runtime_feature_enabled(self, name: str, *, default: bool) -> bool:
        section = self.config.get("runtime", {})
        if section is None:
            return default
        if not isinstance(section, dict):
            logger.warning("[runtime] must be a table; got %s", type(section).__name__)
            return default

        value = section.get(name, default)
        if isinstance(value, bool):
            return value
        logger.warning(
            "runtime.%s must be a boolean; got %r (using default %s)",
            name,
            value,
            default,
        )
        return default

    def _init_dashboard_static_config(self) -> None:
        """Resolve and cache dashboard dist/index paths during infrastructure phase."""
        admin_cfg = self.config.setdefault("admin", {})

        configured_dist = admin_cfg.get("dashboard_dist")
        candidates: list[Path] = []

        if isinstance(configured_dist, str) and configured_dist.strip():
            configured_path = Path(configured_dist)
            if not configured_path.is_absolute():
                configured_path = resolve_project_path(configured_path, config_path=self.config_path)
            candidates.append(configured_path)

        candidates.extend(
            [
                resolve_project_path("dashboard/dist", config_path=self.config_path),
                (Path(__file__).resolve().parents[2] / "dashboard" / "dist").resolve(),
                (Path.cwd() / "dashboard" / "dist").resolve(),
            ]
        )

        resolved_dist = next((candidate for candidate in candidates if candidate.is_dir()), None)
        if resolved_dist is None:
            self.dashboard_dist_dir = None
            self.dashboard_index_file = None
            logger.warning("Dashboard dist directory not found; WebUI static hosting disabled")
            return

        index_file = resolved_dist / "index.html"
        if not index_file.is_file():
            self.dashboard_dist_dir = None
            self.dashboard_index_file = None
            logger.warning(
                "Dashboard dist found at %s but index.html is missing; WebUI static hosting disabled",
                resolved_dist,
            )
            return

        self.dashboard_dist_dir = resolved_dist
        self.dashboard_index_file = index_file
        admin_cfg.setdefault("dashboard_dist", str(resolved_dist))
        logger.info("Dashboard static files configured from %s", resolved_dist)

    def _phase3_kernel_load(self) -> None:
        logger.info("Boot Phase 3/5: kernel load")
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        required = [
            self.bot.message_ingress,
            self.bot.route_table,
            self.bot.route_targets,
            self.bot.command_registry,
            self.bot.keyword_registry,
            self.bot.permission_engine,
            self.bot.session_manager,
        ]
        if any(item is None for item in required):
            self.state = BootState.DEGRADED
            raise RuntimeError("Kernel components are not fully initialized")

    async def _phase4_plugin_loading(self) -> None:
        logger.info("Boot Phase 4/5: plugin loading")
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        user_plugins_dir = self.data_dir / "plugins"
        try:
            await self.bot.plugin_manager.load_all_async(user_plugins_dir)
        except Exception:
            logger.exception("Failed loading plugins")

        configured_plugins = self.config.get("plugins", [])
        if configured_plugins is None:
            configured_plugins = []
        if not isinstance(configured_plugins, list):
            configured_plugins = []

        for plugin_cfg in configured_plugins:
            if not isinstance(plugin_cfg, dict):
                logger.warning("Invalid plugin config entry: expected table, got %s", type(plugin_cfg).__name__)
                continue
            plugin_id = plugin_cfg.get("id")
            module_path = plugin_cfg.get("module")
            if not plugin_id:
                logger.warning("Invalid plugin config entry: missing id")
                continue
            plugin_id = str(plugin_id)
            if self._configured_plugin_id_is_already_loaded(plugin_id):
                continue
            if not module_path:
                logger.warning("Plugin %r is configured but not loaded and has no module path", plugin_id)
                continue
            module_path = str(module_path)
            if self._configured_plugin_module_is_already_loaded(module_path):
                continue
            try:
                await self.bot.load_plugin_async(plugin_id, module_path)
            except Exception:
                logger.exception("Failed to load plugin %s from %s", plugin_id, module_path)

        self._validate_plugin_provider_configs()
        await self._apply_plugin_state_overrides()
        apply_command_enabled_overrides(self.bot.command_registry, self.config)

    async def _apply_plugin_state_overrides(self) -> None:
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        for meta in list(self.bot.plugin_manager.all_plugins):
            enabled = self._configured_plugin_enabled(meta.id)
            if enabled is None:
                continue

            try:
                if enabled and meta.state == PluginState.DISABLED:
                    await self.bot.plugin_manager.enable_plugin_async(meta.id)
                elif not enabled and meta.state != PluginState.DISABLED:
                    await self.bot.plugin_manager.disable_plugin_async(meta.id)
            except Exception:
                logger.exception("Failed to apply persisted state for plugin %s", meta.id)

    def _configured_plugin_id_is_already_loaded(self, plugin_id: str) -> bool:
        if self.bot is None:
            return False
        for meta in self.bot.plugin_manager.all_plugins:
            if meta.id == plugin_id:
                return True
        return False

    def _configured_plugin_module_is_already_loaded(self, module_path: str) -> bool:
        if self.bot is None:
            return False
        for meta in self.bot.plugin_manager.all_plugins:
            if meta.module_path == module_path:
                return True
        return False

    def _configured_plugin_enabled(self, plugin_id: str) -> bool | None:
        return plugin_saved_enabled(self, plugin_id)

    async def _phase5_adapter_activation(self) -> None:
        logger.info("Boot Phase 5/5: adapter activation")
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        self._setup_permissions()
        self._setup_instances()
        await self.bot.start()

    def _setup_instances(self) -> None:
        assert self.bot is not None
        self._validate_adapter_provider_configs()
        instances = iter_adapter_instance_records(self.config)
        if not instances:
            logger.warning("No instances configured - bot will start with no connections")
            return

        for inst_cfg in instances:
            normalized = normalize_adapter_instance_record(inst_cfg)
            if not normalized["enabled"]:
                logger.info("Skipping disabled adapter instance %r", normalized["id"])
                continue

            instance_id = normalized["id"]
            platform = normalized["adapter"]
            config_kwargs = normalized["config"]

            try:
                self.bot.add_adapter(
                    instance_id=instance_id,
                    platform=platform,
                    **config_kwargs,
                )
            except ValueError:
                logger.warning(
                    "No adapter registered for platform %r (instance %r), skipping",
                    platform,
                    instance_id,
                )
                continue
            logger.info("Configured instance %r (platform=%s)", instance_id, platform)

    def _validate_plugin_provider_configs(self) -> None:
        assert self.bot is not None
        issues = validate_plugin_configs(self.config, self.bot.config_provider_registry)
        if issues:
            raise ProviderConfigValidationError(issues)

    def _validate_adapter_provider_configs(self) -> None:
        assert self.bot is not None
        issues = validate_adapter_instance_configs(self.config, self.bot.config_provider_registry)
        if issues:
            raise ProviderConfigValidationError(issues)

    def _setup_permissions(self) -> None:
        assert self.bot is not None
        raw_permissions = self.config.get("permissions", {})
        if raw_permissions and not isinstance(raw_permissions, dict):
            logger.warning("Invalid permissions config: expected table")

        self._load_permission_groups()
        self._load_permission_bindings()
        apply_bot_admin_bindings(self.bot.permission_engine, self.bot_service_configs)
        self._apply_command_permission_overrides()

    def _load_permission_groups(self) -> None:
        assert self.bot is not None
        for loaded_group in groups_from_config(self.config):
            existing = self.bot.permission_engine.get_group(loaded_group.id)
            if existing is not None:
                group = existing.model_copy(deep=True)
                if loaded_group.name:
                    group.name = loaded_group.name
                configured_permissions = set(loaded_group.permissions)
                if loaded_group.id in BUILTIN_GROUP_IDS:
                    negative_permissions = {
                        permission
                        for permission in configured_permissions
                        if permission.startswith("-")
                    }
                    if negative_permissions:
                        logger.warning(
                            "Ignoring negative permissions for built-in group %r: %r",
                            loaded_group.id,
                            sorted(negative_permissions),
                        )
                    configured_permissions -= negative_permissions
                group.permissions = set(group.permissions) | configured_permissions
            else:
                group = loaded_group
            self.bot.permission_engine.add_group(group)
            logger.info("Loaded permission group %r", group.id)

    def _load_permission_bindings(self) -> None:
        assert self.bot is not None
        for binding in bindings_from_config(self.config):
            valid_group_ids: list[str] = []
            for group_id in binding.groups:
                if group_id in RUNTIME_MANAGED_BINDING_GROUP_IDS:
                    logger.warning(
                        "Ignoring runtime-managed permission group %r in binding %r; "
                        "membership is derived from the current event context",
                        group_id,
                        binding.key,
                    )
                    continue
                if self.bot.permission_engine.get_group(group_id) is None:
                    logger.warning(
                        "Permission binding %r references unknown group %r",
                        binding.key,
                        group_id,
                    )
                    continue
                valid_group_ids.append(group_id)
            if not valid_group_ids:
                logger.warning("Permission binding %r has no valid groups", binding.key)
                continue
            try:
                setter = getattr(self.bot.permission_engine, "set_groups_for_key", None)
                if callable(setter):
                    setter(binding.key, valid_group_ids)
                else:
                    self.bot.permission_engine.bind(binding.key, valid_group_ids[0])
                logger.info("Bound %r -> permission groups %r", binding.key, valid_group_ids)
            except ValueError as exc:
                logger.warning("Permission binding error: %s", exc)

    def _apply_command_permission_overrides(self) -> None:
        assert self.bot is not None
        for override in command_overrides_from_config(self.config):
            setter = getattr(self.bot.command_registry, "set_permission_override", None)
            if callable(setter):
                if setter(override.command, override.permission) is None:
                    logger.warning(
                        "Command permission override references unknown command %r",
                        override.command,
                    )
                logger.info("Applied permission override for command %r", override.command)
                continue

            cmd = self.bot.command_registry.get(override.command)
            if cmd is None:
                logger.warning(
                    "Command permission override references unknown command %r",
                    override.command,
                )
                continue
            cmd.permission = override.permission
            logger.info("Applied permission override for command %r", override.command)

    def _configure_logging(
        self,
        level_name: str = "INFO",
        *,
        third_party_noise: str = DEFAULT_THIRD_PARTY_NOISE_POLICY,
        file_config_raw: Any = None,
    ) -> None:
        setup_logging(
            level_name,
            third_party_noise=third_party_noise,
            file_config=parse_file_log_config(file_config_raw),
            data_dir=self.data_dir,
        )

    def _load_config(self, config_path: Path) -> dict[str, Any]:
        if not config_path.exists():
            logger.warning("Config file %s not found, using defaults", config_path)
            return {}
        with config_path.open("rb") as file_obj:
            return tomllib.load(file_obj)

    def _ensure_admin_defaults(self) -> None:
        """Ensure [admin] config always exists with secure credentials.

        On first run (no password configured), a cryptographically random
        password is generated and printed to the terminal so the operator
        can log in and change it via the Dashboard.  The generated password
        is persisted to the configured main config file and is NOT the well-known "admin/admin"
        default, so the system is safe from the moment it starts.
        """
        admin_cfg = self.config.get("admin")
        if not isinstance(admin_cfg, dict):
            admin_cfg = {}

        changed = False
        if not admin_cfg.get("username"):
            admin_cfg["username"] = "admin"
            changed = True
        if not admin_cfg.get("password"):
            generated = secrets.token_urlsafe(16)
            admin_cfg["password"] = generated
            changed = True
            # Print the generated password prominently so the operator can
            # copy it. Use stderr so the credential block preserves ordering
            # with boot logs and still appears if stdout is redirected.
            border = "─" * 54
            print(f"\n┌{border}┐", file=sys.stderr, flush=True)
            print(f"│{'ShinBot — First-Run Credentials':^54}│", file=sys.stderr, flush=True)
            print(f"├{border}┤", file=sys.stderr, flush=True)
            print(f"│  Username : {'admin':<42}│", file=sys.stderr, flush=True)
            print(f"│  Password : {generated:<42}│", file=sys.stderr, flush=True)
            print(f"├{border}┤", file=sys.stderr, flush=True)
            print("│  Log in and change these credentials before       │", file=sys.stderr, flush=True)
            print("│  exposing this server to a network.               │", file=sys.stderr, flush=True)
            print(f"└{border}┘\n", file=sys.stderr, flush=True)
        if "jwt_expire_hours" not in admin_cfg:
            admin_cfg["jwt_expire_hours"] = 24
            changed = True

        _WEAK_PASSWORDS = {"admin", "password", "123456", "admin123"}
        if admin_cfg.get("password") in _WEAK_PASSWORDS:
            logger.warning(
                "Weak or default admin password detected. Please change your password immediately."
            )

        self.config["admin"] = admin_cfg

        if changed:
            saved = self.save_config()
            if not saved:
                logger.warning(
                    "Admin defaults were applied in memory but could not be persisted to %s",
                    self.config_path,
                )

    # ── API integration ──────────────────────────────────────────────
    # ADR-001: This method is the sole core→api integration point.
    # The import is deferred to method scope to keep core's module-level
    # dependency graph free of api references. Do not move to module level.

    def create_api_app(self, runtime_control: RuntimeControl) -> Any:
        """Create the FastAPI management control plane app with bot injected.

        Must be called after ``boot()`` has completed successfully.
        """
        if self.bot is None:
            raise RuntimeError("Cannot create API app before boot() completes")
        from shinbot.api.app import create_api_app as _create

        self.bot.runtime_control = runtime_control

        return _create(self.bot, self, runtime_control)

    def save_config(self) -> bool:
        """Persist the current in-memory config dict back to the physical config file."""
        try:
            import tomli_w
        except ImportError:
            logger.error(
                "tomli-w is unavailable; config persistence is skipped (install with: uv add tomli-w)"
            )
            return False

        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            with self.config_path.open("wb") as fh:
                tomli_w.dump(self.config, fh)
        except Exception:
            logger.exception("Failed to persist config to %s", self.config_path)
            return False

        logger.info("Config persisted to %s", self.config_path)
        return True
