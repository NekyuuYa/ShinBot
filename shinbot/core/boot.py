"""System boot controller implementing the 5-phase startup lifecycle."""

from __future__ import annotations

import shutil
import tomllib
from enum import Enum
from pathlib import Path
from typing import Any

from shinbot.core.app import ShinBot
from shinbot.utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


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
        self.bot: ShinBot | None = None
        self.dashboard_dist_dir: Path | None = None
        self.dashboard_index_file: Path | None = None

    async def boot(self) -> ShinBot:
        """Execute phase 1-5 startup sequence."""
        self.state = BootState.BOOTING

        self._phase1_environment()
        self._phase2_infrastructure()
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

        # 2) Notify plugins and free plugin resources.
        await self.bot.plugin_manager.unload_all_plugins_async()

        # 3) Persist session state.
        for session in self.bot.session_manager.all_sessions:
            self.bot.session_manager.update(session)

        # 4) Infrastructure teardown placeholder (DB pool not present yet).
        self.state = BootState.UNINITIALIZED

    def _phase1_environment(self) -> None:
        logger.info("Boot Phase 1/5: environment")

        self.config = self._load_config(self.config_path)
        self._ensure_admin_defaults()
        cfg_level = self.config.get("logging", {}).get("level", self.log_level)
        self._configure_logging(cfg_level)
        self._cleanup_temp_directory()

        required_dirs = [
            self.data_dir,
            self.data_dir / "plugins",
            self.data_dir / "plugin_data",
            self.data_dir / "sessions",
            self.data_dir / "audit",
        ]
        for path in required_dirs:
            self._ensure_rw(path)

    def _phase2_infrastructure(self) -> None:
        logger.info("Boot Phase 2/5: infrastructure")
        self._init_dashboard_static_config()
        try:
            self.bot = ShinBot(data_dir=self.data_dir)
        except Exception:
            self.state = BootState.DEGRADED
            raise

    def _init_dashboard_static_config(self) -> None:
        """Resolve and cache dashboard dist/index paths during infrastructure phase."""
        admin_cfg = self.config.setdefault("admin", {})

        configured_dist = admin_cfg.get("dashboard_dist")
        candidates: list[Path] = []

        if isinstance(configured_dist, str) and configured_dist.strip():
            configured_path = Path(configured_dist)
            if not configured_path.is_absolute():
                configured_path = (self.config_path.parent / configured_path).resolve()
            candidates.append(configured_path)

        candidates.extend(
            [
                (self.config_path.parent / "dashboard" / "dist").resolve(),
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
            self.bot.pipeline,
            self.bot.command_registry,
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

        for plugin_cfg in self.config.get("plugins", []):
            plugin_id = plugin_cfg.get("id")
            module_path = plugin_cfg.get("module")
            if not plugin_id or not module_path:
                logger.warning("Invalid plugin config entry: %s", plugin_cfg)
                continue
            try:
                await self.bot.load_plugin_async(plugin_id, module_path)
            except Exception:
                logger.exception("Failed to load plugin %s from %s", plugin_id, module_path)

    async def _phase5_adapter_activation(self) -> None:
        logger.info("Boot Phase 5/5: adapter activation")
        if self.bot is None:
            raise RuntimeError("Bot is not initialized")

        self._setup_permissions()
        self._setup_instances()
        await self.bot.start()

    def _setup_instances(self) -> None:
        assert self.bot is not None
        instances = self.config.get("instances", [])
        if not instances:
            logger.warning("No instances configured - bot will start with no connections")
            return

        for inst_cfg in instances:
            instance_id = inst_cfg["id"]
            platform = inst_cfg.get("platform", "satori")
            config_kwargs = inst_cfg.get("config", {})
            if not isinstance(config_kwargs, dict):
                config_kwargs = {}

            # Backward compatibility with legacy layout: instances[].satori
            if platform == "satori" and not config_kwargs:
                legacy_satori_cfg = inst_cfg.get("satori", {})
                if isinstance(legacy_satori_cfg, dict):
                    config_kwargs = {
                        "host": legacy_satori_cfg.get("host", "localhost:5140"),
                        "token": legacy_satori_cfg.get("token", ""),
                        "reconnect_delay": legacy_satori_cfg.get("reconnect_delay", 5.0),
                    }

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

    def _setup_permissions(self) -> None:
        assert self.bot is not None
        perms = self.config.get("permissions", {})
        for binding in perms.get("bindings", []):
            key = binding.get("key", "")
            group = binding.get("group", "default")
            try:
                self.bot.permission_engine.bind(key, group)
                logger.info("Bound %r -> permission group %r", key, group)
            except ValueError as exc:
                logger.warning("Permission binding error: %s", exc)

    def _configure_logging(self, level_name: str = "INFO") -> None:
        setup_logging(level_name)

    def _load_config(self, config_path: Path) -> dict[str, Any]:
        if not config_path.exists():
            logger.warning("Config file %s not found, using defaults", config_path)
            return {}
        with config_path.open("rb") as file_obj:
            return tomllib.load(file_obj)

    def _cleanup_temp_directory(self) -> None:
        temp_dir = self.data_dir / "temp"
        if not temp_dir.exists():
            temp_dir.mkdir(parents=True, exist_ok=True)
            return

        for child in temp_dir.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except Exception:
                logger.exception("Failed to clean temp entry %s", child)

    def _ensure_admin_defaults(self) -> None:
        """Ensure [admin] config always exists and persist only when defaults are injected."""
        admin_cfg = self.config.get("admin")
        if not isinstance(admin_cfg, dict):
            admin_cfg = {}

        changed = False
        if not admin_cfg.get("username"):
            admin_cfg["username"] = "admin"
            changed = True
        if not admin_cfg.get("password"):
            admin_cfg["password"] = "admin"
            changed = True
        if "jwt_expire_hours" not in admin_cfg:
            admin_cfg["jwt_expire_hours"] = 24
            changed = True

        self.config["admin"] = admin_cfg

        if changed:
            logger.warning(
                "[admin] section is missing or incomplete in %s; defaults admin/admin were injected",
                self.config_path,
            )
            saved = self.save_config()
            if not saved:
                logger.warning("Admin defaults were applied in memory but could not be persisted")

    def _ensure_rw(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        probe = directory / ".rw_probe"
        try:
            probe.write_text("ok", encoding="utf-8")
            _ = probe.read_text(encoding="utf-8")
        finally:
            if probe.exists():
                probe.unlink()

    # ── API integration ──────────────────────────────────────────────
    # ADR-001: This method is the sole core→api integration point.
    # The import is deferred to method scope to keep core's module-level
    # dependency graph free of api references. Do not move to module level.

    def create_api_app(self) -> Any:
        """Create the FastAPI management control plane app with bot injected.

        Must be called after ``boot()`` has completed successfully.
        """
        if self.bot is None:
            raise RuntimeError("Cannot create API app before boot() completes")
        from shinbot.api.app import create_api_app as _create

        return _create(self.bot, self)

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
