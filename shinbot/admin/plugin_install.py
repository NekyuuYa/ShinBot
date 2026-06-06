"""Administrative helpers for WebUI-managed plugin installation."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import shutil
import stat
import sys
import time
import uuid
import zipfile
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import quote, urlparse

import httpx

from shinbot.admin.plugin_admin import (
    disable_plugin_or_raise,
    enable_plugin_or_raise,
    rescan_plugins,
)
from shinbot.core.plugins.config import plugin_config_entry, set_plugin_saved_enabled
from shinbot.core.plugins.types import PluginState

PLUGIN_INSTALL_MANIFEST_VERSION = 1
PLUGIN_INSTALL_MAX_ARCHIVE_BYTES = 20 * 1024 * 1024
PLUGIN_INSTALL_MAX_EXTRACTED_BYTES = 100 * 1024 * 1024
_VALID_PLUGIN_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_")
_VALID_ROLE_VALUES = {"logic", "adapter"}
_GITHUB_REF_RE = re.compile(r"^[A-Za-z0-9._/\-]{1,200}$")


@dataclass(slots=True)
class PluginInstallError(RuntimeError):
    """Structured error raised by plugin installation helpers."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class PluginInstallTask:
    """In-memory status for one plugin install operation."""

    task_id: str
    status: Literal["queued", "running", "succeeded", "failed"] = "queued"
    stage: str = "queued"
    message: str = "Queued"
    plugin_id: str | None = None
    error: dict[str, str] | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    def as_dict(self) -> dict[str, Any]:
        """Return a serializable task status snapshot."""
        return {
            "task_id": self.task_id,
            "status": self.status,
            "stage": self.stage,
            "message": self.message,
            "plugin_id": self.plugin_id,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class PluginInstallTaskRegistry:
    """Process-local task status registry for plugin installs."""

    def __init__(self) -> None:
        """Initialize an empty task registry."""
        self._tasks: dict[str, PluginInstallTask] = {}

    def create(self) -> PluginInstallTask:
        """Create and store a new queued install task."""
        task = PluginInstallTask(task_id=f"plugin-install-{uuid.uuid4().hex}")
        self._tasks[task.task_id] = task
        return task

    def get(self, task_id: str) -> PluginInstallTask | None:
        """Return a task by ID, if known."""
        return self._tasks.get(task_id)

    def update(
        self,
        task: PluginInstallTask,
        *,
        status: Literal["queued", "running", "succeeded", "failed"] | None = None,
        stage: str | None = None,
        message: str | None = None,
        plugin_id: str | None = None,
        error: dict[str, str] | None = None,
    ) -> None:
        """Update a stored task in place."""
        if status is not None:
            task.status = status
        if stage is not None:
            task.stage = stage
        if message is not None:
            task.message = message
        if plugin_id is not None:
            task.plugin_id = plugin_id
        if error is not None:
            task.error = error
        task.updated_at = time.time()


@dataclass(slots=True)
class PluginInstallRecord:
    """One WebUI-managed plugin source record."""

    plugin_id: str
    source_type: Literal["github", "archive"]
    source_url: str
    ref: str
    resolved_ref: str
    installed_at: float
    updated_at: float
    installed_version: str
    plugin_path: str = ""
    managed_by_webui: bool = True
    archive_sha256: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a manifest-compatible dict."""
        return {
            "plugin_id": self.plugin_id,
            "source_type": self.source_type,
            "source_url": self.source_url,
            "ref": self.ref,
            "resolved_ref": self.resolved_ref,
            "plugin_path": self.plugin_path,
            "installed_at": self.installed_at,
            "updated_at": self.updated_at,
            "installed_version": self.installed_version,
            "managed_by_webui": self.managed_by_webui,
            "archive_sha256": self.archive_sha256,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PluginInstallRecord:
        """Build a source record from manifest data."""
        source_type = value.get("source_type")
        if source_type not in {"github", "archive"}:
            raise ValueError("source_type must be 'github' or 'archive'")
        plugin_id = value.get("plugin_id")
        if not isinstance(plugin_id, str) or not plugin_id:
            raise ValueError("plugin_id must be a non-empty string")
        try:
            plugin_path = _normalize_plugin_path(str(value.get("plugin_path", "")))
        except PluginInstallError as exc:
            raise ValueError(str(exc)) from exc
        return cls(
            plugin_id=plugin_id,
            source_type=source_type,
            source_url=str(value.get("source_url", "")),
            ref=str(value.get("ref", "")),
            resolved_ref=str(value.get("resolved_ref", "")),
            plugin_path=plugin_path,
            installed_at=float(value.get("installed_at", 0)),
            updated_at=float(value.get("updated_at", 0)),
            installed_version=str(value.get("installed_version", "0.0.0")),
            managed_by_webui=bool(value.get("managed_by_webui", True)),
            archive_sha256=str(value.get("archive_sha256", "")),
        )


class PluginInstallManifest:
    """File-backed WebUI plugin source manifest."""

    def __init__(self, data_dir: Path | str) -> None:
        """Initialize the manifest helper for a ShinBot data directory."""
        self.data_dir = Path(data_dir)
        self.path = self.data_dir / "plugin_install_manifest.json"

    def load(self) -> dict[str, PluginInstallRecord]:
        """Load WebUI-managed plugin source records."""
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_MANIFEST_INVALID",
                message=f"Invalid plugin install manifest: {exc}",
            ) from exc
        if not isinstance(payload, dict):
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_MANIFEST_INVALID",
                message="Plugin install manifest must contain a JSON object",
            )
        if payload.get("schema_version") != PLUGIN_INSTALL_MANIFEST_VERSION:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_MANIFEST_INVALID",
                message="Unsupported plugin install manifest schema version",
            )
        raw_plugins = payload.get("plugins", {})
        if not isinstance(raw_plugins, dict):
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_MANIFEST_INVALID",
                message="Plugin install manifest plugins must be an object",
            )
        records: dict[str, PluginInstallRecord] = {}
        try:
            for plugin_id, value in raw_plugins.items():
                if not isinstance(plugin_id, str) or not isinstance(value, dict):
                    raise ValueError("manifest plugin entries must be objects")
                record = PluginInstallRecord.from_dict(value)
                if record.plugin_id != plugin_id:
                    raise ValueError("manifest plugin_id must match entry key")
                records[plugin_id] = record
        except ValueError as exc:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_MANIFEST_INVALID",
                message=f"Invalid plugin install manifest entry: {exc}",
            ) from exc
        return records

    def save(self, records: dict[str, PluginInstallRecord]) -> None:
        """Persist records atomically."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": PLUGIN_INSTALL_MANIFEST_VERSION,
            "plugins": {
                plugin_id: record.as_dict()
                for plugin_id, record in sorted(records.items(), key=lambda item: item[0])
            },
        }
        tmp_path = self.path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        tmp_path.replace(self.path)

    def list_records(self) -> list[dict[str, Any]]:
        """Return manifest records as serializable dicts."""
        return [record.as_dict() for record in self.load().values()]


@dataclass(slots=True)
class PluginPackagePreview:
    """Validated plugin package preview."""

    plugin_id: str
    name: str
    version: str
    description: str
    author: str
    role: str
    entry: str
    permissions: list[str]
    required_dependencies: list[str]
    optional_dependencies: list[str]
    legacy_dependencies: list[str]
    missing_required_dependencies: list[str]
    missing_optional_dependencies: list[str]
    source_type: Literal["github", "archive"]
    source_url: str
    ref: str
    resolved_ref: str
    plugin_path: str
    archive_sha256: str
    target_exists: bool
    target_managed_by_webui: bool
    can_install: bool
    warnings: list[str]
    plugin_root: Path

    def as_dict(self) -> dict[str, Any]:
        """Return the WebUI/API preview payload."""
        return {
            "plugin_id": self.plugin_id,
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "role": self.role,
            "entry": self.entry,
            "permissions": self.permissions,
            "required_dependencies": self.required_dependencies,
            "optional_dependencies": self.optional_dependencies,
            "legacy_dependencies": self.legacy_dependencies,
            "missing_required_dependencies": self.missing_required_dependencies,
            "missing_optional_dependencies": self.missing_optional_dependencies,
            "source_type": self.source_type,
            "source_url": self.source_url,
            "ref": self.ref,
            "resolved_ref": self.resolved_ref,
            "plugin_path": self.plugin_path,
            "archive_sha256": self.archive_sha256,
            "target_exists": self.target_exists,
            "target_managed_by_webui": self.target_managed_by_webui,
            "can_install": self.can_install,
            "warnings": self.warnings,
        }


class PluginInstallService:
    """Install, update, preview, and uninstall WebUI-managed plugins."""

    def __init__(
        self,
        *,
        bot: Any,
        boot: Any,
        task_registry: PluginInstallTaskRegistry | None = None,
    ) -> None:
        """Initialize a plugin installation service."""
        self.bot = bot
        self.boot = boot
        self.data_dir = Path(boot.data_dir)
        self.plugins_dir = self.data_dir / "plugins"
        self.tmp_dir = self.data_dir / "plugin_install_tmp"
        self.manifest = PluginInstallManifest(self.data_dir)
        self.task_registry = task_registry or PluginInstallTaskRegistry()

    def list_sources(self) -> dict[str, Any]:
        """Return WebUI-managed plugin source records."""
        return {"plugins": self.manifest.list_records()}

    async def preview_archive(
        self,
        archive_bytes: bytes,
        *,
        filename: str = "",
    ) -> dict[str, Any]:
        """Preview a raw zip plugin archive without installing it."""
        task_id = f"preview-{uuid.uuid4().hex}"
        package_path, extract_root = self._prepare_archive_workspace(task_id, archive_bytes)
        try:
            preview = self._preview_extracted_package(
                extract_root,
                source_type="archive",
                source_url=filename or "uploaded_archive",
                ref="",
                resolved_ref="",
                plugin_path="",
                archive_sha256=_sha256_bytes(archive_bytes),
            )
            return preview.as_dict()
        finally:
            shutil.rmtree(self.tmp_dir / task_id, ignore_errors=True)

    async def preview_github(
        self,
        url: str,
        ref: str = "main",
        *,
        plugin_path: str = "",
    ) -> dict[str, Any]:
        """Preview a GitHub archive without installing it."""
        normalized_plugin_path = _normalize_plugin_path(plugin_path)
        archive_bytes, resolved_ref = await self._download_github_archive(url, ref)
        task_id = f"preview-{uuid.uuid4().hex}"
        package_path, extract_root = self._prepare_archive_workspace(task_id, archive_bytes)
        try:
            preview = self._preview_extracted_package(
                extract_root,
                source_type="github",
                source_url=_normalize_github_url(url),
                ref=ref,
                resolved_ref=resolved_ref,
                plugin_path=normalized_plugin_path,
                archive_sha256=_sha256_bytes(archive_bytes),
            )
            return preview.as_dict()
        finally:
            package_path.unlink(missing_ok=True)
            shutil.rmtree(self.tmp_dir / task_id, ignore_errors=True)

    async def install_archive(
        self,
        archive_bytes: bytes,
        *,
        filename: str = "",
        enable_after_install: bool = True,
        allow_overwrite: bool = False,
    ) -> dict[str, Any]:
        """Install a raw zip plugin archive."""
        task = self.task_registry.create()
        await self._run_archive_install(
            task,
            archive_bytes,
            source_type="archive",
            source_url=filename or "uploaded_archive",
            ref="",
            resolved_ref="",
            plugin_path="",
            enable_after_install=enable_after_install,
            allow_overwrite=allow_overwrite,
        )
        return task.as_dict()

    async def install_github(
        self,
        url: str,
        ref: str = "main",
        *,
        plugin_path: str = "",
        enable_after_install: bool = True,
        allow_overwrite: bool = False,
    ) -> dict[str, Any]:
        """Install a plugin from a GitHub repository archive."""
        task = self.task_registry.create()
        normalized_plugin_path = _normalize_plugin_path(plugin_path)
        try:
            self.task_registry.update(
                task,
                status="running",
                stage="downloading",
                message="Downloading GitHub archive",
            )
            archive_bytes, resolved_ref = await self._download_github_archive(url, ref)
            source_url = _normalize_github_url(url)
        except PluginInstallError as exc:
            self._fail_task(task, exc)
            raise
        await self._run_archive_install(
            task,
            archive_bytes,
            source_type="github",
            source_url=source_url,
            ref=ref,
            resolved_ref=resolved_ref,
            plugin_path=normalized_plugin_path,
            enable_after_install=enable_after_install,
            allow_overwrite=allow_overwrite,
        )
        return task.as_dict()

    async def update_plugin(
        self,
        plugin_id: str,
        *,
        enable_after_install: bool = True,
    ) -> dict[str, Any]:
        """Update a WebUI-managed GitHub plugin from its manifest source."""
        records = self.manifest.load()
        record = records.get(plugin_id)
        if record is None or not record.managed_by_webui:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_TARGET_UNMANAGED",
                message=f"Plugin {plugin_id!r} is not managed by WebUI",
            )
        if record.source_type != "github":
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_INVALID_SOURCE",
                message=f"Plugin {plugin_id!r} was not installed from GitHub",
            )
        return await self.install_github(
            record.source_url,
            record.ref or "main",
            plugin_path=record.plugin_path,
            enable_after_install=enable_after_install,
            allow_overwrite=True,
        )

    async def uninstall_plugin(self, plugin_id: str) -> dict[str, Any]:
        """Uninstall a WebUI-managed plugin while preserving plugin data."""
        task = self.task_registry.create()
        self.task_registry.update(
            task,
            status="running",
            stage="uninstalling",
            message="Uninstalling plugin",
            plugin_id=plugin_id,
        )
        try:
            records = self.manifest.load()
            record = records.get(plugin_id)
            if record is None or not record.managed_by_webui:
                raise PluginInstallError(
                    status_code=409,
                    code="PLUGIN_INSTALL_TARGET_UNMANAGED",
                    message=f"Plugin {plugin_id!r} is not managed by WebUI",
                )
            await self._unload_if_loaded(plugin_id)
            target = self._plugin_target(plugin_id)
            if target.exists():
                shutil.rmtree(target)
            records.pop(plugin_id, None)
            self.manifest.save(records)
            self.task_registry.update(
                task,
                status="succeeded",
                stage="succeeded",
                message="Plugin uninstalled",
            )
            return task.as_dict()
        except PluginInstallError as exc:
            self._fail_task(task, exc)
            raise
        except Exception as exc:
            error = PluginInstallError(
                status_code=500,
                code="PLUGIN_INSTALL_LOAD_FAILED",
                message=str(exc),
            )
            self._fail_task(task, error)
            raise error from exc

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        """Return a task status snapshot."""
        task = self.task_registry.get(task_id)
        return task.as_dict() if task is not None else None

    async def _run_archive_install(
        self,
        task: PluginInstallTask,
        archive_bytes: bytes,
        *,
        source_type: Literal["github", "archive"],
        source_url: str,
        ref: str,
        resolved_ref: str,
        plugin_path: str,
        enable_after_install: bool,
        allow_overwrite: bool,
    ) -> None:
        task_id = task.task_id
        try:
            self.task_registry.update(
                task,
                status="running",
                stage="extracting",
                message="Extracting plugin archive",
            )
            _, extract_root = self._prepare_archive_workspace(task_id, archive_bytes)
            preview = self._preview_extracted_package(
                extract_root,
                source_type=source_type,
                source_url=source_url,
                ref=ref,
                resolved_ref=resolved_ref,
                plugin_path=plugin_path,
                archive_sha256=_sha256_bytes(archive_bytes),
            )
            self.task_registry.update(
                task,
                stage="validating",
                message="Validating plugin package",
                plugin_id=preview.plugin_id,
            )
            if preview.missing_required_dependencies:
                missing = ", ".join(preview.missing_required_dependencies)
                raise PluginInstallError(
                    status_code=409,
                    code="PLUGIN_INSTALL_REQUIRED_DEPENDENCY_MISSING",
                    message=f"Required plugin dependencies are missing: {missing}",
                )
            self._validate_target_policy(preview.plugin_id, allow_overwrite=allow_overwrite)
            self.task_registry.update(task, stage="installing", message="Installing plugin files")
            await self._install_preview(
                preview,
                enable_after_install=enable_after_install,
                allow_overwrite=allow_overwrite,
            )
            self.task_registry.update(
                task,
                status="succeeded",
                stage="succeeded",
                message="Plugin installed",
            )
        except PluginInstallError as exc:
            self._fail_task(task, exc)
            raise
        except Exception as exc:
            error = PluginInstallError(
                status_code=500,
                code="PLUGIN_INSTALL_LOAD_FAILED",
                message=str(exc),
            )
            self._fail_task(task, error)
            raise error from exc
        finally:
            shutil.rmtree(self.tmp_dir / task_id, ignore_errors=True)

    async def _install_preview(
        self,
        preview: PluginPackagePreview,
        *,
        enable_after_install: bool,
        allow_overwrite: bool,
    ) -> None:
        plugin_id = preview.plugin_id
        target = self._plugin_target(plugin_id)
        staging = self.plugins_dir / f".installing-{plugin_id}-{uuid.uuid4().hex}"
        backup = self.plugins_dir / f".backup-{plugin_id}-{uuid.uuid4().hex}"
        had_existing = target.exists()
        backup_created = False
        self.plugins_dir.mkdir(parents=True, exist_ok=True)

        await self._unload_if_loaded(plugin_id)
        shutil.copytree(preview.plugin_root, staging)
        records = self.manifest.load()
        previous_record = records.get(plugin_id)
        previous_config = self._snapshot_plugin_config(plugin_id)
        try:
            if had_existing:
                if not allow_overwrite:
                    raise PluginInstallError(
                        status_code=409,
                        code="PLUGIN_INSTALL_TARGET_EXISTS",
                        message=f"Plugin target already exists: {target}",
                    )
                shutil.move(str(target), str(backup))
                backup_created = True
            shutil.move(str(staging), str(target))
            self._persist_enabled(plugin_id, enable_after_install)
            now = time.time()
            installed_at = previous_record.installed_at if previous_record is not None else now
            records[plugin_id] = PluginInstallRecord(
                plugin_id=plugin_id,
                source_type=preview.source_type,
                source_url=preview.source_url,
                ref=preview.ref,
                resolved_ref=preview.resolved_ref,
                plugin_path=preview.plugin_path,
                installed_at=installed_at,
                updated_at=now,
                installed_version=preview.version,
                managed_by_webui=True,
                archive_sha256=preview.archive_sha256,
            )
            self.manifest.save(records)
            self._purge_plugin_modules(plugin_id)
            await rescan_plugins(self.bot, self.boot)
            meta = self.bot.plugin_manager.get_plugin(plugin_id)
            if meta is None or meta.state in {PluginState.LOAD_FAILED, PluginState.ERROR}:
                raise PluginInstallError(
                    status_code=500,
                    code="PLUGIN_INSTALL_LOAD_FAILED",
                    message=f"Plugin {plugin_id!r} was installed but failed to load",
                )
            if not enable_after_install:
                await disable_plugin_or_raise(self.bot, plugin_id, self.boot)
            elif self.bot.plugin_manager.get_plugin(plugin_id) is not None:
                await enable_plugin_or_raise(self.bot, plugin_id, self.boot)
            if backup_created:
                shutil.rmtree(backup, ignore_errors=True)
        except Exception:
            if target.exists():
                shutil.rmtree(target, ignore_errors=True)
            if backup_created and backup.exists():
                shutil.move(str(backup), str(target))
                try:
                    self._purge_plugin_modules(plugin_id)
                    await rescan_plugins(self.bot, self.boot)
                except Exception:
                    pass
            if previous_record is None:
                records.pop(plugin_id, None)
            else:
                records[plugin_id] = previous_record
            self._restore_plugin_config(plugin_id, previous_config)
            try:
                self.manifest.save(records)
            except PluginInstallError:
                pass
            raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    def _preview_extracted_package(
        self,
        extract_root: Path,
        *,
        source_type: Literal["github", "archive"],
        source_url: str,
        ref: str,
        resolved_ref: str,
        plugin_path: str,
        archive_sha256: str,
    ) -> PluginPackagePreview:
        self._ensure_inside_data_dir(extract_root, allow_plugins=False)
        normalized_plugin_path = _normalize_plugin_path(plugin_path)
        plugin_root = self._find_plugin_root(extract_root, plugin_path=normalized_plugin_path)
        metadata = self._load_metadata(plugin_root)
        plugin_id = metadata["id"]
        required = metadata["required_dependencies"]
        optional = metadata["optional_dependencies"]
        legacy = metadata["dependencies"]
        installed = self._available_plugin_ids(extra_roots=[plugin_root])
        missing_required = sorted(dep for dep in required if dep not in installed)
        missing_optional = sorted(dep for dep in optional if dep not in installed)
        warnings: list[str] = []
        for dep in missing_optional:
            warnings.append(f"Optional dependency {dep!r} is not installed")
        for dep in legacy:
            if dep not in installed:
                warnings.append(f"Legacy dependency {dep!r} is not installed")
        records = self.manifest.load()
        target = self._plugin_target(plugin_id)
        target_exists = target.exists()
        target_managed = bool(records.get(plugin_id) and records[plugin_id].managed_by_webui)
        can_install = not missing_required and (not target_exists or target_managed)
        return PluginPackagePreview(
            plugin_id=plugin_id,
            name=metadata["name"],
            version=metadata["version"],
            description=metadata["description"],
            author=metadata["author"],
            role=metadata["role"],
            entry=metadata["entry"],
            permissions=metadata["permissions"],
            required_dependencies=required,
            optional_dependencies=optional,
            legacy_dependencies=legacy,
            missing_required_dependencies=missing_required,
            missing_optional_dependencies=missing_optional,
            source_type=source_type,
            source_url=source_url,
            ref=ref,
            resolved_ref=resolved_ref,
            plugin_path=normalized_plugin_path,
            archive_sha256=archive_sha256,
            target_exists=target_exists,
            target_managed_by_webui=target_managed,
            can_install=can_install,
            warnings=warnings,
            plugin_root=plugin_root,
        )

    def _prepare_archive_workspace(self, task_id: str, archive_bytes: bytes) -> tuple[Path, Path]:
        if len(archive_bytes) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginInstallError(
                status_code=413,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive is too large",
            )
        task_root = self.tmp_dir / task_id
        extract_root = task_root / "extract"
        self._ensure_inside_data_dir(task_root, allow_plugins=False)
        shutil.rmtree(task_root, ignore_errors=True)
        extract_root.mkdir(parents=True, exist_ok=True)
        package_path = task_root / "package.zip"
        package_path.write_bytes(archive_bytes)
        self._extract_zip(package_path, extract_root)
        return package_path, extract_root

    def _extract_zip(self, package_path: Path, extract_root: Path) -> None:
        total_size = 0
        try:
            with zipfile.ZipFile(package_path) as archive:
                for info in archive.infolist():
                    self._validate_zip_entry(info)
                    total_size += info.file_size
                    if total_size > PLUGIN_INSTALL_MAX_EXTRACTED_BYTES:
                        raise PluginInstallError(
                            status_code=413,
                            code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                            message="Plugin archive extracted size is too large",
                        )
                    destination = (extract_root / info.filename).resolve()
                    if not destination.is_relative_to(extract_root.resolve()):
                        raise PluginInstallError(
                            status_code=409,
                            code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                            message="Plugin archive contains unsafe paths",
                        )
                archive.extractall(extract_root)
        except zipfile.BadZipFile as exc:
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive is not a valid zip file",
            ) from exc

    def _validate_zip_entry(self, info: zipfile.ZipInfo) -> None:
        name = info.filename
        if not name or name.startswith(("/", "\\")):
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive contains absolute paths",
            )
        normalized = PurePosixPath(name)
        if ".." in normalized.parts:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive contains parent-directory paths",
            )
        file_type = (info.external_attr >> 16) & 0o170000
        if stat.S_ISLNK(file_type):
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="Plugin archive must not contain symlinks",
            )

    def _find_plugin_root(self, extract_root: Path, *, plugin_path: str) -> Path:
        if plugin_path:
            root = self._repo_relative_path(extract_root, plugin_path)
            if not (root / "metadata.json").is_file():
                raise PluginInstallError(
                    status_code=422,
                    code="PLUGIN_INSTALL_METADATA_NOT_FOUND",
                    message=f"Plugin path {plugin_path!r} does not contain metadata.json",
                )
            return root

        candidates = [extract_root]
        candidates.extend(path for path in sorted(extract_root.iterdir()) if path.is_dir())
        roots = [path for path in candidates if (path / "metadata.json").is_file()]
        if not roots:
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_NOT_FOUND",
                message="Plugin archive does not contain metadata.json",
            )
        if len(roots) > 1:
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message="Plugin archive contains multiple metadata.json roots",
            )
        return roots[0]

    def _repo_relative_path(self, extract_root: Path, plugin_path: str) -> Path:
        relative = PurePosixPath(plugin_path)
        roots = [extract_root]
        roots.extend(path for path in sorted(extract_root.iterdir()) if path.is_dir())
        extract_resolved = extract_root.resolve()
        for root in roots:
            candidate = (root / Path(*relative.parts)).resolve()
            if candidate == extract_resolved or extract_resolved in candidate.parents:
                if candidate.is_dir():
                    return candidate
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_PLUGIN_PATH_NOT_FOUND",
            message=f"Plugin path {plugin_path!r} was not found in the archive",
        )

    def _load_metadata(self, plugin_root: Path) -> dict[str, Any]:
        metadata_path = plugin_root / "metadata.json"
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message=f"Invalid plugin metadata.json: {exc}",
            ) from exc
        if not isinstance(payload, dict):
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message="Plugin metadata.json must contain an object",
            )
        return self._validate_metadata(payload, plugin_root)

    def _validate_metadata(self, metadata: dict[str, Any], plugin_root: Path) -> dict[str, Any]:
        plugin_id = metadata.get("id")
        entry = metadata.get("entry")
        if not isinstance(plugin_id, str) or not plugin_id.strip():
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message="metadata.id must be a non-empty string",
            )
        plugin_id = plugin_id.strip()
        if not any(plugin_id.startswith(prefix) for prefix in _VALID_PLUGIN_PREFIXES):
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_ID_INVALID",
                message=f"Plugin id {plugin_id!r} must start with a ShinBot plugin prefix",
            )
        if not isinstance(entry, str) or not entry.strip():
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message="metadata.entry must be a non-empty string",
            )
        entry_path = Path(entry)
        if entry_path.is_absolute() or ".." in entry_path.parts:
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message="metadata.entry must stay inside the plugin directory",
            )
        abs_entry = (plugin_root / entry_path).resolve()
        if not abs_entry.is_relative_to(plugin_root.resolve()) or not abs_entry.is_file():
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_ENTRY_NOT_FOUND",
                message=f"metadata.entry file does not exist: {entry}",
            )
        role = metadata.get("role", "logic")
        if not isinstance(role, str) or role.strip().lower() not in _VALID_ROLE_VALUES:
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_METADATA_INVALID",
                message="metadata.role must be 'logic' or 'adapter'",
            )
        return {
            "id": plugin_id,
            "entry": entry_path.as_posix(),
            "name": _metadata_string(metadata, "name", plugin_id),
            "version": _metadata_string(metadata, "version", "0.0.0"),
            "description": _metadata_string(metadata, "description", ""),
            "author": _metadata_string(metadata, "author", ""),
            "role": role.strip().lower(),
            "permissions": _metadata_string_list(metadata, "permissions"),
            "dependencies": _metadata_string_list(metadata, "dependencies"),
            "required_dependencies": _metadata_string_list(metadata, "required_dependencies"),
            "optional_dependencies": _metadata_string_list(metadata, "optional_dependencies"),
        }

    def _available_plugin_ids(self, *, extra_roots: list[Path] | None = None) -> set[str]:
        ids = {meta.id for meta in self.bot.plugin_manager.all_plugins}
        roots = [self.plugins_dir]
        if extra_roots:
            roots.extend(extra_roots)
        for root in roots:
            if root.is_dir() and (root / "metadata.json").is_file():
                try:
                    ids.add(self._load_metadata(root)["id"])
                except PluginInstallError:
                    pass
            elif root.is_dir():
                for child in root.iterdir():
                    if child.is_dir() and (child / "metadata.json").is_file():
                        try:
                            ids.add(self._load_metadata(child)["id"])
                        except PluginInstallError:
                            pass
        return ids

    def _validate_target_policy(self, plugin_id: str, *, allow_overwrite: bool) -> None:
        target = self._plugin_target(plugin_id)
        if not target.exists():
            return
        records = self.manifest.load()
        record = records.get(plugin_id)
        if record is None or not record.managed_by_webui:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_TARGET_UNMANAGED",
                message=f"Plugin {plugin_id!r} already exists and is not WebUI-managed",
            )
        if not allow_overwrite:
            raise PluginInstallError(
                status_code=409,
                code="PLUGIN_INSTALL_TARGET_EXISTS",
                message=f"Plugin {plugin_id!r} already exists",
            )

    def _plugin_target(self, plugin_id: str) -> Path:
        target = (self.plugins_dir / plugin_id).resolve()
        root = self.plugins_dir.resolve()
        if target != root and root in target.parents:
            return target
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_ID_INVALID",
            message=f"Invalid plugin id for target path: {plugin_id!r}",
        )

    def _ensure_inside_data_dir(self, path: Path, *, allow_plugins: bool) -> None:
        resolved = path.resolve()
        data_root = self.data_dir.resolve()
        if resolved == data_root or data_root in resolved.parents:
            if not allow_plugins and self.plugins_dir.resolve() in resolved.parents:
                raise PluginInstallError(
                    status_code=409,
                    code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                    message="Plugin install temporary paths must not be inside data/plugins",
                )
            return
        raise PluginInstallError(
            status_code=409,
            code="PLUGIN_INSTALL_ARCHIVE_INVALID",
            message="Plugin install path escapes the data directory",
        )

    def _persist_enabled(self, plugin_id: str, enabled: bool) -> None:
        set_plugin_saved_enabled(self.boot, plugin_id, enabled)
        if not self.boot.save_config():
            raise PluginInstallError(
                status_code=500,
                code="CONFIG_WRITE_FAILED",
                message=f"Failed to persist enabled state for plugin {plugin_id!r}",
            )

    def _snapshot_plugin_config(self, plugin_id: str) -> dict[str, Any] | None:
        entry = plugin_config_entry(self.boot.config, plugin_id)
        return deepcopy(entry) if entry is not None else None

    def _restore_plugin_config(self, plugin_id: str, snapshot: dict[str, Any] | None) -> None:
        plugins = self.boot.config.get("plugins", [])
        if not isinstance(plugins, list):
            if snapshot is None:
                return
            plugins = []
            self.boot.config["plugins"] = plugins

        existing_index = None
        for index, item in enumerate(plugins):
            if isinstance(item, dict) and item.get("id") == plugin_id:
                existing_index = index
                break

        if snapshot is None:
            if existing_index is not None:
                plugins.pop(existing_index)
        elif existing_index is None:
            plugins.append(deepcopy(snapshot))
        else:
            plugins[existing_index] = deepcopy(snapshot)

        if not self.boot.save_config():
            raise PluginInstallError(
                status_code=500,
                code="CONFIG_WRITE_FAILED",
                message=f"Failed to restore configuration for plugin {plugin_id!r}",
            )

    async def _unload_if_loaded(self, plugin_id: str) -> None:
        if self.bot.plugin_manager.get_plugin(plugin_id) is None:
            self._purge_plugin_modules(plugin_id)
            return
        try:
            await disable_plugin_or_raise(self.bot, plugin_id, self.boot)
        except Exception:
            pass
        await self.bot.plugin_manager.unload_plugin_async(plugin_id)
        self._purge_plugin_modules(plugin_id)

    def _purge_plugin_modules(self, plugin_id: str) -> None:
        candidates = [
            name
            for name in sys.modules
            if name == plugin_id
            or name.endswith(f".{plugin_id}")
            or name.startswith(f"{plugin_id}.")
            or f".{plugin_id}." in name
        ]
        for name in candidates:
            sys.modules.pop(name, None)
        importlib.invalidate_caches()

    async def _download_github_archive(self, url: str, ref: str) -> tuple[bytes, str]:
        owner, repo = _parse_github_repo(url)
        _validate_github_ref(ref)
        archive_url = (
            f"https://api.github.com/repos/{quote(owner)}/{quote(repo)}/zipball/{quote(ref, safe='')}"
        )
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={"User-Agent": "ShinBot-WebUI-Plugin-Installer"},
            ) as client:
                response = await client.get(archive_url)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise PluginInstallError(
                status_code=502,
                code="PLUGIN_INSTALL_DOWNLOAD_FAILED",
                message=f"Failed to download GitHub plugin archive: {exc}",
            ) from exc
        content = response.content
        if len(content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginInstallError(
                status_code=413,
                code="PLUGIN_INSTALL_ARCHIVE_INVALID",
                message="GitHub plugin archive is too large",
            )
        return content, ""

    def _fail_task(self, task: PluginInstallTask, exc: PluginInstallError) -> None:
        self.task_registry.update(
            task,
            status="failed",
            stage="failed",
            message=exc.message,
            error={"code": exc.code, "message": exc.message},
        )


def build_plugin_install_service(bot: Any, boot: Any) -> PluginInstallService:
    """Build or reuse the process-local plugin install service."""
    service = getattr(boot, "plugin_install_service", None)
    if isinstance(service, PluginInstallService) and service.bot is bot:
        return service
    service = PluginInstallService(bot=bot, boot=boot)
    boot.plugin_install_service = service
    return service


def install_source_for_plugin(boot: Any, plugin_id: str) -> dict[str, Any] | None:
    """Return install-source metadata for a plugin, if WebUI-managed."""
    try:
        record = PluginInstallManifest(Path(boot.data_dir)).load().get(plugin_id)
    except PluginInstallError:
        return None
    if record is None:
        return None
    return {
        "source_type": record.source_type,
        "source_url": record.source_url,
        "ref": record.ref,
        "resolved_ref": record.resolved_ref,
        "plugin_path": record.plugin_path,
        "installed_version": record.installed_version,
        "managed_by_webui": record.managed_by_webui,
        "can_update": record.managed_by_webui and record.source_type == "github",
        "can_uninstall": record.managed_by_webui,
    }


def _metadata_string(metadata: dict[str, Any], key: str, default: str) -> str:
    value = metadata.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_METADATA_INVALID",
            message=f"metadata.{key} must be a string",
        )
    return value.strip() or default


def _metadata_string_list(metadata: dict[str, Any], key: str) -> list[str]:
    value = metadata.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_METADATA_INVALID",
            message=f"metadata.{key} must be a list of strings",
        )
    return [item.strip() for item in value if item.strip()]


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _normalize_plugin_path(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if "\\" in raw:
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_INVALID_PLUGIN_PATH",
            message="Plugin path must use forward slashes",
        )
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_INVALID_PLUGIN_PATH",
            message="Plugin path must be a relative repository directory",
        )
    return path.as_posix()


def _parse_github_repo(url: str) -> tuple[str, str]:
    raw = url.strip()
    if raw.startswith("git@github.com:"):
        path = raw[len("git@github.com:") :]
        if path.endswith(".git"):
            path = path[:-4]
        parts = path.strip("/").split("/")
    else:
        parsed = urlparse(raw)
        if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
            raise PluginInstallError(
                status_code=422,
                code="PLUGIN_INSTALL_INVALID_SOURCE",
                message="Only github.com plugin repositories are supported",
            )
        parts = parsed.path.strip("/").split("/")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_INVALID_SOURCE",
            message="GitHub repository URL must include owner and repository",
        )
    owner = parts[0]
    repo = parts[1][:-4] if parts[1].endswith(".git") else parts[1]
    if not owner or not repo:
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_INVALID_SOURCE",
            message="Invalid GitHub repository URL",
        )
    return owner, repo


def _normalize_github_url(url: str) -> str:
    owner, repo = _parse_github_repo(url)
    return f"https://github.com/{owner}/{repo}"


def _validate_github_ref(ref: str) -> None:
    if not ref or not _GITHUB_REF_RE.fullmatch(ref) or ".." in ref or ref.endswith(".lock"):
        raise PluginInstallError(
            status_code=422,
            code="PLUGIN_INSTALL_INVALID_SOURCE",
            message="Invalid GitHub ref",
        )
