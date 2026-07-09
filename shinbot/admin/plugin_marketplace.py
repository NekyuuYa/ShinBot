"""Administrative helpers for plugin marketplace discovery."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import shutil
import time
import uuid
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import quote, urlparse

import httpx

from shinbot.admin.plugin_install import (
    PLUGIN_INSTALL_MAX_ARCHIVE_BYTES,
    PLUGIN_INSTALL_MAX_EXTRACTED_BYTES,
    PluginInstallError,
    PluginInstallManifest,
    PluginInstallRecord,
    build_plugin_install_service,
)

OFFICIAL_MARKETPLACE_SOURCE_ID = "official"
OFFICIAL_MARKETPLACE_REPOSITORY_URL = "https://github.com/NekyuuYa/shinbot-plugins"
OFFICIAL_MARKETPLACE_REF = "main"
OFFICIAL_MARKETPLACE_PLUGIN_ROOT = "plugins.json"
PLUGIN_MARKETPLACE_CACHE_TTL_SECONDS = 6 * 60 * 60
PLUGIN_MARKETPLACE_CACHE_SCHEMA_VERSION = 1
_VALID_PLUGIN_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_", "shinbot_converter_")
_VALID_ROLE_VALUES = {"logic", "adapter"}
_GITHUB_INDEX_DEFAULT_PATH = "plugins.json"
_GITHUB_INDEX_PLUGIN_REF = "HEAD"

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PluginMarketplaceError(RuntimeError):
    """Structured error raised by plugin marketplace helpers."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(frozen=True, slots=True)
class PluginMarketplaceSource:
    """One plugin marketplace source definition."""

    id: str
    name: str
    source_type: Literal["github_monorepo", "github_index"]
    repository_url: str
    ref: str
    plugin_root: str
    installer_type: str = "shinbot"
    owner_plugin_id: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return an API-friendly source payload."""
        return {
            "id": self.id,
            "name": self.name,
            "source_type": self.source_type,
            "repository_url": self.repository_url,
            "repo_url": self.repository_url,
            "ref": self.ref,
            "plugin_root": self.plugin_root,
            "installer_type": self.installer_type,
            "owner_plugin_id": self.owner_plugin_id,
        }


@dataclass(slots=True)
class PluginMarketplaceItem:
    """One discovered marketplace plugin item."""

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
    tags: list[str]
    homepage: str
    repository: str
    plugin_path: str
    source: PluginMarketplaceSource
    ref: str = ""
    installed: bool = False
    installed_version: str = ""
    installed_source: dict[str, Any] | None = None
    managed_by_webui: bool = False
    can_install: bool = True
    can_update: bool = False
    update_available: bool = False
    missing_required_dependencies: list[str] | None = None
    missing_optional_dependencies: list[str] | None = None
    warnings: list[str] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return an API-friendly marketplace item payload."""
        missing_required = self.missing_required_dependencies or []
        missing_optional = self.missing_optional_dependencies or []
        return {
            "id": self.plugin_id,
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
            "missing_required_dependencies": missing_required,
            "missing_optional_dependencies": missing_optional,
            "tags": self.tags,
            "homepage": self.homepage,
            "repository": self.repository,
            "repository_url": self.source.repository_url,
            "ref": self.ref or self.source.ref,
            "plugin_path": self.plugin_path,
            "installed": self.installed,
            "installed_version": self.installed_version,
            "installed_source": self.installed_source,
            "managed_by_webui": self.managed_by_webui,
            "can_install": self.can_install,
            "can_update": self.can_update,
            "update_available": self.update_available,
            "warnings": self.warnings or [],
        }


@dataclass(slots=True)
class PluginMarketplaceArchive:
    """One downloaded or cached marketplace repository archive."""

    content: bytes
    resolved_ref: str = ""
    cached: bool = False
    cached_at: float = 0
    expires_at: float = 0

    def cache_dict(self) -> dict[str, Any]:
        """Return cache metadata for API payloads."""
        return {
            "cached": self.cached,
            "cached_at": self.cached_at,
            "expires_at": self.expires_at,
            "ttl_seconds": PLUGIN_MARKETPLACE_CACHE_TTL_SECONDS,
        }


OFFICIAL_MARKETPLACE_SOURCE = PluginMarketplaceSource(
    id=OFFICIAL_MARKETPLACE_SOURCE_ID,
    name="ShinBot Official Plugins",
    source_type="github_index",
    repository_url=OFFICIAL_MARKETPLACE_REPOSITORY_URL,
    ref=OFFICIAL_MARKETPLACE_REF,
    plugin_root=OFFICIAL_MARKETPLACE_PLUGIN_ROOT,
)


class PluginMarketplaceService:
    """Discover and install plugins from configured marketplace sources."""

    def __init__(self, *, bot: Any, boot: Any) -> None:
        """Initialize the marketplace service."""
        self.bot = bot
        self.boot = boot
        self.data_dir = Path(boot.data_dir)
        self.plugins_dir = self.data_dir / "plugins"
        self.cache_dir = self.data_dir / "plugin_marketplace_cache"
        self.manifest = PluginInstallManifest(self.data_dir)
        self.sources: dict[str, PluginMarketplaceSource] = {
            OFFICIAL_MARKETPLACE_SOURCE.id: OFFICIAL_MARKETPLACE_SOURCE
        }
        self._plugin_installers: dict[str, dict[str, Any]] = {}

    def register_source(
        self,
        *,
        source_id: str,
        name: str,
        source_type: str = "github_monorepo",
        repository_url: str,
        ref: str = "main",
        plugin_root: str = "plugins",
        installer_type: str = "shinbot",
        owner_plugin_id: str = "",
    ) -> None:
        """Register a marketplace source from a plugin.

        Args:
            source_id:       Unique source identifier.
            name:            Display name.
            source_type:     Source type.
            repository_url:  GitHub repository URL.
            ref:             Git ref.
            plugin_root:     Plugin root directory in repo.
            installer_type:  Installer type for this source.
            owner_plugin_id: Plugin that registered this source.
        """
        if not source_id.strip():
            raise ValueError("marketplace source_id must be non-empty")
        if source_id == OFFICIAL_MARKETPLACE_SOURCE_ID and owner_plugin_id:
            raise ValueError("plugins cannot override the official marketplace source")
        if source_type not in {"github_monorepo", "github_index"}:
            raise ValueError("only github_monorepo and github_index marketplace sources are supported")
        existing = self.sources.get(source_id)
        if (
            existing is not None
            and existing.owner_plugin_id
            and existing.owner_plugin_id != owner_plugin_id
        ):
            raise ValueError(
                f"marketplace source {source_id!r} is already owned by "
                f"{existing.owner_plugin_id!r}"
            )
        source = PluginMarketplaceSource(
            id=source_id,
            name=name,
            source_type=source_type,
            repository_url=repository_url,
            ref=ref,
            plugin_root=plugin_root,
            installer_type=installer_type,
            owner_plugin_id=owner_plugin_id,
        )
        self.sources[source_id] = source
        logger.info(
            "Registered marketplace source %r from plugin %r (installer=%s)",
            source_id,
            owner_plugin_id,
            installer_type,
        )

    def register_installer(
        self,
        installer_type: str,
        *,
        owner_plugin_id: str,
        install_fn: Any,
        uninstall_fn: Any | None = None,
        validate_fn: Any | None = None,
        target_dir: Path | str | None = None,
    ) -> None:
        """Register a custom plugin installer.

        Args:
            installer_type:  Unique installer type identifier.
            owner_plugin_id: Plugin that registered this installer.
            install_fn:      Async function to install a plugin.
            uninstall_fn:    Optional async function to uninstall.
            validate_fn:     Optional function to validate metadata.
            target_dir:      Optional installer-owned target directory.
        """
        if not installer_type.strip():
            raise ValueError("installer_type must be non-empty")
        if installer_type == "shinbot":
            raise ValueError("plugins cannot override the built-in shinbot installer")
        if not callable(install_fn):
            raise TypeError("install_fn must be callable")
        if uninstall_fn is not None and not callable(uninstall_fn):
            raise TypeError("uninstall_fn must be callable")
        if validate_fn is not None and not callable(validate_fn):
            raise TypeError("validate_fn must be callable")
        self._plugin_installers[installer_type] = {
            "install": install_fn,
            "uninstall": uninstall_fn,
            "validate": validate_fn,
            "owner": owner_plugin_id,
            "target_dir": str(target_dir) if target_dir is not None else "",
        }
        logger.info(
            "Registered plugin installer %r from plugin %r",
            installer_type,
            owner_plugin_id,
        )

    def get_installer(self, installer_type: str) -> dict[str, Any] | None:
        """Get a registered installer by type."""
        return self._plugin_installers.get(installer_type)

    def list_installers(self) -> dict[str, Any]:
        """Return custom plugin installers available to WebUI install flows."""
        installers = [
            {
                "type": "shinbot",
                "name": "ShinBot",
                "owner_plugin_id": "",
                "target_dir": "",
            }
        ]
        installers.extend(
            {
                "type": installer_type,
                "name": installer_type,
                "owner_plugin_id": str(installer.get("owner") or ""),
                "target_dir": str(installer.get("target_dir") or ""),
            }
            for installer_type, installer in sorted(self._plugin_installers.items())
        )
        return {"installers": installers}

    def unregister_owner(self, owner_plugin_id: str) -> None:
        """Remove marketplace sources and installers registered by a plugin."""
        if not owner_plugin_id:
            return
        source_ids = [
            source_id
            for source_id, source in self.sources.items()
            if source.owner_plugin_id == owner_plugin_id
        ]
        for source_id in source_ids:
            self.sources.pop(source_id, None)
            logger.info(
                "Unregistered marketplace source %r from plugin %r",
                source_id,
                owner_plugin_id,
            )
        installer_types = [
            installer_type
            for installer_type, installer in self._plugin_installers.items()
            if installer.get("owner") == owner_plugin_id
        ]
        for installer_type in installer_types:
            self._plugin_installers.pop(installer_type, None)
            logger.info(
                "Unregistered plugin installer %r from plugin %r",
                installer_type,
                owner_plugin_id,
            )

    def list_sources(self) -> dict[str, Any]:
        """Return configured marketplace sources."""
        return {"sources": [source.as_dict() for source in self.sources.values()]}

    async def list_plugins(
        self,
        source_id: str = OFFICIAL_MARKETPLACE_SOURCE_ID,
        *,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """Return discovered marketplace plugins with local install state."""
        source = self._source_or_raise(source_id)
        archive = await self._load_metadata_archive(source, refresh=refresh)
        items = self._scan_monorepo_archive(source, archive.content)
        return {
            "source": source.as_dict(),
            "cache": archive.cache_dict(),
            "plugins": [self._enrich_item(item).as_dict() for item in items],
        }

    async def get_plugin(
        self,
        source_id: str,
        plugin_id: str,
        *,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """Return one discovered marketplace plugin."""
        payload = await self.list_plugins(source_id, refresh=refresh)
        for item in payload["plugins"]:
            if item["plugin_id"] == plugin_id:
                return {"source": payload["source"], "plugin": item}
        raise PluginMarketplaceError(
            status_code=404,
            code="PLUGIN_MARKETPLACE_ITEM_NOT_FOUND",
            message=f"Marketplace plugin {plugin_id!r} was not found",
        )

    async def preview_plugin(
        self,
        source_id: str,
        plugin_id: str,
        *,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """Preview installing one marketplace plugin."""
        item, archive = await self._raw_plugin_or_raise(source_id, plugin_id, refresh=refresh)
        if item.source.installer_type != "shinbot":
            return await self._preview_custom_plugin(item, archive)
        service = build_plugin_install_service(self.bot, self.boot)
        try:
            return await service.preview_github_archive(
                _item_source_url(item),
                _item_ref(item),
                archive_bytes=archive.content,
                resolved_ref=archive.resolved_ref,
                plugin_path=item.plugin_path,
            )
        except PluginInstallError as exc:
            raise PluginMarketplaceError(exc.status_code, exc.code, exc.message) from exc

    async def install_plugin(
        self,
        source_id: str,
        plugin_id: str,
        *,
        enable_after_install: bool,
        allow_overwrite: bool,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """Install one marketplace plugin through the WebUI install service."""
        item, archive = await self._raw_plugin_or_raise(source_id, plugin_id, refresh=refresh)
        if item.source.installer_type != "shinbot":
            return await self._install_custom_plugin(
                item,
                archive,
                enable_after_install=enable_after_install,
                allow_overwrite=allow_overwrite,
            )
        service = build_plugin_install_service(self.bot, self.boot)
        try:
            return await service.install_github_archive(
                _item_source_url(item),
                _item_ref(item),
                archive_bytes=archive.content,
                resolved_ref=archive.resolved_ref,
                plugin_path=item.plugin_path,
                enable_after_install=enable_after_install,
                allow_overwrite=allow_overwrite,
            )
        except PluginInstallError as exc:
            raise PluginMarketplaceError(exc.status_code, exc.code, exc.message) from exc

    async def _raw_plugin_or_raise(
        self,
        source_id: str,
        plugin_id: str,
        *,
        refresh: bool = False,
    ) -> tuple[PluginMarketplaceItem, PluginMarketplaceArchive]:
        source = self._source_or_raise(source_id)
        metadata_archive = await self._load_metadata_archive(source, refresh=refresh)
        for item in self._scan_monorepo_archive(source, metadata_archive.content):
            if item.plugin_id == plugin_id:
                plugin_archive = await self._load_plugin_archive(item, refresh=refresh)
                return item, plugin_archive
        raise PluginMarketplaceError(
            status_code=404,
            code="PLUGIN_MARKETPLACE_ITEM_NOT_FOUND",
            message=f"Marketplace plugin {plugin_id!r} was not found",
        )

    def _source_or_raise(self, source_id: str) -> PluginMarketplaceSource:
        source = self.sources.get(source_id)
        if source is None:
            raise PluginMarketplaceError(
                status_code=404,
                code="PLUGIN_MARKETPLACE_SOURCE_NOT_FOUND",
                message=f"Plugin marketplace source {source_id!r} was not found",
            )
        return source

    def _installer_or_raise(self, installer_type: str) -> dict[str, Any]:
        installer = self._plugin_installers.get(installer_type)
        if installer is None:
            raise PluginMarketplaceError(
                status_code=409,
                code="PLUGIN_MARKETPLACE_INSTALLER_NOT_FOUND",
                message=f"Plugin installer {installer_type!r} was not found",
            )
        return installer

    def _metadata_sparse_paths(self, source: PluginMarketplaceSource) -> list[str]:
        root = source.plugin_root.strip().strip("/")
        prefix = "" if root in {"", "."} else f"{root}/"
        if source.installer_type == "shinbot":
            return [f"{prefix}*/metadata.json"]
        return [
            f"{prefix}*/metadata.json",
            f"{prefix}*/metadata.yaml",
            f"{prefix}*/metadata.yml",
        ]

    async def _preview_custom_plugin(
        self,
        item: PluginMarketplaceItem,
        archive: PluginMarketplaceArchive,
    ) -> dict[str, Any]:
        service = build_plugin_install_service(self.bot, self.boot)
        task_id = f"preview-{uuid.uuid4().hex}"
        try:
            _, extract_root = service._prepare_archive_workspace(task_id, archive.content)
            plugin_root = self._custom_plugin_root_from_archive(item, extract_root)
            metadata = self._validate_custom_plugin_root(item.source, plugin_root)
            return self._custom_preview_payload(item, archive, metadata)
        except PluginInstallError as exc:
            raise PluginMarketplaceError(exc.status_code, exc.code, exc.message) from exc
        finally:
            shutil.rmtree(service.tmp_dir / task_id, ignore_errors=True)

    async def _install_custom_plugin(
        self,
        item: PluginMarketplaceItem,
        archive: PluginMarketplaceArchive,
        *,
        enable_after_install: bool,
        allow_overwrite: bool,
    ) -> dict[str, Any]:
        installer = self._installer_or_raise(item.source.installer_type)
        service = build_plugin_install_service(self.bot, self.boot)
        task = service.task_registry.create()
        try:
            service.task_registry.update(
                task,
                status="running",
                stage="extracting",
                message="Extracting plugin archive",
                plugin_id=item.plugin_id,
            )
            _, extract_root = service._prepare_archive_workspace(task.task_id, archive.content)
            plugin_root = self._custom_plugin_root_from_archive(item, extract_root)
            metadata = self._validate_custom_plugin_root(item.source, plugin_root)
            plugin_id = _custom_plugin_id(metadata, fallback=item.plugin_id)
            target_dir = self._custom_installer_target_dir(installer)
            if target_dir is not None:
                target = self._custom_plugin_target(target_dir, plugin_id)
                if target.exists() and not allow_overwrite:
                    raise PluginMarketplaceError(
                        status_code=409,
                        code="PLUGIN_MARKETPLACE_TARGET_EXISTS",
                        message=f"Plugin {plugin_id!r} already exists",
                    )
            service.task_registry.update(
                task,
                stage="installing",
                message="Installing plugin files",
                plugin_id=plugin_id,
            )
            source_info = {
                "source": item.source.as_dict(),
                "plugin": item.as_dict(),
                "resolved_ref": archive.resolved_ref,
            }
            result = await self._call_registered_function(
                installer["install"],
                plugin_root,
                target_dir=target_dir,
                source_info=source_info,
            )
            if result is False:
                raise PluginMarketplaceError(
                    status_code=500,
                    code="PLUGIN_MARKETPLACE_INSTALL_FAILED",
                    message=f"Plugin installer {item.source.installer_type!r} reported failure",
                )
            self._save_custom_install_record(item, archive, metadata, plugin_id)
            if enable_after_install:
                await self._reload_installer_owner(installer)
            service.task_registry.update(
                task,
                status="succeeded",
                stage="succeeded",
                message="Plugin installed",
                plugin_id=plugin_id,
            )
            return task.as_dict()
        except PluginMarketplaceError as exc:
            service._fail_task(
                task,
                PluginInstallError(exc.status_code, exc.code, exc.message),
            )
            raise
        except PluginInstallError as exc:
            service._fail_task(task, exc)
            raise PluginMarketplaceError(exc.status_code, exc.code, exc.message) from exc
        except Exception as exc:
            error = PluginMarketplaceError(
                status_code=500,
                code="PLUGIN_MARKETPLACE_INSTALL_FAILED",
                message=str(exc),
            )
            service._fail_task(
                task,
                PluginInstallError(error.status_code, error.code, error.message),
            )
            raise error from exc
        finally:
            shutil.rmtree(service.tmp_dir / task.task_id, ignore_errors=True)

    async def preview_custom_github(
        self,
        *,
        installer_type: str,
        repository_url: str,
        ref: str = "main",
        plugin_path: str = "",
    ) -> dict[str, Any]:
        """Preview a custom-installer plugin from a direct GitHub repository."""
        source = self._direct_custom_source(
            installer_type=installer_type,
            source_url=repository_url,
            ref=ref,
        )
        archive = await self._load_github_zip_archive(
            source,
            cache_scope="direct",
        )
        item, metadata = self._custom_item_from_archive(
            source,
            archive,
            plugin_path=plugin_path,
        )
        return self._custom_preview_payload(item, archive, metadata)

    async def install_custom_github(
        self,
        *,
        installer_type: str,
        repository_url: str,
        ref: str = "main",
        plugin_path: str = "",
        enable_after_install: bool = True,
        allow_overwrite: bool = False,
    ) -> dict[str, Any]:
        """Install a custom-installer plugin from a direct GitHub repository."""
        source = self._direct_custom_source(
            installer_type=installer_type,
            source_url=repository_url,
            ref=ref,
        )
        archive = await self._load_github_zip_archive(
            source,
            cache_scope="direct",
        )
        item, _ = self._custom_item_from_archive(
            source,
            archive,
            plugin_path=plugin_path,
        )
        return await self._install_custom_plugin(
            item,
            archive,
            enable_after_install=enable_after_install,
            allow_overwrite=allow_overwrite,
        )

    async def preview_custom_archive(
        self,
        *,
        installer_type: str,
        archive_bytes: bytes,
        filename: str = "",
    ) -> dict[str, Any]:
        """Preview a custom-installer plugin from an uploaded archive."""
        source = self._direct_custom_source(
            installer_type=installer_type,
            source_url=filename or "uploaded_archive",
            ref="",
        )
        archive = PluginMarketplaceArchive(content=archive_bytes, resolved_ref="")
        item, metadata = self._custom_item_from_archive(source, archive, plugin_path="")
        return self._custom_preview_payload(item, archive, metadata)

    async def install_custom_archive(
        self,
        *,
        installer_type: str,
        archive_bytes: bytes,
        filename: str = "",
        enable_after_install: bool = True,
        allow_overwrite: bool = False,
    ) -> dict[str, Any]:
        """Install a custom-installer plugin from an uploaded archive."""
        source = self._direct_custom_source(
            installer_type=installer_type,
            source_url=filename or "uploaded_archive",
            ref="",
        )
        archive = PluginMarketplaceArchive(content=archive_bytes, resolved_ref="")
        item, _ = self._custom_item_from_archive(source, archive, plugin_path="")
        return await self._install_custom_plugin(
            item,
            archive,
            enable_after_install=enable_after_install,
            allow_overwrite=allow_overwrite,
        )

    def _direct_custom_source(
        self,
        *,
        installer_type: str,
        source_url: str,
        ref: str,
    ) -> PluginMarketplaceSource:
        if installer_type == "shinbot":
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INSTALLER_INVALID",
                message="Direct custom install requires a non-shinbot installer",
            )
        self._installer_or_raise(installer_type)
        return PluginMarketplaceSource(
            id=f"direct:{installer_type}",
            name=f"Direct {installer_type} install",
            source_type="github_monorepo",
            repository_url=source_url,
            ref=ref,
            plugin_root="",
            installer_type=installer_type,
        )

    def _custom_item_from_archive(
        self,
        source: PluginMarketplaceSource,
        archive: PluginMarketplaceArchive,
        *,
        plugin_path: str,
    ) -> tuple[PluginMarketplaceItem, dict[str, Any]]:
        service = build_plugin_install_service(self.bot, self.boot)
        task_id = f"inspect-{uuid.uuid4().hex}"
        try:
            _, extract_root = service._prepare_archive_workspace(task_id, archive.content)
            placeholder = PluginMarketplaceItem(
                plugin_id="",
                name="",
                version="",
                description="",
                author="",
                role="logic",
                entry="",
                permissions=[],
                required_dependencies=[],
                optional_dependencies=[],
                legacy_dependencies=[],
                tags=[],
                homepage="",
                repository=source.repository_url,
                plugin_path=plugin_path,
                source=source,
                ref=source.ref,
            )
            plugin_root = self._custom_plugin_root_from_archive(placeholder, extract_root)
            metadata = self._validate_custom_plugin_root(source, plugin_root)
            item = self._custom_item_from_metadata(
                source,
                metadata,
                metadata_name="",
                plugin_path=plugin_path,
            )
            item.repository = source.repository_url
            item.ref = source.ref
            return item, metadata
        except PluginInstallError as exc:
            raise PluginMarketplaceError(exc.status_code, exc.code, exc.message) from exc
        finally:
            shutil.rmtree(service.tmp_dir / task_id, ignore_errors=True)

    def _custom_plugin_root_from_archive(
        self,
        item: PluginMarketplaceItem,
        extract_root: Path,
    ) -> Path:
        service = build_plugin_install_service(self.bot, self.boot)
        if item.plugin_path:
            return service._repo_relative_path(extract_root, item.plugin_path)
        roots = [extract_root]
        roots.extend(path for path in sorted(extract_root.iterdir()) if path.is_dir())
        metadata_names = {"metadata.yaml", "metadata.yml", "metadata.json"}
        matches = [path for path in roots if any((path / name).is_file() for name in metadata_names)]
        if not matches:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Plugin archive does not contain custom plugin metadata",
            )
        if len(matches) > 1:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Plugin archive contains multiple custom plugin metadata roots",
            )
        return matches[0]

    async def uninstall_installed_plugin(
        self,
        plugin_id: str,
        record: PluginInstallRecord,
    ) -> None:
        """Uninstall a marketplace-managed custom plugin record."""
        installer_type = record.installer_type
        installer = self._installer_or_raise(installer_type)
        uninstall_fn = installer.get("uninstall")
        if uninstall_fn is None:
            raise PluginMarketplaceError(
                status_code=409,
                code="PLUGIN_MARKETPLACE_UNINSTALL_UNSUPPORTED",
                message=f"Plugin installer {installer_type!r} does not support uninstall",
            )
        target_dir = self._custom_installer_target_dir(installer)
        result = await self._call_registered_function(
            uninstall_fn,
            plugin_id,
            target_dir=target_dir,
            source_info=record.as_dict(),
        )
        if result is False:
            raise PluginMarketplaceError(
                status_code=500,
                code="PLUGIN_MARKETPLACE_UNINSTALL_FAILED",
                message=f"Plugin installer {installer_type!r} reported failure",
            )
        await self._reload_installer_owner(installer)

    def _validate_custom_plugin_root(
        self,
        source: PluginMarketplaceSource,
        plugin_root: Path,
    ) -> dict[str, Any]:
        installer = self._installer_or_raise(source.installer_type)
        validate_fn = installer.get("validate")
        if validate_fn is None:
            return {
                "id": plugin_root.name,
                "name": plugin_root.name,
                "version": "0.0.0",
                "role": "logic",
            }
        try:
            metadata = validate_fn(plugin_root)
        except Exception as exc:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Custom marketplace metadata is invalid: {exc}",
            ) from exc
        if inspect.isawaitable(metadata):
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Custom marketplace validate_fn must be synchronous",
            )
        if metadata is None:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Custom marketplace metadata validator returned no metadata",
            )
        if not isinstance(metadata, dict):
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Custom marketplace metadata validator must return an object",
            )
        return dict(metadata)

    def _custom_preview_payload(
        self,
        item: PluginMarketplaceItem,
        archive: PluginMarketplaceArchive,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        plugin_id = _custom_plugin_id(metadata, fallback=item.plugin_id)
        target_dir = self._custom_installer_target_dir(
            self._installer_or_raise(item.source.installer_type)
        )
        target_exists = False
        if target_dir is not None:
            target_exists = self._custom_plugin_target(target_dir, plugin_id).exists()
        required = _custom_metadata_string_list(metadata, "required_dependencies")
        optional = _custom_metadata_string_list(metadata, "optional_dependencies")
        legacy = _custom_metadata_string_list(metadata, "dependencies")
        available_ids = self._available_plugin_ids()
        missing_required = sorted(dep for dep in required if dep not in available_ids)
        missing_optional = sorted(dep for dep in optional if dep not in available_ids)
        warnings = [f"Optional dependency {dep!r} is not installed" for dep in missing_optional]
        warnings.extend(
            f"Legacy dependency {dep!r} is not installed"
            for dep in legacy
            if dep not in available_ids
        )
        return {
            "plugin_id": plugin_id,
            "name": _custom_metadata_display_name(metadata, plugin_id),
            "version": _custom_metadata_string(metadata, "version", "0.0.0"),
            "description": _custom_metadata_description(metadata),
            "author": _custom_metadata_string(metadata, "author", ""),
            "role": _custom_metadata_role(metadata),
            "entry": _custom_metadata_string(metadata, "entry", ""),
            "permissions": _custom_metadata_string_list(metadata, "permissions"),
            "required_dependencies": required,
            "optional_dependencies": optional,
            "legacy_dependencies": legacy,
            "missing_required_dependencies": missing_required,
            "missing_optional_dependencies": missing_optional,
            "source_type": "marketplace",
            "source_url": _item_source_url(item),
            "ref": _item_ref(item),
            "resolved_ref": archive.resolved_ref,
            "plugin_path": item.plugin_path,
            "installer_type": item.source.installer_type,
            "archive_sha256": hashlib.sha256(archive.content).hexdigest(),
            "target_exists": target_exists,
            "target_managed_by_webui": target_exists,
            "can_install": bool(not target_exists and not missing_required),
            "warnings": warnings,
        }

    def _save_custom_install_record(
        self,
        item: PluginMarketplaceItem,
        archive: PluginMarketplaceArchive,
        metadata: dict[str, Any],
        plugin_id: str,
    ) -> None:
        records = self.manifest.load()
        now = time.time()
        previous = records.get(plugin_id)
        installed_at = previous.installed_at if previous is not None else now
        records[plugin_id] = PluginInstallRecord(
            plugin_id=plugin_id,
            source_type="marketplace",
            source_url=_item_source_url(item),
            ref=_item_ref(item),
            resolved_ref=archive.resolved_ref,
            plugin_path=item.plugin_path,
            installed_at=installed_at,
            updated_at=now,
            installed_version=_custom_metadata_string(metadata, "version", "0.0.0"),
            managed_by_webui=True,
            archive_sha256=hashlib.sha256(archive.content).hexdigest(),
            installer_type=item.source.installer_type,
            marketplace_source_id=item.source.id,
        )
        self.manifest.save(records)

    def _custom_installer_target_dir(self, installer: dict[str, Any]) -> Path | None:
        target_dir = str(installer.get("target_dir") or "").strip()
        return Path(target_dir) if target_dir else None

    def _custom_plugin_target(self, target_dir: Path, plugin_id: str) -> Path:
        root = target_dir.resolve()
        target = (root / plugin_id).resolve()
        if target == root or not target.is_relative_to(root):
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_PLUGIN_ID_INVALID",
                message=f"Invalid custom marketplace plugin id: {plugin_id!r}",
            )
        return target

    async def _call_registered_function(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        filtered_kwargs = kwargs
        try:
            signature = inspect.signature(func)
        except (TypeError, ValueError):
            signature = None
        if signature is not None and not any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in signature.parameters.values()
        ):
            filtered_kwargs = {
                key: value
                for key, value in kwargs.items()
                if key in signature.parameters and value is not None
            }
        result = func(*args, **filtered_kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    async def _reload_installer_owner(self, installer: dict[str, Any]) -> None:
        owner = str(installer.get("owner") or "")
        if not owner or self.bot.plugin_manager.get_plugin(owner) is None:
            return
        await self.bot.plugin_manager.reload_plugin_async(owner)

    async def _load_sparse_archive(
        self,
        source: PluginMarketplaceSource,
        *,
        sparse_paths: list[str],
        cache_scope: str,
        refresh: bool = False,
    ) -> PluginMarketplaceArchive:
        if not refresh:
            cached = self._load_cached_archive(source, cache_scope=cache_scope)
            if cached is not None:
                return cached

        archive = await self._create_sparse_archive(source, sparse_paths=sparse_paths)
        if len(archive.content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginMarketplaceError(
                status_code=413,
                code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                message="Plugin marketplace archive is too large",
            )
        return self._save_cached_archive(
            source,
            archive.content,
            cache_scope=cache_scope,
            resolved_ref=archive.resolved_ref,
        )

    async def _load_metadata_archive(
        self,
        source: PluginMarketplaceSource,
        *,
        refresh: bool = False,
    ) -> PluginMarketplaceArchive:
        if source.source_type == "github_index":
            return await self._load_github_index_archive(source, refresh=refresh)
        return await self._load_sparse_archive(
            source,
            sparse_paths=self._metadata_sparse_paths(source),
            cache_scope="metadata",
            refresh=refresh,
        )

    async def _load_plugin_archive(
        self,
        item: PluginMarketplaceItem,
        *,
        refresh: bool = False,
    ) -> PluginMarketplaceArchive:
        if item.source.source_type == "github_index":
            source = PluginMarketplaceSource(
                id=f"{item.source.id}:{item.plugin_id}",
                name=item.name,
                source_type="github_monorepo",
                repository_url=item.repository,
                ref=item.ref or _GITHUB_INDEX_PLUGIN_REF,
                plugin_root="",
                installer_type=item.source.installer_type,
                owner_plugin_id=item.source.owner_plugin_id,
            )
            return await self._load_github_zip_archive(
                source,
                cache_scope="plugin",
                refresh=refresh,
            )
        return await self._load_sparse_archive(
            item.source,
            sparse_paths=[f"{item.plugin_path}/**"],
            cache_scope=f"plugin:{item.plugin_path}",
            refresh=refresh,
        )

    async def _load_github_index_archive(
        self,
        source: PluginMarketplaceSource,
        *,
        refresh: bool = False,
    ) -> PluginMarketplaceArchive:
        if not refresh:
            cached = self._load_cached_archive(source, cache_scope="metadata")
            if cached is not None:
                return cached

        index_path = _github_index_path(source.plugin_root)
        content, resolved_ref = await self._download_github_file(source, index_path)
        archive_content = _zip_single_file(f"repo/{index_path}", content)
        if len(archive_content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginMarketplaceError(
                status_code=413,
                code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                message="Plugin marketplace index is too large",
            )
        return self._save_cached_archive(
            source,
            archive_content,
            cache_scope="metadata",
            resolved_ref=resolved_ref,
        )

    async def _load_github_zip_archive(
        self,
        source: PluginMarketplaceSource,
        *,
        cache_scope: str,
        refresh: bool = False,
    ) -> PluginMarketplaceArchive:
        if not refresh:
            cached = self._load_cached_archive(source, cache_scope=cache_scope)
            if cached is not None:
                return cached

        archive = await self._download_github_zip_archive(source)
        if len(archive.content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginMarketplaceError(
                status_code=413,
                code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                message="Plugin marketplace archive is too large",
            )
        return self._save_cached_archive(
            source,
            archive.content,
            cache_scope=cache_scope,
            resolved_ref=archive.resolved_ref,
        )

    async def _download_github_file(
        self,
        source: PluginMarketplaceSource,
        index_path: str,
    ) -> tuple[bytes, str]:
        owner, repo = _parse_github_repo(source.repository_url)
        _validate_github_ref(source.ref)
        url = (
            f"https://raw.githubusercontent.com/{quote(owner)}/{quote(repo)}/"
            f"{quote(source.ref, safe='')}/{quote(index_path, safe='/')}"
        )
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(20.0, connect=8.0),
                headers={"User-Agent": "ShinBot-WebUI-Plugin-Marketplace"},
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise PluginMarketplaceError(
                status_code=502,
                code="PLUGIN_MARKETPLACE_SOURCE_UNAVAILABLE",
                message=f"Failed to fetch GitHub marketplace index: {exc.response.status_code}",
            ) from exc
        except httpx.HTTPError as exc:
            raise PluginMarketplaceError(
                status_code=502,
                code="PLUGIN_MARKETPLACE_SOURCE_UNAVAILABLE",
                message=f"Failed to fetch GitHub marketplace index: {exc}",
            ) from exc
        return response.content, source.ref

    async def _download_github_zip_archive(
        self,
        source: PluginMarketplaceSource,
    ) -> PluginMarketplaceArchive:
        owner, repo = _parse_github_repo(source.repository_url)
        _validate_github_ref(source.ref)
        url = _github_codeload_zip_url(owner, repo, source.ref)
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={"User-Agent": "ShinBot-WebUI-Plugin-Marketplace"},
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise PluginMarketplaceError(
                status_code=502,
                code="PLUGIN_MARKETPLACE_SOURCE_UNAVAILABLE",
                message=f"Failed to download GitHub plugin archive: {exc}",
            ) from exc
        return PluginMarketplaceArchive(content=response.content, resolved_ref=source.ref)

    def _load_cached_archive(
        self,
        source: PluginMarketplaceSource,
        *,
        cache_scope: str,
    ) -> PluginMarketplaceArchive | None:
        archive_path, metadata_path = self._cache_paths(source, cache_scope=cache_scope)
        if not archive_path.is_file() or not metadata_path.is_file():
            return None
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            if not isinstance(metadata, dict):
                return None
            if metadata.get("schema_version") != PLUGIN_MARKETPLACE_CACHE_SCHEMA_VERSION:
                return None
            expires_at = float(metadata.get("expires_at", 0))
            if expires_at <= time.time():
                return None
            content = archive_path.read_bytes()
        except Exception:
            return None
        if len(content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            return None
        return PluginMarketplaceArchive(
            content=content,
            resolved_ref=str(metadata.get("resolved_ref", "")),
            cached=True,
            cached_at=float(metadata.get("cached_at", 0)),
            expires_at=expires_at,
        )

    def _save_cached_archive(
        self,
        source: PluginMarketplaceSource,
        content: bytes,
        *,
        cache_scope: str,
        resolved_ref: str,
    ) -> PluginMarketplaceArchive:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        archive_path, metadata_path = self._cache_paths(source, cache_scope=cache_scope)
        now = time.time()
        expires_at = now + PLUGIN_MARKETPLACE_CACHE_TTL_SECONDS
        archive_tmp = archive_path.with_suffix(".zip.tmp")
        metadata_tmp = metadata_path.with_suffix(".json.tmp")
        archive_tmp.write_bytes(content)
        metadata = {
            "schema_version": PLUGIN_MARKETPLACE_CACHE_SCHEMA_VERSION,
            "source": source.as_dict(),
            "resolved_ref": resolved_ref,
            "cache_scope": cache_scope,
            "cached_at": now,
            "expires_at": expires_at,
            "archive_sha256": hashlib.sha256(content).hexdigest(),
            "archive_size": len(content),
        }
        metadata_tmp.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        archive_tmp.replace(archive_path)
        metadata_tmp.replace(metadata_path)
        return PluginMarketplaceArchive(
            content=content,
            resolved_ref=resolved_ref,
            cached=False,
            cached_at=now,
            expires_at=expires_at,
        )

    async def _create_sparse_archive(
        self,
        source: PluginMarketplaceSource,
        *,
        sparse_paths: list[str],
    ) -> PluginMarketplaceArchive:
        _parse_github_repo(source.repository_url)
        _validate_github_ref(source.ref)
        normalized_paths = [_normalize_sparse_path(path) for path in sparse_paths]
        workspace = self.cache_dir / f".sparse-{uuid.uuid4().hex}"
        repo_dir = workspace / "repo"
        try:
            repo_dir.mkdir(parents=True, exist_ok=True)
            await self._run_git("init", str(repo_dir))
            await self._run_git("-C", str(repo_dir), "remote", "add", "origin", source.repository_url)
            await self._run_git("-C", str(repo_dir), "config", "advice.detachedHead", "false")
            await self._run_git(
                "-C",
                str(repo_dir),
                "fetch",
                "--depth=1",
                "--filter=blob:none",
                "origin",
                source.ref,
            )
            await self._run_git("-C", str(repo_dir), "sparse-checkout", "init", "--no-cone")
            await self._run_git(
                "-C",
                str(repo_dir),
                "sparse-checkout",
                "set",
                "--no-cone",
                *normalized_paths,
            )
            await self._run_git("-C", str(repo_dir), "checkout", "--detach", "FETCH_HEAD")
            resolved_ref = (
                await self._run_git("-C", str(repo_dir), "rev-parse", "HEAD")
            ).strip()
            return PluginMarketplaceArchive(
                content=self._zip_sparse_checkout(repo_dir),
                resolved_ref=resolved_ref,
            )
        finally:
            shutil.rmtree(workspace, ignore_errors=True)

    async def _run_git(self, *args: str) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                "git",
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise PluginMarketplaceError(
                status_code=500,
                code="PLUGIN_MARKETPLACE_GIT_UNAVAILABLE",
                message=f"Failed to start git: {exc}",
            ) from exc
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
        except TimeoutError as exc:
            process.kill()
            await process.wait()
            raise PluginMarketplaceError(
                status_code=504,
                code="PLUGIN_MARKETPLACE_GIT_FAILED",
                message="Git sparse checkout timed out",
            ) from exc
        if process.returncode != 0:
            error = stderr.decode("utf-8", errors="replace").strip()
            raise PluginMarketplaceError(
                status_code=502,
                code="PLUGIN_MARKETPLACE_GIT_FAILED",
                message=error or "Git sparse checkout failed",
            )
        return stdout.decode("utf-8", errors="replace")

    def _zip_sparse_checkout(self, repo_dir: Path) -> bytes:
        total_size = 0
        output = BytesIO()
        with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(repo_dir.rglob("*")):
                if path.is_dir() or ".git" in path.relative_to(repo_dir).parts:
                    continue
                if path.is_symlink():
                    raise PluginMarketplaceError(
                        status_code=409,
                        code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                        message="Marketplace plugin sources must not contain symlinks",
                    )
                relative = path.relative_to(repo_dir).as_posix()
                total_size += path.stat().st_size
                if total_size > PLUGIN_INSTALL_MAX_EXTRACTED_BYTES:
                    raise PluginMarketplaceError(
                        status_code=413,
                        code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                        message="Marketplace plugin source is too large",
                    )
                archive.write(path, f"repo/{relative}")
        content = output.getvalue()
        if len(content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginMarketplaceError(
                status_code=413,
                code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                message="Marketplace plugin archive is too large",
            )
        return content

    def _cache_paths(
        self,
        source: PluginMarketplaceSource,
        *,
        cache_scope: str,
    ) -> tuple[Path, Path]:
        cache_key = hashlib.sha256(
            "\0".join(
                [
                    source.id,
                    source.source_type,
                    source.repository_url,
                    source.ref,
                    source.plugin_root,
                    source.installer_type,
                    cache_scope,
                ]
            ).encode("utf-8")
        ).hexdigest()
        return self.cache_dir / f"{cache_key}.zip", self.cache_dir / f"{cache_key}.json"

    def _scan_monorepo_archive(
        self,
        source: PluginMarketplaceSource,
        archive_bytes: bytes,
    ) -> list[PluginMarketplaceItem]:
        if source.source_type == "github_index":
            return self._scan_github_index_archive(source, archive_bytes)
        if source.installer_type != "shinbot":
            return self._scan_custom_monorepo_archive(source, archive_bytes)
        try:
            with zipfile.ZipFile(BytesIO(archive_bytes)) as archive:
                metadata_names = sorted(
                    name
                    for name in archive.namelist()
                    if PurePosixPath(name).name == "metadata.json"
                )
                items = [
                    self._item_from_metadata_file(source, archive, name)
                    for name in metadata_names
                ]
        except PluginMarketplaceError:
            raise
        except (OSError, zipfile.BadZipFile, json.JSONDecodeError) as exc:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Invalid plugin marketplace archive: {exc}",
            ) from exc
        if not items:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Plugin marketplace source did not contain plugin metadata",
            )
        return sorted(items, key=lambda item: item.plugin_id)

    def _scan_github_index_archive(
        self,
        source: PluginMarketplaceSource,
        archive_bytes: bytes,
    ) -> list[PluginMarketplaceItem]:
        self._installer_or_raise(source.installer_type)
        index_path = _github_index_path(source.plugin_root)
        try:
            with zipfile.ZipFile(BytesIO(archive_bytes)) as archive:
                payload = json.loads(archive.read(f"repo/{index_path}").decode("utf-8"))
        except KeyError as exc:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Plugin marketplace index {index_path!r} was not found",
            ) from exc
        except (OSError, zipfile.BadZipFile, json.JSONDecodeError) as exc:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Invalid plugin marketplace index: {exc}",
            ) from exc
        if not isinstance(payload, dict):
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Plugin marketplace index must contain an object",
            )

        items: list[PluginMarketplaceItem] = []
        seen_plugin_ids: set[str] = set()
        for raw_plugin_id, raw_metadata in payload.items():
            if not isinstance(raw_plugin_id, str) or not isinstance(raw_metadata, dict):
                continue
            try:
                item = self._github_index_item_from_metadata(
                    source,
                    raw_plugin_id,
                    raw_metadata,
                )
            except PluginMarketplaceError:
                continue
            if item.plugin_id in seen_plugin_ids:
                continue
            seen_plugin_ids.add(item.plugin_id)
            items.append(item)
        if not items:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Plugin marketplace source did not contain plugin metadata",
            )
        return sorted(items, key=lambda item: item.plugin_id)

    def _scan_custom_monorepo_archive(
        self,
        source: PluginMarketplaceSource,
        archive_bytes: bytes,
    ) -> list[PluginMarketplaceItem]:
        self._installer_or_raise(source.installer_type)
        workspace = self.cache_dir / f".metadata-{uuid.uuid4().hex}"
        items: list[PluginMarketplaceItem] = []
        seen_plugin_paths: set[str] = set()
        try:
            with zipfile.ZipFile(BytesIO(archive_bytes)) as archive:
                metadata_names = sorted(
                    name
                    for name in archive.namelist()
                    if PurePosixPath(name).name in {"metadata.yaml", "metadata.yml", "metadata.json"}
                )
                for metadata_name in metadata_names:
                    plugin_path = _plugin_path_from_metadata_name(metadata_name, source.plugin_root)
                    if plugin_path in seen_plugin_paths:
                        continue
                    seen_plugin_paths.add(plugin_path)
                    metadata_filename = PurePosixPath(metadata_name).name
                    plugin_dir = _safe_workspace_child(workspace, plugin_path)
                    plugin_dir.mkdir(parents=True, exist_ok=True)
                    (plugin_dir / metadata_filename).write_bytes(archive.read(metadata_name))
                    try:
                        metadata = self._validate_custom_plugin_root(source, plugin_dir)
                    except PluginMarketplaceError:
                        continue
                    items.append(
                        self._custom_item_from_metadata(
                            source,
                            metadata,
                            metadata_name=metadata_name,
                            plugin_path=plugin_path,
                        )
                    )
        except PluginMarketplaceError:
            raise
        except (OSError, zipfile.BadZipFile) as exc:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Invalid plugin marketplace archive: {exc}",
            ) from exc
        finally:
            shutil.rmtree(workspace, ignore_errors=True)
        if not items:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message="Plugin marketplace source did not contain plugin metadata",
            )
        return sorted(items, key=lambda item: item.plugin_id)

    def _item_from_metadata_file(
        self,
        source: PluginMarketplaceSource,
        archive: zipfile.ZipFile,
        metadata_name: str,
    ) -> PluginMarketplaceItem:
        try:
            payload = json.loads(archive.read(metadata_name).decode("utf-8"))
        except Exception as exc:
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Invalid marketplace plugin metadata {metadata_name!r}: {exc}",
            ) from exc
        if not isinstance(payload, dict):
            raise PluginMarketplaceError(
                status_code=422,
                code="PLUGIN_MARKETPLACE_INDEX_INVALID",
                message=f"Marketplace metadata {metadata_name!r} must contain an object",
            )
        plugin_path = _plugin_path_from_metadata_name(metadata_name, source.plugin_root)
        metadata = _validate_marketplace_metadata(payload, metadata_name)
        return PluginMarketplaceItem(
            plugin_id=metadata["id"],
            name=metadata["name"],
            version=metadata["version"],
            description=metadata["description"],
            author=metadata["author"],
            role=metadata["role"],
            entry=metadata["entry"],
            permissions=metadata["permissions"],
            required_dependencies=metadata["required_dependencies"],
            optional_dependencies=metadata["optional_dependencies"],
            legacy_dependencies=metadata["dependencies"],
            tags=metadata["tags"],
            homepage=metadata["homepage"],
            repository=metadata["repository"] or source.repository_url,
            plugin_path=plugin_path,
            source=source,
        )

    def _custom_item_from_metadata(
        self,
        source: PluginMarketplaceSource,
        metadata: dict[str, Any],
        *,
        metadata_name: str,
        plugin_path: str,
    ) -> PluginMarketplaceItem:
        plugin_id = _custom_plugin_id(metadata, fallback=PurePosixPath(plugin_path).name)
        return PluginMarketplaceItem(
            plugin_id=plugin_id,
            name=_custom_metadata_display_name(metadata, plugin_id),
            version=_custom_metadata_string(metadata, "version", "0.0.0"),
            description=_custom_metadata_description(metadata),
            author=_custom_metadata_string(metadata, "author", ""),
            role=_custom_metadata_role(metadata),
            entry=_custom_metadata_string(metadata, "entry", ""),
            permissions=_custom_metadata_string_list(metadata, "permissions"),
            required_dependencies=_custom_metadata_string_list(metadata, "required_dependencies"),
            optional_dependencies=_custom_metadata_string_list(metadata, "optional_dependencies"),
            legacy_dependencies=_custom_metadata_string_list(metadata, "dependencies"),
            tags=_custom_metadata_string_list(metadata, "tags"),
            homepage=_custom_metadata_string(metadata, "homepage", ""),
            repository=_custom_metadata_string(metadata, "repository", source.repository_url),
            plugin_path=plugin_path,
            source=source,
        )

    def _github_index_item_from_metadata(
        self,
        source: PluginMarketplaceSource,
        raw_plugin_id: str,
        metadata: dict[str, Any],
    ) -> PluginMarketplaceItem:
        plugin_id = _custom_plugin_id(metadata, fallback=raw_plugin_id)
        repository = _normalize_index_repository_url(
            _custom_metadata_string(metadata, "repo", "")
            or _custom_metadata_string(metadata, "repository", "")
        )
        _parse_github_repo(repository)
        ref = _custom_metadata_string(metadata, "ref", "") or _custom_metadata_string(
            metadata,
            "branch",
            "",
        )
        if ref:
            _validate_github_ref(ref)
        plugin_path = _normalize_index_plugin_path(
            _custom_metadata_string(metadata, "plugin_path", "")
            or _custom_metadata_string(metadata, "path", "")
        )
        return PluginMarketplaceItem(
            plugin_id=plugin_id,
            name=_custom_metadata_display_name(metadata, plugin_id),
            version=_custom_metadata_string(metadata, "version", "0.0.0"),
            description=_custom_metadata_description(metadata),
            author=_custom_metadata_string(metadata, "author", ""),
            role=_custom_metadata_role(metadata),
            entry=_custom_metadata_string(metadata, "entry", ""),
            permissions=_custom_metadata_string_list(metadata, "permissions"),
            required_dependencies=_custom_metadata_string_list(metadata, "required_dependencies"),
            optional_dependencies=_custom_metadata_string_list(metadata, "optional_dependencies"),
            legacy_dependencies=_custom_metadata_string_list(metadata, "dependencies"),
            tags=_custom_metadata_string_list(metadata, "tags"),
            homepage=_custom_metadata_string(metadata, "social_link", "")
            or _custom_metadata_string(metadata, "homepage", ""),
            repository=repository,
            plugin_path=plugin_path,
            source=source,
            ref=ref or _GITHUB_INDEX_PLUGIN_REF,
        )

    def _enrich_item(self, item: PluginMarketplaceItem) -> PluginMarketplaceItem:
        if item.source.installer_type != "shinbot":
            return self._enrich_custom_item(item)
        available_ids = self._available_plugin_ids()
        records = self._manifest_records()
        record = records.get(item.plugin_id)
        target_exists = (self.plugins_dir / item.plugin_id).exists()
        loaded_plugin = self.bot.plugin_manager.get_plugin(item.plugin_id)
        installed = bool(target_exists or loaded_plugin is not None)
        installed_version = ""
        if record is not None:
            installed_version = record.installed_version
        elif loaded_plugin is not None:
            installed_version = str(getattr(loaded_plugin, "version", ""))
        elif target_exists:
            installed_version = self._metadata_version(target_exists_path=self.plugins_dir / item.plugin_id)

        missing_required = sorted(
            dep for dep in item.required_dependencies if dep not in available_ids
        )
        missing_optional = sorted(
            dep for dep in item.optional_dependencies if dep not in available_ids
        )
        warnings: list[str] = []
        for dep in missing_optional:
            warnings.append(f"Optional dependency {dep!r} is not installed")
        for dep in item.legacy_dependencies:
            if dep not in available_ids:
                warnings.append(f"Legacy dependency {dep!r} is not installed")

        managed = bool(record and record.managed_by_webui)
        same_source = bool(
            record
            and record.source_type == "github"
            and record.source_url == _item_source_url(item)
            and (record.ref or _item_ref(item)) == _item_ref(item)
            and record.plugin_path == item.plugin_path
        )
        update_available = bool(installed and managed and same_source and installed_version != item.version)
        if installed and not managed:
            warnings.append("Plugin is already installed locally and is not WebUI-managed")
        elif installed and managed and not same_source:
            warnings.append("Plugin is installed from a different WebUI source")

        item.installed = installed
        item.installed_version = installed_version
        item.installed_source = record.as_dict() if record is not None else None
        item.managed_by_webui = managed
        item.missing_required_dependencies = missing_required
        item.missing_optional_dependencies = missing_optional
        item.update_available = update_available
        item.can_update = bool(update_available and not missing_required)
        item.can_install = bool(not installed and not missing_required)
        item.warnings = warnings
        return item

    def _enrich_custom_item(self, item: PluginMarketplaceItem) -> PluginMarketplaceItem:
        available_ids = self._available_plugin_ids()
        missing_required = sorted(
            dep for dep in item.required_dependencies if dep not in available_ids
        )
        missing_optional = sorted(
            dep for dep in item.optional_dependencies if dep not in available_ids
        )
        warnings = [f"Optional dependency {dep!r} is not installed" for dep in missing_optional]
        warnings.extend(
            f"Legacy dependency {dep!r} is not installed"
            for dep in item.legacy_dependencies
            if dep not in available_ids
        )

        records = self._manifest_records()
        record = records.get(item.plugin_id)
        installed = False
        installed_version = ""
        installer = self.get_installer(item.source.installer_type)
        if record is not None and record.source_type == "marketplace":
            installed = True
            installed_version = record.installed_version
        elif installer is not None:
            target_dir = self._custom_installer_target_dir(installer)
            if target_dir is not None:
                target = self._custom_plugin_target(target_dir, item.plugin_id)
                installed = target.is_dir()
                if installed:
                    try:
                        metadata = self._validate_custom_plugin_root(item.source, target)
                        installed_version = _custom_metadata_string(
                            metadata,
                            "version",
                            "",
                        )
                    except PluginMarketplaceError:
                        installed_version = ""

        update_available = bool(installed and installed_version and installed_version != item.version)
        item.installed = installed
        item.installed_version = installed_version
        item.installed_source = record.as_dict() if record is not None else None
        item.managed_by_webui = bool(installed)
        item.missing_required_dependencies = missing_required
        item.missing_optional_dependencies = missing_optional
        item.update_available = update_available
        item.can_update = bool(update_available and not missing_required)
        item.can_install = bool(not installed and not missing_required)
        item.warnings = warnings
        return item

    def _manifest_records(self) -> dict[str, Any]:
        try:
            return self.manifest.load()
        except PluginInstallError:
            return {}

    def _available_plugin_ids(self) -> set[str]:
        ids = {meta.id for meta in self.bot.plugin_manager.all_plugins}
        if self.plugins_dir.is_dir():
            for child in self.plugins_dir.iterdir():
                metadata_path = child / "metadata.json"
                if child.is_dir() and metadata_path.is_file():
                    try:
                        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
                    except Exception:
                        continue
                    plugin_id = payload.get("id")
                    if isinstance(plugin_id, str) and plugin_id:
                        ids.add(plugin_id)
        return ids

    def _metadata_version(self, *, target_exists_path: Path) -> str:
        metadata_path = target_exists_path / "metadata.json"
        if not metadata_path.is_file():
            return ""
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception:
            return ""
        version = payload.get("version")
        return version if isinstance(version, str) else ""


def build_plugin_marketplace_service(bot: Any, boot: Any) -> PluginMarketplaceService:
    """Build or reuse the process-local plugin marketplace service."""
    service = getattr(boot, "plugin_marketplace_service", None)
    if isinstance(service, PluginMarketplaceService) and service.bot is bot:
        return service
    service = PluginMarketplaceService(bot=bot, boot=boot)
    boot.plugin_marketplace_service = service
    return service


def _github_index_path(value: str) -> str:
    raw = value.strip().strip("/") or _GITHUB_INDEX_DEFAULT_PATH
    if raw == ".":
        raw = _GITHUB_INDEX_DEFAULT_PATH
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts or "." in path.parts or path.name != path.parts[-1]:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="GitHub marketplace index path must stay inside the repository",
        )
    if path.name != _GITHUB_INDEX_DEFAULT_PATH:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="GitHub marketplace index path must point to plugins.json",
        )
    return path.as_posix()


def _zip_single_file(name: str, content: bytes) -> bytes:
    output = BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, content)
    return output.getvalue()


def _normalize_index_repository_url(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message="Indexed marketplace plugin must include repo",
        )
    parsed = urlparse(raw)
    normalized = parsed._replace(query="", fragment="").geturl().rstrip("/")
    if normalized.endswith(".git"):
        normalized = normalized[:-4]
    owner, repo = _parse_github_repo(normalized)
    return f"https://github.com/{owner}/{repo}"


def _normalize_index_plugin_path(value: str) -> str:
    raw = value.strip().strip("/")
    if not raw:
        return ""
    if "\\" in raw:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message="Indexed marketplace plugin_path must use forward slashes",
        )
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message="Indexed marketplace plugin_path must be a relative repository directory",
        )
    return path.as_posix()


def _item_source_url(item: PluginMarketplaceItem) -> str:
    if item.source.source_type == "github_index":
        return item.repository
    return item.source.repository_url


def _item_ref(item: PluginMarketplaceItem) -> str:
    return item.ref or item.source.ref


def _safe_workspace_child(root: Path, plugin_path: str) -> Path:
    relative = PurePosixPath(plugin_path)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Invalid custom marketplace plugin path: {plugin_path!r}",
        )
    candidate = (root / Path(*relative.parts)).resolve()
    resolved_root = root.resolve()
    if candidate == resolved_root or not candidate.is_relative_to(resolved_root):
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Invalid custom marketplace plugin path: {plugin_path!r}",
        )
    return candidate


def _custom_plugin_id(metadata: dict[str, Any], *, fallback: str) -> str:
    raw = metadata.get("id") or metadata.get("plugin_id") or metadata.get("name") or fallback
    plugin_id = str(raw).strip() or fallback.strip()
    path = PurePosixPath(plugin_id)
    if (
        not plugin_id
        or "/" in plugin_id
        or "\\" in plugin_id
        or plugin_id in {".", ".."}
        or ".." in path.parts
    ):
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_PLUGIN_ID_INVALID",
            message=f"Invalid custom marketplace plugin id: {plugin_id!r}",
        )
    return plugin_id


def _custom_metadata_string(metadata: dict[str, Any], key: str, default: str) -> str:
    value = metadata.get(key, default)
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip() or default
    return str(value).strip() or default


def _custom_metadata_string_list(metadata: dict[str, Any], key: str) -> list[str]:
    value = metadata.get(key, [])
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _custom_metadata_display_name(metadata: dict[str, Any], plugin_id: str) -> str:
    return (
        _custom_metadata_string(metadata, "display_name", "")
        or _custom_metadata_string(metadata, "name", "")
        or plugin_id
    )


def _custom_metadata_description(metadata: dict[str, Any]) -> str:
    return _custom_metadata_string(metadata, "description", "") or _custom_metadata_string(
        metadata,
        "desc",
        "",
    )


def _custom_metadata_role(metadata: dict[str, Any]) -> str:
    role = _custom_metadata_string(metadata, "role", "logic").lower()
    return role if role in _VALID_ROLE_VALUES else "logic"


def _plugin_path_from_metadata_name(metadata_name: str, plugin_root: str) -> str:
    parts = PurePosixPath(metadata_name).parts
    if len(parts) < 2 or parts[-1] not in {"metadata.json", "metadata.yaml", "metadata.yml"}:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Invalid metadata path in marketplace archive: {metadata_name!r}",
        )
    parent_parts = parts[:-1]
    root_parts = PurePosixPath(plugin_root).parts
    for index in range(0, len(parent_parts)):
        if tuple(parent_parts[index : index + len(root_parts)]) == root_parts:
            plugin_parts = parent_parts[index:]
            if len(plugin_parts) == len(root_parts) + 1:
                return PurePosixPath(*plugin_parts).as_posix()
    raise PluginMarketplaceError(
        status_code=422,
        code="PLUGIN_MARKETPLACE_INDEX_INVALID",
        message=f"Marketplace metadata {metadata_name!r} is outside {plugin_root!r}",
    )


def _validate_marketplace_metadata(metadata: dict[str, Any], metadata_name: str) -> dict[str, Any]:
    plugin_id = _metadata_string(metadata, "id", "")
    if not plugin_id:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Marketplace metadata {metadata_name!r} must include id",
        )
    if not any(plugin_id.startswith(prefix) for prefix in _VALID_PLUGIN_PREFIXES):
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Marketplace plugin id {plugin_id!r} has an invalid prefix",
        )
    entry = _metadata_string(metadata, "entry", "")
    if not entry:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Marketplace metadata {metadata_name!r} must include entry",
        )
    entry_path = PurePosixPath(entry)
    if entry_path.is_absolute() or ".." in entry_path.parts:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Marketplace metadata {metadata_name!r} contains an invalid entry",
        )
    role = _metadata_string(metadata, "role", "logic").lower()
    if role not in _VALID_ROLE_VALUES:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"Marketplace metadata {metadata_name!r} has an invalid role",
        )
    return {
        "id": plugin_id,
        "entry": entry_path.as_posix(),
        "name": _metadata_string(metadata, "name", plugin_id),
        "version": _metadata_string(metadata, "version", "0.0.0"),
        "description": _metadata_string(metadata, "description", ""),
        "author": _metadata_string(metadata, "author", ""),
        "role": role,
        "permissions": _metadata_string_list(metadata, "permissions"),
        "dependencies": _metadata_string_list(metadata, "dependencies"),
        "required_dependencies": _metadata_string_list(metadata, "required_dependencies"),
        "optional_dependencies": _metadata_string_list(metadata, "optional_dependencies"),
        "tags": _metadata_string_list(metadata, "tags"),
        "homepage": _metadata_string(metadata, "homepage", ""),
        "repository": _metadata_string(metadata, "repository", ""),
    }


def _metadata_string(metadata: dict[str, Any], key: str, default: str) -> str:
    value = metadata.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"metadata.{key} must be a string",
        )
    return value.strip() or default


def _metadata_string_list(metadata: dict[str, Any], key: str) -> list[str]:
    value = metadata.get(key, [])
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_INDEX_INVALID",
            message=f"metadata.{key} must be a list of strings",
        )
    return [item.strip() for item in value if item.strip()]


def _parse_github_repo(url: str) -> tuple[str, str]:
    parsed = urlparse(url.strip())
    if parsed.scheme != "https" or parsed.netloc.lower() != "github.com":
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="Only github.com marketplace repositories are supported",
        )
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 2 or not parts[0] or not parts[1]:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="GitHub repository URL must include owner and repository",
        )
    repo = parts[1][:-4] if parts[1].endswith(".git") else parts[1]
    return parts[0], repo


def _validate_github_ref(ref: str) -> None:
    if not ref or ".." in ref or ref.endswith(".lock") or len(ref) > 200:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="Invalid GitHub marketplace ref",
        )


def _github_codeload_zip_url(owner: str, repo: str, ref: str) -> str:
    return f"https://codeload.github.com/{quote(owner)}/{quote(repo)}/zip/{quote(ref, safe='')}"


def _normalize_sparse_path(value: str) -> str:
    raw = value.strip()
    if not raw or "\\" in raw:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="Sparse checkout path must be a relative repository path",
        )
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise PluginMarketplaceError(
            status_code=422,
            code="PLUGIN_MARKETPLACE_SOURCE_INVALID",
            message="Sparse checkout path must stay inside the repository",
        )
    return path.as_posix()
