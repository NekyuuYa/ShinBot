"""Administrative helpers for plugin marketplace discovery."""

from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import time
import uuid
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import urlparse

from shinbot.admin.plugin_install import (
    PLUGIN_INSTALL_MAX_ARCHIVE_BYTES,
    PLUGIN_INSTALL_MAX_EXTRACTED_BYTES,
    PluginInstallError,
    PluginInstallManifest,
    build_plugin_install_service,
)

OFFICIAL_MARKETPLACE_SOURCE_ID = "official"
OFFICIAL_MARKETPLACE_REPOSITORY_URL = "https://github.com/NekyuuYa/shinbot-plugins"
OFFICIAL_MARKETPLACE_REF = "main"
OFFICIAL_MARKETPLACE_PLUGIN_ROOT = "plugins"
PLUGIN_MARKETPLACE_CACHE_TTL_SECONDS = 6 * 60 * 60
PLUGIN_MARKETPLACE_CACHE_SCHEMA_VERSION = 1
_VALID_PLUGIN_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_", "shinbot_converter_")
_VALID_ROLE_VALUES = {"logic", "adapter"}


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
    source_type: Literal["github_monorepo"]
    repository_url: str
    ref: str
    plugin_root: str

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
            "ref": self.source.ref,
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
    source_type="github_monorepo",
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
        self.sources = {OFFICIAL_MARKETPLACE_SOURCE.id: OFFICIAL_MARKETPLACE_SOURCE}

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
        archive = await self._load_sparse_archive(
            source,
            sparse_paths=[f"{source.plugin_root}/*/metadata.json"],
            cache_scope="metadata",
            refresh=refresh,
        )
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
        service = build_plugin_install_service(self.bot, self.boot)
        try:
            return await service.preview_github_archive(
                item.source.repository_url,
                item.source.ref,
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
        service = build_plugin_install_service(self.bot, self.boot)
        try:
            return await service.install_github_archive(
                item.source.repository_url,
                item.source.ref,
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
        metadata_archive = await self._load_sparse_archive(
            source,
            sparse_paths=[f"{source.plugin_root}/*/metadata.json"],
            cache_scope="metadata",
            refresh=refresh,
        )
        for item in self._scan_monorepo_archive(source, metadata_archive.content):
            if item.plugin_id == plugin_id:
                plugin_archive = await self._load_sparse_archive(
                    source,
                    sparse_paths=[f"{item.plugin_path}/**"],
                    cache_scope=f"plugin:{item.plugin_path}",
                    refresh=refresh,
                )
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
                    source.repository_url,
                    source.ref,
                    source.plugin_root,
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

    def _enrich_item(self, item: PluginMarketplaceItem) -> PluginMarketplaceItem:
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
            and record.source_url == item.source.repository_url
            and (record.ref or item.source.ref) == item.source.ref
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


def _plugin_path_from_metadata_name(metadata_name: str, plugin_root: str) -> str:
    parts = PurePosixPath(metadata_name).parts
    if len(parts) < 2 or parts[-1] != "metadata.json":
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
