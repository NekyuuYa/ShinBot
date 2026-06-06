"""Administrative helpers for plugin marketplace discovery."""

from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from urllib.parse import quote, urlparse

import httpx

from shinbot.admin.plugin_install import (
    PLUGIN_INSTALL_MAX_ARCHIVE_BYTES,
    PluginInstallError,
    PluginInstallManifest,
    build_plugin_install_service,
)

OFFICIAL_MARKETPLACE_SOURCE_ID = "official"
OFFICIAL_MARKETPLACE_REPOSITORY_URL = "https://github.com/NekyuuYa/shinbot-plugins"
OFFICIAL_MARKETPLACE_REF = "main"
OFFICIAL_MARKETPLACE_PLUGIN_ROOT = "plugins"
_VALID_PLUGIN_PREFIXES = ("shinbot_plugin_", "shinbot_adapter_", "shinbot_debug_")
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
        self.manifest = PluginInstallManifest(self.data_dir)
        self.sources = {OFFICIAL_MARKETPLACE_SOURCE.id: OFFICIAL_MARKETPLACE_SOURCE}

    def list_sources(self) -> dict[str, Any]:
        """Return configured marketplace sources."""
        return {"sources": [source.as_dict() for source in self.sources.values()]}

    async def list_plugins(self, source_id: str = OFFICIAL_MARKETPLACE_SOURCE_ID) -> dict[str, Any]:
        """Return discovered marketplace plugins with local install state."""
        source = self._source_or_raise(source_id)
        archive_bytes = await self._download_github_archive(source)
        items = self._scan_monorepo_archive(source, archive_bytes)
        return {
            "source": source.as_dict(),
            "plugins": [self._enrich_item(item).as_dict() for item in items],
        }

    async def get_plugin(
        self,
        source_id: str,
        plugin_id: str,
    ) -> dict[str, Any]:
        """Return one discovered marketplace plugin."""
        payload = await self.list_plugins(source_id)
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
    ) -> dict[str, Any]:
        """Preview installing one marketplace plugin."""
        item = await self._raw_plugin_or_raise(source_id, plugin_id)
        service = build_plugin_install_service(self.bot, self.boot)
        try:
            return await service.preview_github(
                item.source.repository_url,
                item.source.ref,
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
    ) -> dict[str, Any]:
        """Install one marketplace plugin through the WebUI install service."""
        item = await self._raw_plugin_or_raise(source_id, plugin_id)
        service = build_plugin_install_service(self.bot, self.boot)
        try:
            return await service.install_github(
                item.source.repository_url,
                item.source.ref,
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
    ) -> PluginMarketplaceItem:
        source = self._source_or_raise(source_id)
        archive_bytes = await self._download_github_archive(source)
        for item in self._scan_monorepo_archive(source, archive_bytes):
            if item.plugin_id == plugin_id:
                return item
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

    async def _download_github_archive(self, source: PluginMarketplaceSource) -> bytes:
        owner, repo = _parse_github_repo(source.repository_url)
        _validate_github_ref(source.ref)
        archive_url = (
            f"https://api.github.com/repos/{quote(owner)}/{quote(repo)}/zipball/"
            f"{quote(source.ref, safe='')}"
        )
        try:
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=httpx.Timeout(30.0, connect=10.0),
                headers={"User-Agent": "ShinBot-WebUI-Plugin-Marketplace"},
            ) as client:
                response = await client.get(archive_url)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            raise PluginMarketplaceError(
                status_code=502,
                code="PLUGIN_MARKETPLACE_DOWNLOAD_FAILED",
                message=f"Failed to download plugin marketplace source: {exc}",
            ) from exc
        content = response.content
        if len(content) > PLUGIN_INSTALL_MAX_ARCHIVE_BYTES:
            raise PluginMarketplaceError(
                status_code=413,
                code="PLUGIN_MARKETPLACE_ARCHIVE_INVALID",
                message="Plugin marketplace archive is too large",
            )
        return content

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
