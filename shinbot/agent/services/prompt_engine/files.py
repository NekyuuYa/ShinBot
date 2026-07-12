"""File-backed prompt component loading utilities."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from importlib import import_module
from pathlib import Path
from typing import Any

from shinbot.agent.services.prompt_engine.prompt_assets import (
    PromptAssetSynchronizer,
    PromptAssetSyncResult,
    PromptSyncStatus,
)
from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)

DEFAULT_PROMPT_LOCALE = "zh-CN"
DEFAULT_PROMPT_FALLBACK_LOCALE = "en-US"
DEFAULT_PROMPT_DATA_ROOT = Path("data/prompts")


class PromptFileError(RuntimeError):
    """Raised when a prompt markdown file cannot be loaded."""


@dataclass(slots=True, frozen=True)
class PromptFileManifest:
    """Manifest entry for one file-backed prompt component."""

    prompt_id: str
    locale: str
    source_path: Path
    runtime_path: Path
    loaded_path: Path
    source_exists: bool
    runtime_exists: bool
    loaded_from: str
    sync_status: PromptSyncStatus = PromptSyncStatus.SOURCE_ONLY
    source_version: str = ""
    source_sha256: str = ""
    runtime_version: str = ""
    runtime_sha256: str = ""
    base_version: str = ""
    base_sha256: str = ""
    pending_path: Path | None = None

    def refresh(self) -> PromptFileManifest:
        """Return a copy with file existence fields refreshed from disk."""

        source_exists = self.source_path.exists()
        runtime_exists = self.runtime_path.exists()
        if runtime_exists:
            loaded_from = "runtime"
            loaded_path = self.runtime_path
        elif source_exists:
            loaded_from = "source"
            loaded_path = self.source_path
        else:
            loaded_from = self.loaded_from
            loaded_path = self.loaded_path
        return replace(
            self,
            source_exists=source_exists,
            runtime_exists=runtime_exists,
            loaded_from=loaded_from,
            loaded_path=loaded_path,
        )

    def to_payload(self) -> dict[str, Any]:
        """Return a JSON-serialisable manifest payload."""

        manifest = self.refresh()
        payload = asdict(manifest)
        payload["source_path"] = str(manifest.source_path)
        payload["runtime_path"] = str(manifest.runtime_path)
        payload["loaded_path"] = str(manifest.loaded_path)
        payload["sync_status"] = manifest.sync_status.value
        payload["pending_path"] = (
            str(manifest.pending_path) if manifest.pending_path is not None else ""
        )
        return payload


class PromptFileCatalogService:
    """In-memory catalog of registered file-backed prompt files."""

    def __init__(self) -> None:
        """Initialize an empty prompt-file catalog."""

        self._entries: dict[tuple[str, str], PromptFileManifest] = {}

    def record(self, manifest: PromptFileManifest) -> None:
        """Record or replace one manifest entry."""

        self._entries[(manifest.locale, manifest.prompt_id)] = manifest

    def list(self) -> list[PromptFileManifest]:
        """Return all manifest entries in a stable order."""

        return sorted(
            (item.refresh() for item in self._entries.values()),
            key=lambda item: (item.locale, item.prompt_id),
        )

    def get(self, *, prompt_id: str, locale: str) -> PromptFileManifest | None:
        """Return one manifest entry by prompt ID and locale."""

        manifest = self._entries.get((locale, prompt_id))
        return manifest.refresh() if manifest is not None else None

    def prompt_ids(self) -> set[str]:
        """Return the set of registered file-backed prompt IDs."""

        return {manifest.prompt_id for manifest in self._entries.values()}


@dataclass(slots=True, frozen=True)
class PromptFileLoadConfig:
    """Runtime options for loading file-backed prompt components."""

    locale: str = DEFAULT_PROMPT_LOCALE
    fallback_locales: tuple[str, ...] = (DEFAULT_PROMPT_FALLBACK_LOCALE,)
    data_root: Path | str = DEFAULT_PROMPT_DATA_ROOT
    sync_to_data: bool = False

    @classmethod
    def from_data_dir(
        cls,
        data_dir: Path | str,
        *,
        locale: str = DEFAULT_PROMPT_LOCALE,
        fallback_locales: list[str] | tuple[str, ...] = (DEFAULT_PROMPT_FALLBACK_LOCALE,),
        sync_to_data: bool = True,
    ) -> PromptFileLoadConfig:
        """Build the default runtime prompt config for one ShinBot data dir."""

        return cls(
            locale=locale,
            fallback_locales=tuple(str(item) for item in fallback_locales if str(item).strip()),
            data_root=Path(data_dir) / "prompts",
            sync_to_data=sync_to_data,
        )


def register_prompt_files(
    registry: Any,
    *,
    package: str,
    prompt_ids: list[str] | tuple[str, ...],
    file_config: PromptFileLoadConfig | None = None,
    locale: str = DEFAULT_PROMPT_LOCALE,
    fallback_locales: list[str] | tuple[str, ...] = (DEFAULT_PROMPT_FALLBACK_LOCALE,),
    data_root: Path | str = DEFAULT_PROMPT_DATA_ROOT,
    sync_to_data: bool = False,
) -> list[PromptComponent]:
    """Load prompt markdown files and upsert them into ``registry``.

    Source files live under ``<package>/prompts/{locale}/{prompt_id}.md``.
    By default this reads source files directly.  Passing ``sync_to_data=True``
    performs a non-destructive copy to ``data/prompts/{locale}/{prompt_id}.md``
    and then loads the runtime copy, which is the future user-editable path.
    """

    if file_config is not None:
        locale = file_config.locale
        fallback_locales = file_config.fallback_locales
        data_root = file_config.data_root
        sync_to_data = file_config.sync_to_data

    module_dir = _module_dir(package)
    data_root_path = Path(data_root)
    synchronizer = PromptAssetSynchronizer(data_root_path) if sync_to_data else None
    registered: list[PromptComponent] = []
    for prompt_id in prompt_ids:
        source_path, selected_locale = _resolve_source_prompt(
            module_dir,
            prompt_id,
            locale=locale,
            fallback_locales=fallback_locales,
        )
        runtime_path = data_root_path / selected_locale / f"{prompt_id}.md"
        sync_result = (
            synchronizer.sync(
                prompt_id=prompt_id,
                locale=selected_locale,
                source_path=source_path,
                runtime_path=runtime_path,
            )
            if synchronizer is not None
            else None
        )
        load_path = sync_result.active_path if sync_result is not None else source_path
        loaded_from = "runtime" if load_path == runtime_path else "source"
        component = load_prompt_component(
            load_path,
            locale=selected_locale,
            source_path=source_path,
            runtime_path=runtime_path,
            expected_id=prompt_id,
        )
        if sync_result is not None:
            component.metadata.update(_sync_metadata(sync_result))
        registry.upsert_component(component)
        catalog = getattr(registry, "prompt_file_catalog", None)
        if isinstance(catalog, PromptFileCatalogService):
            catalog.record(
                PromptFileManifest(
                    prompt_id=prompt_id,
                    locale=selected_locale,
                    source_path=source_path,
                    runtime_path=runtime_path,
                    loaded_path=load_path,
                    source_exists=source_path.exists(),
                    runtime_exists=runtime_path.exists(),
                    loaded_from=loaded_from,
                    **_manifest_sync_kwargs(sync_result),
                )
            )
        registered.append(component)
    return registered


def load_prompt_component(
    path: Path | str,
    *,
    locale: str = "",
    source_path: Path | None = None,
    runtime_path: Path | None = None,
    expected_id: str | None = None,
) -> PromptComponent:
    """Parse one prompt markdown file into a ``PromptComponent``."""

    path = Path(path)
    front_matter, body = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
    metadata = dict(_mapping(front_matter.get("metadata")))
    metadata.setdefault("builtin", True)
    metadata["prompt_file"] = str(path)
    if locale:
        metadata["locale"] = locale
    if source_path is not None:
        metadata["source_prompt_file"] = str(source_path)
    if runtime_path is not None:
        metadata["runtime_prompt_file"] = str(runtime_path)

    try:
        component_id = str(front_matter["id"]).strip()
        _validate_prompt_filename(path, component_id, expected_id=expected_id)
        kind = PromptComponentKind(str(front_matter["kind"]))
        component = PromptComponent(
            id=component_id,
            stage=PromptStage(str(front_matter["stage"]).strip()),
            kind=kind,
            version=str(front_matter.get("version") or "1.0.0"),
            priority=int(front_matter.get("priority", 100)),
            enabled=bool(front_matter.get("enabled", True)),
            content=body.strip() if kind in {PromptComponentKind.STATIC_TEXT, PromptComponentKind.TEMPLATE} else "",
            template_vars=[str(item) for item in _list(front_matter.get("template_vars"))],
            resolver_ref=str(front_matter.get("resolver_ref") or ""),
            bundle_refs=[str(item) for item in _list(front_matter.get("bundle_refs"))],
            tags=[str(item) for item in _list(front_matter.get("tags"))],
            metadata=metadata,
        )
    except KeyError as exc:
        raise PromptFileError(f"Prompt file {path} missing required field {exc.args[0]!r}") from exc
    except Exception as exc:
        raise PromptFileError(f"Prompt file {path} is invalid: {exc}") from exc
    return component


def parse_prompt_markdown(text: str, *, path: Path | None = None) -> tuple[dict[str, Any], str]:
    """Parse a Markdown prompt file with YAML front matter."""

    if not text.startswith("---\n"):
        location = f" {path}" if path is not None else ""
        raise PromptFileError(f"Prompt file{location} must start with front matter")
    try:
        raw_front_matter, body = text[4:].split("\n---", 1)
    except ValueError as exc:
        location = f" {path}" if path is not None else ""
        raise PromptFileError(f"Prompt file{location} missing front matter terminator") from exc
    if body.startswith("\n"):
        body = body[1:]
    return _parse_front_matter(raw_front_matter, path=path), body


def _module_dir(package: str) -> Path:
    module = import_module(package)
    module_file = getattr(module, "__file__", None)
    if not module_file:
        raise PromptFileError(f"Package {package!r} has no filesystem path")
    return Path(module_file).resolve().parent


def _resolve_source_prompt(
    module_dir: Path,
    prompt_id: str,
    *,
    locale: str,
    fallback_locales: list[str] | tuple[str, ...],
) -> tuple[Path, str]:
    seen: set[str] = set()
    candidates = [locale, *fallback_locales]
    for candidate_locale in candidates:
        normalized_locale = str(candidate_locale or "").strip()
        if not normalized_locale or normalized_locale in seen:
            continue
        seen.add(normalized_locale)
        path = module_dir / "prompts" / normalized_locale / f"{prompt_id}.md"
        if path.exists():
            return path, normalized_locale
    raise PromptFileError(
        f"Prompt {prompt_id!r} was not found under {module_dir / 'prompts'} "
        f"for locale {locale!r} or fallbacks {list(fallback_locales)!r}"
    )


def _sync_metadata(result: PromptAssetSyncResult) -> dict[str, Any]:
    return {
        "prompt_sync_status": result.status.value,
        "source_prompt_version": result.source_revision.version,
        "source_prompt_sha256": result.source_revision.sha256,
        "runtime_prompt_version": (
            result.runtime_revision.version if result.runtime_revision is not None else ""
        ),
        "runtime_prompt_sha256": (
            result.runtime_revision.sha256 if result.runtime_revision is not None else ""
        ),
        "prompt_update_pending": result.pending_path is not None,
    }


def _manifest_sync_kwargs(result: PromptAssetSyncResult | None) -> dict[str, Any]:
    if result is None:
        return {}
    return {
        "sync_status": result.status,
        "source_version": result.source_revision.version,
        "source_sha256": result.source_revision.sha256,
        "runtime_version": (
            result.runtime_revision.version if result.runtime_revision is not None else ""
        ),
        "runtime_sha256": (
            result.runtime_revision.sha256 if result.runtime_revision is not None else ""
        ),
        "base_version": result.base_revision.version if result.base_revision is not None else "",
        "base_sha256": result.base_revision.sha256 if result.base_revision is not None else "",
        "pending_path": result.pending_path,
    }


def _parse_front_matter(raw: str, *, path: Path | None) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency is expected in the app env
        raise PromptFileError("PyYAML is required to parse prompt front matter") from exc

    try:
        parsed = yaml.safe_load(raw) or {}
    except Exception as exc:
        raise PromptFileError(f"Prompt file {path} has invalid YAML front matter: {exc}") from exc
    if not isinstance(parsed, dict):
        raise PromptFileError(f"Prompt file {path} front matter must be a mapping")
    return parsed


def _validate_prompt_filename(
    path: Path,
    component_id: str,
    *,
    expected_id: str | None,
) -> None:
    if expected_id is not None and component_id != expected_id:
        raise PromptFileError(
            f"Prompt file {path} declares id {component_id!r}, expected {expected_id!r}"
        )
    expected_name = f"{component_id}.md"
    if path.name != expected_name:
        raise PromptFileError(
            f"Prompt file {path} name must be {expected_name!r} for id {component_id!r}"
        )


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


__all__ = [
    "DEFAULT_PROMPT_DATA_ROOT",
    "DEFAULT_PROMPT_FALLBACK_LOCALE",
    "DEFAULT_PROMPT_LOCALE",
    "PromptFileError",
    "PromptFileCatalogService",
    "PromptFileLoadConfig",
    "PromptFileManifest",
    "load_prompt_component",
    "parse_prompt_markdown",
    "register_prompt_files",
]
