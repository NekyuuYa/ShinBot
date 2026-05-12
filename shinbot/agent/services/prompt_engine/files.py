"""File-backed prompt component loading utilities."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

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
    registered: list[PromptComponent] = []
    for prompt_id in prompt_ids:
        source_path, selected_locale = _resolve_source_prompt(
            module_dir,
            prompt_id,
            locale=locale,
            fallback_locales=fallback_locales,
        )
        runtime_path = data_root_path / selected_locale / f"{prompt_id}.md"
        if sync_to_data:
            _ensure_runtime_prompt(source_path, runtime_path)
        load_path = runtime_path if sync_to_data and runtime_path.exists() else source_path
        component = load_prompt_component(
            load_path,
            locale=selected_locale,
            source_path=source_path,
            runtime_path=runtime_path,
            expected_id=prompt_id,
        )
        registry.upsert_component(component)
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


def _ensure_runtime_prompt(source_path: Path, runtime_path: Path) -> None:
    if runtime_path.exists():
        return
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, runtime_path)


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
    "PromptFileLoadConfig",
    "load_prompt_component",
    "parse_prompt_markdown",
    "register_prompt_files",
]
