"""File-backed prompt-definition management helpers."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any

from shinbot.agent.services.prompt_engine import PromptComponent, PromptComponentKind, PromptStage
from shinbot.agent.services.prompt_engine.files import PromptFileError, parse_prompt_markdown
from shinbot.persistence.records import utc_now_iso

PROMPT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


@dataclass(slots=True)
class PromptDefinitionAdminError(RuntimeError):
    """Structured admin-layer error for API adapters."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class PromptDefinitionDraft:
    uuid: str
    prompt_id: str
    name: str
    stage: str
    type: str
    source_type: str = "unknown_source"
    source_id: str = ""
    owner_plugin_id: str = ""
    owner_module: str = ""
    module_path: str = ""
    priority: int = 100
    version: str = "1.0.0"
    description: str = ""
    enabled: bool = True
    content: str = ""
    template_vars: list[str] | None = None
    resolver_ref: str = ""
    bundle_refs: list[str] | None = None
    config: dict[str, Any] | None = None
    tags: list[str] | None = None
    metadata: dict[str, Any] | None = None
    created_at: str = ""
    updated_at: str = ""

    def to_payload(self, *, path: Path | None = None) -> dict[str, Any]:
        """Convert the draft to a serialisable payload dict.

        Args:
            path: Optional file path to include in the payload.

        Returns:
            A dict representation of the draft.
        """
        payload = asdict(self)
        payload["template_vars"] = list(self.template_vars or [])
        payload["bundle_refs"] = list(self.bundle_refs or [])
        payload["config"] = dict(self.config or {})
        payload["tags"] = list(self.tags or [])
        payload["metadata"] = dict(self.metadata or {})
        if path is not None:
            payload["path"] = str(path)
        return payload


class PromptDefinitionFileRepository:
    """Repository for user-editable prompt definitions under ``data/prompts/custom``."""

    def __init__(self, root: Path | str) -> None:
        """Initialize the repository with a root directory for prompt files.

        Args:
            root: Path to the directory containing prompt Markdown files.
        """
        self.root = Path(root)

    @classmethod
    def from_data_dir(cls, data_dir: Path | str) -> PromptDefinitionFileRepository:
        """Create a repository rooted at ``<data_dir>/prompts/custom``.

        Args:
            data_dir: Application data directory.

        Returns:
            A new PromptDefinitionFileRepository instance.
        """
        return cls(Path(data_dir) / "prompts" / "custom")

    def list(self) -> list[dict[str, Any]]:
        """Return all prompt definition records as serialised dictionaries.

        Returns:
            A list of prompt definition payload dicts sorted by stage, then
            priority, then prompt ID.
        """
        if not self.root.is_dir():
            return []
        payloads = [self._load_file(path) for path in sorted(self.root.glob("*.md"))]
        return sorted(payloads, key=lambda item: (str(item["stage"]), int(item["priority"]), str(item["prompt_id"])))

    def get(self, prompt_uuid: str) -> dict[str, Any] | None:
        """Look up a prompt definition by UUID (delegates to prompt ID lookup).

        Args:
            prompt_uuid: The prompt UUID to search for.

        Returns:
            Serialised prompt dict, or ``None`` if not found.
        """
        return self.get_by_prompt_id(prompt_uuid)

    def get_by_prompt_id(self, prompt_id: str) -> dict[str, Any] | None:
        """Look up a prompt definition by its normalised prompt ID.

        Args:
            prompt_id: The prompt identifier.

        Returns:
            Serialised prompt dict, or ``None`` if not found.
        """
        normalized = normalize_prompt_id(prompt_id)
        path = self._path_for_prompt_id(normalized)
        if not path.is_file():
            return None
        return self._load_file(path)

    def create(self, draft: PromptDefinitionDraft) -> dict[str, Any]:
        """Create a new prompt definition file on disk.

        Args:
            draft: The prompt definition draft to persist.

        Returns:
            Serialised payload of the newly created prompt.

        Raises:
            PromptDefinitionAdminError: If the prompt ID already exists.
        """
        prompt_id = normalize_prompt_id(draft.prompt_id)
        if self.get_by_prompt_id(prompt_id) is not None:
            raise PromptDefinitionAdminError(
                status_code=409,
                code="PROMPT_ALREADY_EXISTS",
                message=f"Prompt {prompt_id!r} already exists",
            )
        now = utc_now_iso()
        record = replace(
            draft,
            uuid=prompt_id,
            prompt_id=prompt_id,
            created_at=now,
            updated_at=now,
        )
        path = self._path_for_prompt_id(prompt_id)
        self._write_file(path, record)
        payload = self.get_by_prompt_id(prompt_id)
        assert payload is not None
        return payload

    def update(self, prompt_uuid: str, draft: PromptDefinitionDraft) -> dict[str, Any]:
        """Update an existing prompt definition file on disk.

        Args:
            prompt_uuid: The UUID of the prompt to update.
            draft: The new prompt definition draft.

        Returns:
            Serialised payload of the updated prompt.

        Raises:
            PromptDefinitionAdminError: If the prompt does not exist or
                the new ID conflicts.
        """
        current_id = normalize_prompt_id(prompt_uuid)
        current = self.get_by_prompt_id(current_id)
        if current is None:
            raise PromptDefinitionAdminError(
                status_code=404,
                code="PROMPT_NOT_FOUND",
                message=f"Prompt {prompt_uuid!r} was not found",
            )

        next_id = normalize_prompt_id(draft.prompt_id)
        if next_id != current_id and self.get_by_prompt_id(next_id) is not None:
            raise PromptDefinitionAdminError(
                status_code=409,
                code="PROMPT_ALREADY_EXISTS",
                message=f"Prompt {next_id!r} already exists",
            )
        record = replace(
            draft,
            uuid=next_id,
            prompt_id=next_id,
            created_at=str(current["created_at"]),
            updated_at=utc_now_iso(),
        )
        old_path = self._path_for_prompt_id(current_id)
        new_path = self._path_for_prompt_id(next_id)
        self._write_file(new_path, record)
        if new_path != old_path and old_path.exists():
            old_path.unlink()
        payload = self.get_by_prompt_id(next_id)
        assert payload is not None
        return payload

    def delete(self, prompt_uuid: str) -> None:
        """Delete a prompt definition file by UUID.

        Args:
            prompt_uuid: The prompt UUID to delete.

        Raises:
            PromptDefinitionAdminError: If the prompt does not exist.
        """
        prompt_id = normalize_prompt_id(prompt_uuid)
        path = self._path_for_prompt_id(prompt_id)
        if not path.is_file():
            raise PromptDefinitionAdminError(
                status_code=404,
                code="PROMPT_NOT_FOUND",
                message=f"Prompt {prompt_uuid!r} was not found",
            )
        path.unlink()

    def _path_for_prompt_id(self, prompt_id: str) -> Path:
        return self.root / f"{prompt_id}.md"

    def _load_file(self, path: Path) -> dict[str, Any]:
        try:
            front_matter, body = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
        except PromptFileError as exc:
            raise PromptDefinitionAdminError(
                status_code=500,
                code="INVALID_PROMPT_FILE",
                message=str(exc),
            ) from exc

        try:
            prompt_id = normalize_prompt_id(str(front_matter["id"]))
            if path.name != f"{prompt_id}.md":
                raise PromptDefinitionAdminError(
                    status_code=500,
                    code="INVALID_PROMPT_FILE",
                    message=f"Prompt file {path} id must match file name",
                )
            source = _mapping(front_matter.get("source"))
            metadata = normalize_prompt_metadata(_mapping(front_matter.get("metadata")))
            description = str(front_matter.get("description") or metadata.get("description") or "")
            name = str(
                front_matter.get("name")
                or metadata.get("display_name")
                or metadata.get("displayName")
                or prompt_id
            ).strip()
            draft = normalize_prompt_definition_input(
                prompt_id=prompt_id,
                name=name,
                source_type=str(
                    source.get("source_type")
                    or source.get("type")
                    or front_matter.get("source_type")
                    or "unknown_source"
                ),
                source_id=str(source.get("source_id") or front_matter.get("source_id") or ""),
                owner_plugin_id=str(
                    source.get("owner_plugin_id") or front_matter.get("owner_plugin_id") or ""
                ),
                owner_module=str(
                    source.get("owner_module") or front_matter.get("owner_module") or ""
                ),
                module_path=str(source.get("module_path") or front_matter.get("module_path") or ""),
                stage=str(front_matter["stage"]),
                type=str(front_matter.get("kind") or front_matter.get("type") or ""),
                priority=int(front_matter.get("priority", 100)),
                version=str(front_matter.get("version") or "1.0.0"),
                description=description,
                enabled=bool(front_matter.get("enabled", True)),
                content=body.strip(),
                template_vars=[str(item) for item in _list(front_matter.get("template_vars"))],
                resolver_ref=str(front_matter.get("resolver_ref") or ""),
                bundle_refs=[str(item) for item in _list(front_matter.get("bundle_refs"))],
                config=_mapping(front_matter.get("config")),
                tags=[str(item) for item in _list(front_matter.get("tags"))],
                metadata=metadata,
            )
        except PromptDefinitionAdminError:
            raise
        except KeyError as exc:
            raise PromptDefinitionAdminError(
                status_code=500,
                code="INVALID_PROMPT_FILE",
                message=f"Prompt file {path} missing required field {exc.args[0]!r}",
            ) from exc
        except Exception as exc:
            raise PromptDefinitionAdminError(
                status_code=500,
                code="INVALID_PROMPT_FILE",
                message=f"Prompt file {path} is invalid: {exc}",
            ) from exc

        created_at = str(front_matter.get("created_at") or "")
        updated_at = str(front_matter.get("updated_at") or path.stat().st_mtime)
        return replace(
            draft,
            uuid=prompt_id,
            created_at=created_at,
            updated_at=updated_at,
        ).to_payload(path=path)

    def _write_file(self, path: Path, draft: PromptDefinitionDraft) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_prompt_definition_markdown(draft), encoding="utf-8")


def serialize_prompt_definition(payload: dict[str, Any]) -> dict[str, Any]:
    """Serialise a prompt definition payload to the camelCase API shape.

    Args:
        payload: Internal prompt definition dict with snake_case keys.

    Returns:
        A dict with camelCase keys for the front-end.
    """
    return {
        "uuid": payload["uuid"],
        "promptId": payload["prompt_id"],
        "name": payload["name"],
        "source": {
            "sourceType": payload["source_type"],
            "sourceId": payload["source_id"],
            "ownerPluginId": payload["owner_plugin_id"],
            "ownerModule": payload["owner_module"],
            "modulePath": payload["module_path"],
        },
        "stage": payload["stage"],
        "type": payload["type"],
        "priority": payload["priority"],
        "version": payload["version"],
        "description": payload["description"],
        "enabled": payload["enabled"],
        "content": payload["content"],
        "templateVars": payload["template_vars"],
        "resolverRef": payload["resolver_ref"],
        "bundleRefs": payload["bundle_refs"],
        "config": payload["config"],
        "tags": payload["tags"],
        "metadata": payload["metadata"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
    }


def normalize_prompt_id(value: str) -> str:
    """Validate and normalise a prompt identifier.

    Args:
        value: Raw prompt ID string.

    Returns:
        The stripped, validated prompt ID.

    Raises:
        PromptDefinitionAdminError: If the ID is empty or contains
            invalid characters.
    """
    normalized = value.strip()
    if not normalized or not PROMPT_ID_RE.fullmatch(normalized):
        raise PromptDefinitionAdminError(
            status_code=422,
            code="INVALID_ACTION",
            message="Prompt id must be a safe file name stem",
        )
    return normalized


def normalize_prompt_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Remove reserved top-level keys from a metadata dict.

    Args:
        metadata: The raw metadata dict from a prompt front-matter.

    Returns:
        A new dict containing only non-reserved metadata keys.
    """
    reserved_keys = {
        "prompt_id",
        "promptId",
        "name",
        "source",
        "source_type",
        "sourceType",
        "source_id",
        "sourceId",
        "owner_plugin_id",
        "ownerPluginId",
        "owner_module",
        "ownerModule",
        "module_path",
        "modulePath",
        "stage",
        "kind",
        "type",
        "priority",
        "version",
        "description",
        "enabled",
        "content",
        "template_vars",
        "templateVars",
        "resolver_ref",
        "resolverRef",
        "bundle_refs",
        "bundleRefs",
        "config",
        "tags",
        "created_at",
        "createdAt",
        "updated_at",
        "updatedAt",
        "lastModified",
        "display_name",
        "displayName",
    }
    return {str(key): value for key, value in metadata.items() if str(key) not in reserved_keys}


def normalize_prompt_definition_input(
    *,
    prompt_id: str,
    name: str,
    source_type: str,
    source_id: str,
    owner_plugin_id: str,
    owner_module: str,
    module_path: str,
    stage: str,
    type: str,
    priority: int,
    version: str,
    description: str,
    enabled: bool,
    content: str,
    template_vars: list[str],
    resolver_ref: str,
    bundle_refs: list[str],
    config: dict[str, Any],
    tags: list[str],
    metadata: dict[str, Any],
) -> PromptDefinitionDraft:
    """Normalise and validate raw prompt-definition input fields.

    Args:
        prompt_id: The prompt identifier.
        name: Human-readable prompt name.
        source_type: Source type string.
        source_id: Source identifier.
        owner_plugin_id: Plugin that owns this prompt.
        owner_module: Module that owns this prompt.
        module_path: Module file path.
        stage: Prompt stage string.
        type: Prompt component kind string.
        priority: Numeric priority.
        version: Semantic version string.
        description: Human-readable description.
        enabled: Whether the prompt is enabled.
        content: The prompt body text.
        template_vars: List of template variable names.
        resolver_ref: Optional resolver reference.
        bundle_refs: List of bundle reference IDs.
        config: Additional config dict.
        tags: List of tag strings.
        metadata: Additional metadata dict.

    Returns:
        A normalised ``PromptDefinitionDraft`` dataclass.

    Raises:
        PromptDefinitionAdminError: On validation failures.
    """
    normalized_prompt_id = normalize_prompt_id(prompt_id)
    normalized_name = name.strip()
    normalized_source_type = source_type.strip() or "unknown_source"
    normalized_source_id = source_id.strip()
    normalized_owner_plugin_id = owner_plugin_id.strip()
    normalized_owner_module = owner_module.strip()
    normalized_module_path = module_path.strip()
    normalized_version = version.strip() or "1.0.0"
    normalized_description = description.strip()
    normalized_template_vars = [item.strip() for item in template_vars if item.strip()]
    normalized_bundle_refs = [item.strip() for item in bundle_refs if item.strip()]
    normalized_tags = [item.strip() for item in tags if item.strip()]

    if not normalized_name:
        raise PromptDefinitionAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Prompt name must not be empty",
        )
    if normalized_source_type == "persona":
        raise PromptDefinitionAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message="Persona prompts are stored in data/personas/*.md",
        )

    deduped_tags: list[str] = []
    seen_tags: set[str] = set()
    for tag in normalized_tags:
        if tag in seen_tags:
            continue
        seen_tags.add(tag)
        deduped_tags.append(tag)

    normalized_metadata = normalize_prompt_metadata(dict(metadata))

    try:
        component = PromptComponent(
            id=normalized_prompt_id,
            stage=PromptStage(stage),
            kind=PromptComponentKind(type),
            version=normalized_version,
            priority=priority,
            enabled=enabled,
            content=content,
            template_vars=normalized_template_vars,
            resolver_ref=resolver_ref.strip(),
            bundle_refs=normalized_bundle_refs,
            tags=deduped_tags,
            metadata=normalized_metadata,
        )
    except ValueError as exc:
        raise PromptDefinitionAdminError(
            status_code=400,
            code="INVALID_ACTION",
            message=str(exc),
        ) from exc

    return PromptDefinitionDraft(
        uuid=component.id,
        prompt_id=component.id,
        name=normalized_name,
        source_type=normalized_source_type,
        source_id=normalized_source_id,
        owner_plugin_id=normalized_owner_plugin_id,
        owner_module=normalized_owner_module,
        module_path=normalized_module_path,
        stage=component.stage.value,
        type=component.kind.value,
        priority=component.priority,
        version=component.version,
        description=normalized_description,
        enabled=component.enabled,
        content=component.content,
        template_vars=list(component.template_vars),
        resolver_ref=component.resolver_ref,
        bundle_refs=list(component.bundle_refs),
        config=dict(config),
        tags=list(component.tags),
        metadata=dict(component.metadata),
    )


def get_prompt_definition_or_raise(
    repository: PromptDefinitionFileRepository,
    prompt_uuid: str,
) -> dict[str, Any]:
    """Retrieve a prompt definition by UUID, or raise a 404 error.

    Args:
        repository: The prompt definition file repository.
        prompt_uuid: The prompt UUID to look up.

    Returns:
        Serialised prompt definition dict.

    Raises:
        PromptDefinitionAdminError: If the prompt is not found.
    """
    payload = repository.get(prompt_uuid)
    if payload is None:
        raise PromptDefinitionAdminError(
            status_code=404,
            code="PROMPT_NOT_FOUND",
            message=f"Prompt {prompt_uuid!r} was not found",
        )
    return payload


def assert_prompt_id_available(
    repository: PromptDefinitionFileRepository,
    prompt_id: str,
    *,
    current_uuid: str | None,
) -> None:
    """Assert that no other prompt definition uses the given prompt ID.

    Args:
        repository: The prompt definition file repository.
        prompt_id: The prompt ID to check.
        current_uuid: UUID of the prompt being updated, or ``None`` for creation.

    Raises:
        PromptDefinitionAdminError: If the ID is already taken by another prompt.
    """
    existing = repository.get_by_prompt_id(prompt_id)
    if existing is not None and existing["uuid"] != current_uuid:
        raise PromptDefinitionAdminError(
            status_code=409,
            code="PROMPT_ALREADY_EXISTS",
            message=f"Prompt {prompt_id!r} already exists",
        )


def assert_no_runtime_prompt_conflict(prompt_id: str, data_dir: Path | str) -> None:
    """Assert that a custom prompt ID does not shadow a runtime prompt file.

    Args:
        prompt_id: The custom prompt ID to validate.
        data_dir: ShinBot data directory used for runtime prompt discovery.

    Raises:
        PromptDefinitionAdminError: If the ID is owned by a runtime prompt file.
    """
    from shinbot.agent.services.prompt_engine.discovery import discover_file_backed_prompts
    from shinbot.agent.services.prompt_engine.files import PromptFileLoadConfig

    config = PromptFileLoadConfig.from_data_dir(data_dir, sync_to_data=False)
    registry = discover_file_backed_prompts(data_dir, prompt_file_config=config)
    if any(manifest.prompt_id == prompt_id for manifest in registry.prompt_file_catalog.list()):
        raise PromptDefinitionAdminError(
            status_code=409,
            code="PROMPT_FILE_CONFLICT",
            message=f"Prompt {prompt_id!r} conflicts with a runtime prompt file",
        )


def render_prompt_definition_markdown(draft: PromptDefinitionDraft) -> str:
    """Render a prompt definition draft as Markdown with YAML front-matter.

    Args:
        draft: The prompt definition draft to render.

    Returns:
        A complete Markdown string with YAML front-matter.
    """
    import yaml

    front_matter: dict[str, Any] = {
        "id": draft.prompt_id,
        "name": draft.name,
        "stage": draft.stage,
        "kind": draft.type,
        "priority": draft.priority,
        "version": draft.version,
        "enabled": draft.enabled,
    }
    if draft.description:
        front_matter["description"] = draft.description
    source = {
        "source_type": draft.source_type,
        "source_id": draft.source_id,
        "owner_plugin_id": draft.owner_plugin_id,
        "owner_module": draft.owner_module,
        "module_path": draft.module_path,
    }
    front_matter["source"] = {key: value for key, value in source.items() if value}
    if draft.template_vars:
        front_matter["template_vars"] = list(draft.template_vars)
    if draft.resolver_ref:
        front_matter["resolver_ref"] = draft.resolver_ref
    if draft.bundle_refs:
        front_matter["bundle_refs"] = list(draft.bundle_refs)
    if draft.config:
        front_matter["config"] = dict(draft.config)
    if draft.tags:
        front_matter["tags"] = list(draft.tags)
    if draft.metadata:
        front_matter["metadata"] = dict(draft.metadata)
    if draft.created_at:
        front_matter["created_at"] = draft.created_at
    if draft.updated_at:
        front_matter["updated_at"] = draft.updated_at

    yaml_text = yaml.safe_dump(
        front_matter,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    ).strip()
    return f"---\n{yaml_text}\n---\n\n{draft.content.strip()}\n"


def _list(value: object) -> list[object]:
    return list(value) if isinstance(value, list | tuple) else []


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


__all__ = [
    "PromptDefinitionAdminError",
    "PromptDefinitionDraft",
    "PromptDefinitionFileRepository",
    "assert_prompt_id_available",
    "get_prompt_definition_or_raise",
    "normalize_prompt_definition_input",
    "normalize_prompt_metadata",
    "render_prompt_definition_markdown",
    "serialize_prompt_definition",
]
