"""File-backed persona management helpers."""

from __future__ import annotations

import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from shinbot.agent.services.prompt_engine.files import (
    PromptFileError,
    parse_prompt_markdown,
)
from shinbot.agent.services.prompt_engine.schema import (
    PromptComponent,
    PromptComponentKind,
    PromptStage,
)
from shinbot.persistence.records import utc_now_iso

PERSONA_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
DEFAULT_PERSONA_ID = "default"


@dataclass(slots=True)
class FewShotExample:
    """A single few-shot example for a persona."""

    user: str
    assistant: str


@dataclass(slots=True)
class PersonaFileError(RuntimeError):
    """Structured admin-layer error for file-backed persona flows."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True, frozen=True)
class PersonaFileRecord:
    """A persona loaded from ``data/personas/*.md``."""

    uuid: str
    name: str
    prompt_text: str
    tags: list[str]
    enabled: bool
    created_at: str
    updated_at: str
    path: Path
    version: str = "1.0.0"
    description: str = ""
    few_shot: list[FewShotExample] = field(default_factory=list)


class PersonaFileRepository:
    """Repository for user-editable persona Markdown files."""

    def __init__(self, root: Path | str) -> None:
        """Initialize the repository with a root directory for persona files.

        Args:
            root: Path to the directory containing persona Markdown files.
        """
        self.root = Path(root)

    @classmethod
    def from_data_dir(cls, data_dir: Path | str) -> PersonaFileRepository:
        """Create a repository rooted at ``<data_dir>/personas``.

        Args:
            data_dir: Application data directory.

        Returns:
            A new PersonaFileRepository instance.
        """
        return cls(Path(data_dir) / "personas")

    def ensure_default_persona(self) -> Path:
        """Copy the packaged default persona if the user has not created one."""

        target = self.root / f"{DEFAULT_PERSONA_ID}.md"
        if target.exists():
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(_default_persona_source_path(), target)
        return target

    def list(self) -> list[dict[str, object]]:
        """Return all persona records as serialised dictionaries.

        Returns:
            A list of persona payload dicts sorted by default-first, then name.
        """
        if not self.root.is_dir():
            return []
        records = [self._load_file(path) for path in sorted(self.root.glob("*.md"))]
        return [serialize_persona_record(record) for record in sorted(records, key=_sort_key)]

    def get(self, persona_id: str) -> dict[str, object] | None:
        """Look up a persona by its normalised ID.

        Args:
            persona_id: The persona identifier.

        Returns:
            Serialised persona dict, or ``None`` if not found.
        """
        normalized = normalize_persona_id(persona_id)
        path = self.root / f"{normalized}.md"
        if not path.is_file():
            return None
        return serialize_persona_record(self._load_file(path))

    def get_by_name(self, name: str) -> dict[str, object] | None:
        """Look up a persona by display name.

        Args:
            name: The persona display name to search for.

        Returns:
            Serialised persona dict, or ``None`` if not found.
        """
        normalized = name.strip()
        if not normalized:
            return None
        for payload in self.list():
            if payload["name"] == normalized:
                return payload
        return None

    def create(
        self,
        *,
        persona_id: str | None,
        name: str,
        prompt_text: str,
        tags: list[str],
        enabled: bool,
        few_shot: list[FewShotExample] | None = None,
    ) -> dict[str, object]:
        """Create a new persona file on disk.

        Args:
            persona_id: Optional explicit persona ID; derived from name if omitted.
            name: Display name for the persona.
            prompt_text: The prompt body text.
            tags: List of tags.
            enabled: Whether the persona is enabled.
            few_shot: Optional list of few-shot examples.

        Returns:
            Serialised payload of the newly created persona.

        Raises:
            PersonaFileError: If a persona with the same ID or name already exists.
        """
        normalized_name, normalized_prompt = normalize_persona_input(name, prompt_text)
        normalized_tags = normalize_persona_tags(tags)
        normalized_id = normalize_persona_id(persona_id or _derive_persona_id(normalized_name))
        path = self.root / f"{normalized_id}.md"
        if path.exists():
            raise PersonaFileError(
                status_code=409,
                code="PERSONA_ALREADY_EXISTS",
                message=f"Persona {normalized_id!r} already exists",
            )
        if self.get_by_name(normalized_name) is not None:
            raise PersonaFileError(
                status_code=409,
                code="PERSONA_ALREADY_EXISTS",
                message=f"Persona {normalized_name!r} already exists",
            )
        now = utc_now_iso()
        self._write_file(
            path,
            persona_id=normalized_id,
            name=normalized_name,
            prompt_text=normalized_prompt,
            tags=normalized_tags,
            enabled=enabled,
            created_at=now,
            updated_at=now,
            few_shot=few_shot,
        )
        payload = self.get(normalized_id)
        assert payload is not None
        return payload

    def update(
        self,
        persona_id: str,
        *,
        name: str,
        prompt_text: str,
        tags: list[str],
        enabled: bool,
        few_shot: list[FewShotExample] | None = None,
    ) -> dict[str, object]:
        """Update an existing persona file on disk.

        Args:
            persona_id: The persona identifier to update.
            name: New display name.
            prompt_text: New prompt body text.
            tags: New list of tags.
            enabled: New enabled state.
            few_shot: Optional list of few-shot examples.

        Returns:
            Serialised payload of the updated persona.

        Raises:
            PersonaFileError: If the persona does not exist or name conflicts.
        """
        normalized_id = normalize_persona_id(persona_id)
        current = self.get(normalized_id)
        if current is None:
            raise PersonaFileError(
                status_code=404,
                code="PERSONA_NOT_FOUND",
                message=f"Persona {normalized_id!r} was not found",
            )
        normalized_name, normalized_prompt = normalize_persona_input(name, prompt_text)
        normalized_tags = normalize_persona_tags(tags)
        existing = self.get_by_name(normalized_name)
        if existing is not None and existing["uuid"] != normalized_id:
            raise PersonaFileError(
                status_code=409,
                code="PERSONA_ALREADY_EXISTS",
                message=f"Persona {normalized_name!r} already exists",
            )
        self._write_file(
            self.root / f"{normalized_id}.md",
            persona_id=normalized_id,
            name=normalized_name,
            prompt_text=normalized_prompt,
            tags=normalized_tags,
            enabled=enabled,
            created_at=str(current["created_at"]),
            updated_at=utc_now_iso(),
            version=str(current.get("version") or "1.0.0"),
            description=str(current.get("description") or ""),
            few_shot=few_shot,
        )
        payload = self.get(normalized_id)
        assert payload is not None
        return payload

    def delete(self, persona_id: str) -> None:
        """Delete a persona file by its normalised ID.

        Args:
            persona_id: The persona identifier.

        Raises:
            PersonaFileError: If the persona does not exist.
        """
        normalized = normalize_persona_id(persona_id)
        path = self.root / f"{normalized}.md"
        if not path.is_file():
            raise PersonaFileError(
                status_code=404,
                code="PERSONA_NOT_FOUND",
                message=f"Persona {normalized!r} was not found",
            )
        path.unlink()

    def _load_file(self, path: Path) -> PersonaFileRecord:
        try:
            front_matter, body = parse_prompt_markdown(
                path.read_text(encoding="utf-8"),
                path=path,
            )
        except PromptFileError as exc:
            raise PersonaFileError(
                status_code=500,
                code="INVALID_PERSONA_FILE",
                message=str(exc),
            ) from exc

        persona_id = normalize_persona_id(str(front_matter.get("id") or path.stem))
        expected_path = self.root / f"{persona_id}.md"
        if path.name != expected_path.name:
            raise PersonaFileError(
                status_code=500,
                code="INVALID_PERSONA_FILE",
                message=f"Persona file {path} id must match file name",
            )
        name = str(front_matter.get("name") or persona_id).strip()
        prompt_text = body.strip()
        if not prompt_text:
            raise PersonaFileError(
                status_code=500,
                code="INVALID_PERSONA_FILE",
                message=f"Persona file {path} body must not be empty",
            )
        return PersonaFileRecord(
            uuid=persona_id,
            name=name,
            prompt_text=prompt_text,
            tags=normalize_persona_tags(_list(front_matter.get("tags"))),
            enabled=bool(front_matter.get("enabled", True)),
            created_at=str(front_matter.get("created_at") or ""),
            updated_at=str(front_matter.get("updated_at") or path.stat().st_mtime),
            path=path,
            version=str(front_matter.get("version") or "1.0.0"),
            description=str(front_matter.get("description") or ""),
            few_shot=_parse_few_shot(front_matter.get("few_shot")),
        )

    def _write_file(
        self,
        path: Path,
        *,
        persona_id: str,
        name: str,
        prompt_text: str,
        tags: list[str],
        enabled: bool,
        created_at: str,
        updated_at: str,
        version: str = "1.0.0",
        description: str = "",
        few_shot: list[FewShotExample] | None = None,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        text = render_persona_markdown(
            persona_id=persona_id,
            name=name,
            prompt_text=prompt_text,
            tags=tags,
            enabled=enabled,
            created_at=created_at,
            updated_at=updated_at,
            version=version,
            description=description,
            few_shot=few_shot,
        )
        path.write_text(text, encoding="utf-8")


def serialize_persona_record(record: PersonaFileRecord) -> dict[str, object]:
    """Convert a ``PersonaFileRecord`` to a plain dictionary.

    Args:
        record: The persona file record to serialise.

    Returns:
        A dict suitable for API responses.
    """
    return {
        "uuid": record.uuid,
        "name": record.name,
        "prompt_text": record.prompt_text,
        "tags": record.tags,
        "enabled": record.enabled,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "path": str(record.path),
        "version": record.version,
        "description": record.description,
        "few_shot": [{"user": ex.user, "assistant": ex.assistant} for ex in record.few_shot],
    }


def serialize_persona(payload: dict[str, object]) -> dict[str, object]:
    """Map an internal persona payload to the camelCase API shape.

    Args:
        payload: Internal persona dict with snake_case keys.

    Returns:
        A dict with camelCase keys for the front-end.
    """
    few_shot = payload.get("few_shot", [])
    return {
        "uuid": payload["uuid"],
        "name": payload["name"],
        "promptText": payload["prompt_text"],
        "tags": payload["tags"],
        "enabled": payload["enabled"],
        "createdAt": payload["created_at"],
        "lastModified": payload["updated_at"],
        "fewShotExamples": [
            {"user": ex["user"], "assistant": ex["assistant"]}
            for ex in few_shot
            if isinstance(ex, dict)
        ],
    }


def persona_component_id(persona_id: str) -> str:
    """Return the prompt component ID for a given persona.

    Args:
        persona_id: The persona identifier.

    Returns:
        A component ID string in the form ``persona.<id>``.
    """
    return f"persona.{normalize_persona_id(persona_id)}"


def persona_prompt_component(payload: dict[str, object]) -> PromptComponent:
    """Build a ``PromptComponent`` from a serialised persona payload.

    Args:
        payload: Serialised persona dict (snake_case keys).

    Returns:
        A PromptComponent placed in the ``IDENTITY`` stage.
    """
    persona_id = normalize_persona_id(str(payload["uuid"]))
    prompt_text = str(payload.get("prompt_text") or "").strip()

    # Append few-shot examples if present
    few_shot = payload.get("few_shot", [])
    if isinstance(few_shot, list) and few_shot:
        few_shot_lines = [
            "",
            "---",
            "【对话风格示例 - 以下为示例对话，非实际历史消息，请勿回复或总结这些内容】",
        ]
        for ex in few_shot:
            if isinstance(ex, dict):
                user = str(ex.get("user") or "").strip()
                assistant = str(ex.get("assistant") or "").strip()
                if user and assistant:
                    few_shot_lines.append(f"[示例] 用户: {user}")
                    few_shot_lines.append(f"[示例] 助手: {assistant}")
        few_shot_lines.append("【示例结束】")
        prompt_text = prompt_text + "\n" + "\n".join(few_shot_lines)

    return PromptComponent(
        id=persona_component_id(persona_id),
        stage=PromptStage.IDENTITY,
        kind=PromptComponentKind.STATIC_TEXT,
        version=str(payload.get("version") or "1.0.0"),
        priority=100,
        enabled=bool(payload.get("enabled", True)),
        content=prompt_text,
        tags=[str(item) for item in _list(payload.get("tags"))],
        metadata={
            "display_name": str(payload.get("name") or persona_id),
            "description": str(payload.get("description") or ""),
            "source_type": "persona",
            "source_id": persona_id,
            "persona_file": str(payload.get("path") or ""),
        },
    )


def normalize_persona_id(value: str) -> str:
    """Validate and normalise a persona identifier.

    Args:
        value: Raw persona ID string.

    Returns:
        The stripped, validated persona ID.

    Raises:
        PersonaFileError: If the ID is empty or contains invalid characters.
    """
    normalized = value.strip()
    if not normalized or not PERSONA_ID_RE.fullmatch(normalized):
        raise PersonaFileError(
            status_code=422,
            code="INVALID_ACTION",
            message="Persona id must be a safe file name stem",
        )
    return normalized


def normalize_persona_input(name: str, prompt_text: str) -> tuple[str, str]:
    """Validate and strip a persona name and prompt text pair.

    Args:
        name: Raw persona name.
        prompt_text: Raw prompt text.

    Returns:
        A tuple of (stripped name, stripped prompt text).

    Raises:
        PersonaFileError: If either field is empty after stripping.
    """
    normalized_name = name.strip()
    normalized_prompt = prompt_text.strip()
    if not normalized_name:
        raise PersonaFileError(
            status_code=400,
            code="INVALID_ACTION",
            message="Persona name must not be empty",
        )
    if not normalized_prompt:
        raise PersonaFileError(
            status_code=400,
            code="INVALID_ACTION",
            message="Persona promptText must not be empty",
        )
    return normalized_name, normalized_prompt


def normalize_persona_tags(tags: list[str]) -> list[str]:
    """Deduplicate and strip a list of persona tags.

    Args:
        tags: Raw tag strings.

    Returns:
        Deduplicated list of stripped, non-empty tags.
    """
    seen: set[str] = set()
    normalized: list[str] = []
    for tag in tags:
        value = str(tag).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def render_persona_markdown(
    *,
    persona_id: str,
    name: str,
    prompt_text: str,
    tags: list[str],
    enabled: bool,
    created_at: str,
    updated_at: str,
    version: str = "1.0.0",
    description: str = "",
    few_shot: list[FewShotExample] | None = None,
) -> str:
    """Render a persona as a Markdown file with YAML front-matter.

    Args:
        persona_id: The persona identifier.
        name: Display name.
        prompt_text: The prompt body text.
        tags: List of tags.
        enabled: Whether the persona is enabled.
        created_at: ISO timestamp of creation.
        updated_at: ISO timestamp of last update.
        version: Semantic version string.
        description: Optional human-readable description.
        few_shot: Optional list of few-shot examples.

    Returns:
        A complete Markdown string with YAML front-matter.
    """
    lines = [
        "---",
        f"id: {_yaml_string(persona_id)}",
        f"name: {_yaml_string(name)}",
        f"version: {_yaml_string(version)}",
        f"enabled: {_yaml_bool(enabled)}",
    ]
    if description:
        lines.append(f"description: {_yaml_string(description)}")
    if few_shot:
        lines.append("few_shot:")
        for ex in few_shot:
            lines.append(f"  - user: {_yaml_string(ex.user)}")
            lines.append(f"    assistant: {_yaml_string(ex.assistant)}")
    lines.extend(
        [
            "tags:",
            *[f"  - {_yaml_string(tag)}" for tag in tags],
            f"created_at: {_yaml_string(created_at)}",
            f"updated_at: {_yaml_string(updated_at)}",
            "---",
            "",
            prompt_text.strip(),
            "",
        ]
    )
    return "\n".join(lines)


def _derive_persona_id(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_.-]+", "-", name.strip()).strip(".-")
    return stem if stem else f"persona-{uuid4().hex[:8]}"


def _default_persona_source_path() -> Path:
    return Path(__file__).resolve().parents[1] / "agent" / "personas" / "default.md"


def _sort_key(record: PersonaFileRecord) -> tuple[int, str, str]:
    return (0 if record.uuid == DEFAULT_PERSONA_ID else 1, record.name, record.uuid)


def _list(value: object) -> list[object]:
    return list(value) if isinstance(value, list | tuple) else []


def _parse_few_shot(value: object) -> list[FewShotExample]:
    """Parse few-shot examples from YAML front-matter."""
    if not isinstance(value, list):
        return []
    examples: list[FewShotExample] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        user = str(item.get("user") or "").strip()
        assistant = str(item.get("assistant") or "").strip()
        if user and assistant:
            examples.append(FewShotExample(user=user, assistant=assistant))
    return examples


def _yaml_bool(value: bool) -> str:
    return "true" if value else "false"


def _yaml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


__all__ = [
    "DEFAULT_PERSONA_ID",
    "PersonaFileError",
    "PersonaFileRepository",
    "normalize_persona_id",
    "normalize_persona_input",
    "normalize_persona_tags",
    "persona_component_id",
    "persona_prompt_component",
    "render_persona_markdown",
    "serialize_persona",
]
