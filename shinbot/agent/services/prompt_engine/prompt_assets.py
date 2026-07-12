"""Version-aware synchronization for editable runtime prompt assets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from difflib import SequenceMatcher
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml

_MANIFEST_SCHEMA_VERSION = 1
_SOURCE_OWNED_FRONT_MATTER_KEYS = frozenset(
    {
        "id",
        "stage",
        "kind",
        "resolver_ref",
        "template_vars",
        "bundle_refs",
    }
)

# Runtime prompt copies created before asset baselines existed can only be
# upgraded safely when their digest matches a source revision shipped by an
# earlier ShinBot release.
KNOWN_BUILTIN_PROMPT_HASHES: dict[tuple[str, str], frozenset[str]] = {
    (
        "zh-CN",
        "review.idle_review_planning.task",
    ): frozenset(
        {
            "371b3da1eaff10b89305a6f640531ab39f70195631856fd9b8f99b1f8f95f773",
            "b1c5859453ed2bfae4c7554b3d78e02558219cf69fca1ac8b17a69c628df0fc8",
            "ab249d2f69cd2614e2f3ebf434560bdead910220faf5c236e64d4745c8de80f8",
        }
    ),
    (
        "zh-CN",
        "review.idle_review_planning.constraints",
    ): frozenset(
        {
            "dd4a6cd0a8d180cac9d05fd6825f03bf77b165c8b55150011f01d08e4fb566ab",
            "3882a3357b23f0061dee1388b984abcdafba3370eeabd33e8c5ec64446796b20",
            "6fd3d146a7384fc42fb174021660b957b7f345c16edb81d0e0548762ece5d5ab",
        }
    ),
    (
        "en-US",
        "review.idle_review_planning.task",
    ): frozenset(
        {
            "e7dd2d09f0d18891505fea44643c19db3e36775864391d6d8153b05336b7247f",
            "f61fdac371ba6e344e744d18c2c460b58b51410aee4c9743fc1a31bacc3792dc",
            "cf561e437c195fcf3b757fedfc20a4de0fe0d1207bc883bfaf96357d443b8d91",
        }
    ),
    (
        "en-US",
        "review.idle_review_planning.constraints",
    ): frozenset(
        {
            "fc7737e922ffa118238bf47175ddbc4b8a2a29686c4407f78dc668d220f1084f",
            "083c140d235299b4e482fd67b3359bdca844551c6ba4da813ad2c084c29ce387",
            "f6518d5f75787d5609f405abf528ff6203e09c23ee75b428e8d85da69254199a",
        }
    ),
}


class PromptSyncStatus(StrEnum):
    """Result of reconciling a package prompt with its runtime copy."""

    SOURCE_ONLY = "source_only"
    SYNCED = "synced"
    AUTO_UPDATED = "auto_updated"
    USER_MODIFIED = "user_modified"
    MERGED = "merged"
    CONFLICT = "conflict"
    UNTRACKED_RUNTIME = "untracked_runtime"


@dataclass(slots=True, frozen=True)
class PromptAssetRevision:
    """Content identity for one prompt asset revision."""

    version: str
    sha256: str


@dataclass(slots=True, frozen=True)
class PromptAssetSyncResult:
    """Resolved runtime asset and its source synchronization state."""

    prompt_id: str
    locale: str
    status: PromptSyncStatus
    source_revision: PromptAssetRevision
    runtime_revision: PromptAssetRevision | None
    base_revision: PromptAssetRevision | None
    active_path: Path
    pending_path: Path | None = None


@dataclass(slots=True, frozen=True)
class _LineEdit:
    start: int
    end: int
    replacement: tuple[str, ...]


class PromptAssetSynchronizer:
    """Synchronize package prompts with editable runtime copies safely."""

    def __init__(
        self,
        data_root: Path | str,
        *,
        known_hashes: dict[tuple[str, str], frozenset[str]] | None = None,
    ) -> None:
        self._data_root = Path(data_root)
        self._metadata_root = self._data_root / ".shinbot"
        self._manifest_path = self._metadata_root / "assets.json"
        self._baseline_root = self._metadata_root / "baselines"
        self._pending_root = self._metadata_root / "pending"
        self._known_hashes = (
            KNOWN_BUILTIN_PROMPT_HASHES if known_hashes is None else known_hashes
        )

    def sync(
        self,
        *,
        prompt_id: str,
        locale: str,
        source_path: Path,
        runtime_path: Path,
    ) -> PromptAssetSyncResult:
        """Resolve one runtime prompt through source/base/local reconciliation."""

        source_content = source_path.read_text(encoding="utf-8")
        source_revision = _revision(source_content)
        key = _manifest_key(prompt_id, locale)
        manifest = self._load_manifest()
        entry = _mapping(manifest["assets"].get(key))
        baseline_path = self._baseline_path(prompt_id, locale)
        pending_path = self._pending_path(prompt_id, locale)

        if not runtime_path.exists():
            _atomic_write(runtime_path, source_content)
            _atomic_write(baseline_path, source_content)
            pending_path.unlink(missing_ok=True)
            result = PromptAssetSyncResult(
                prompt_id=prompt_id,
                locale=locale,
                status=PromptSyncStatus.SYNCED,
                source_revision=source_revision,
                runtime_revision=source_revision,
                base_revision=source_revision,
                active_path=runtime_path,
            )
            self._record_result(manifest, key, result)
            return result

        runtime_content = runtime_path.read_text(encoding="utf-8")
        runtime_revision = _revision(runtime_content)

        if not entry or not baseline_path.exists():
            known_hashes = self._known_hashes.get((locale, prompt_id), frozenset())
            if (
                runtime_revision.sha256 == source_revision.sha256
                or runtime_revision.sha256 in known_hashes
            ):
                _atomic_write(runtime_path, source_content)
                _atomic_write(baseline_path, source_content)
                pending_path.unlink(missing_ok=True)
                result = PromptAssetSyncResult(
                    prompt_id=prompt_id,
                    locale=locale,
                    status=(
                        PromptSyncStatus.SYNCED
                        if runtime_revision.sha256 == source_revision.sha256
                        else PromptSyncStatus.AUTO_UPDATED
                    ),
                    source_revision=source_revision,
                    runtime_revision=source_revision,
                    base_revision=source_revision,
                    active_path=runtime_path,
                )
            else:
                _atomic_write(pending_path, source_content)
                result = PromptAssetSyncResult(
                    prompt_id=prompt_id,
                    locale=locale,
                    status=PromptSyncStatus.UNTRACKED_RUNTIME,
                    source_revision=source_revision,
                    runtime_revision=runtime_revision,
                    base_revision=None,
                    active_path=runtime_path,
                    pending_path=pending_path,
                )
            self._record_result(manifest, key, result)
            return result

        baseline_content = baseline_path.read_text(encoding="utf-8")
        baseline_revision = _revision(baseline_content)

        if runtime_revision.sha256 == source_revision.sha256:
            _atomic_write(baseline_path, source_content)
            pending_path.unlink(missing_ok=True)
            result = PromptAssetSyncResult(
                prompt_id=prompt_id,
                locale=locale,
                status=PromptSyncStatus.SYNCED,
                source_revision=source_revision,
                runtime_revision=runtime_revision,
                base_revision=source_revision,
                active_path=runtime_path,
            )
        elif runtime_revision.sha256 == baseline_revision.sha256:
            _atomic_write(runtime_path, source_content)
            _atomic_write(baseline_path, source_content)
            pending_path.unlink(missing_ok=True)
            result = PromptAssetSyncResult(
                prompt_id=prompt_id,
                locale=locale,
                status=PromptSyncStatus.AUTO_UPDATED,
                source_revision=source_revision,
                runtime_revision=source_revision,
                base_revision=source_revision,
                active_path=runtime_path,
            )
        elif source_revision.sha256 == baseline_revision.sha256:
            pending_path.unlink(missing_ok=True)
            result = PromptAssetSyncResult(
                prompt_id=prompt_id,
                locale=locale,
                status=PromptSyncStatus.USER_MODIFIED,
                source_revision=source_revision,
                runtime_revision=runtime_revision,
                base_revision=baseline_revision,
                active_path=runtime_path,
            )
        else:
            merged = _merge_prompt_content(
                base=baseline_content,
                local=runtime_content,
                incoming=source_content,
            )
            if merged is None:
                _atomic_write(pending_path, source_content)
                result = PromptAssetSyncResult(
                    prompt_id=prompt_id,
                    locale=locale,
                    status=PromptSyncStatus.CONFLICT,
                    source_revision=source_revision,
                    runtime_revision=runtime_revision,
                    base_revision=baseline_revision,
                    active_path=runtime_path,
                    pending_path=pending_path,
                )
            else:
                _atomic_write(runtime_path, merged)
                _atomic_write(baseline_path, source_content)
                pending_path.unlink(missing_ok=True)
                result = PromptAssetSyncResult(
                    prompt_id=prompt_id,
                    locale=locale,
                    status=PromptSyncStatus.MERGED,
                    source_revision=source_revision,
                    runtime_revision=_revision(merged),
                    base_revision=source_revision,
                    active_path=runtime_path,
                )

        self._record_result(manifest, key, result)
        return result

    def _record_result(
        self,
        manifest: dict[str, Any],
        key: str,
        result: PromptAssetSyncResult,
    ) -> None:
        manifest["assets"][key] = {
            "prompt_id": result.prompt_id,
            "locale": result.locale,
            "status": result.status.value,
            "source": _revision_payload(result.source_revision),
            "runtime": _revision_payload(result.runtime_revision),
            "base": _revision_payload(result.base_revision),
            "active_path": str(result.active_path),
            "pending_path": str(result.pending_path) if result.pending_path else "",
        }
        _atomic_write(
            self._manifest_path,
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        )

    def _load_manifest(self) -> dict[str, Any]:
        if not self._manifest_path.exists():
            return {"schema_version": _MANIFEST_SCHEMA_VERSION, "assets": {}}
        try:
            payload = json.loads(self._manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"schema_version": _MANIFEST_SCHEMA_VERSION, "assets": {}}
        assets = payload.get("assets")
        return {
            "schema_version": _MANIFEST_SCHEMA_VERSION,
            "assets": dict(assets) if isinstance(assets, dict) else {},
        }

    def _baseline_path(self, prompt_id: str, locale: str) -> Path:
        return self._baseline_root / locale / f"{prompt_id}.md"

    def _pending_path(self, prompt_id: str, locale: str) -> Path:
        return self._pending_root / locale / f"{prompt_id}.md"


def _revision(content: str) -> PromptAssetRevision:
    front_matter = _front_matter(content)
    return PromptAssetRevision(
        version=str(front_matter.get("version") or "1.0.0"),
        sha256=hashlib.sha256(content.encode("utf-8")).hexdigest(),
    )


def _front_matter(content: str) -> dict[str, Any]:
    if not content.startswith("---\n"):
        return {}
    try:
        raw, _body = content[4:].split("\n---", 1)
        parsed = yaml.safe_load(raw) or {}
    except (ValueError, yaml.YAMLError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _merge_prompt_content(*, base: str, local: str, incoming: str) -> str | None:
    base_front = _front_matter(base)
    local_front = _front_matter(local)
    for key in _SOURCE_OWNED_FRONT_MATTER_KEYS:
        if local_front.get(key) != base_front.get(key):
            return None

    base_lines = tuple(base.splitlines(keepends=True))
    local_edits = _line_edits(base_lines, tuple(local.splitlines(keepends=True)))
    incoming_edits = _line_edits(base_lines, tuple(incoming.splitlines(keepends=True)))
    merged_edits = list(local_edits)
    for incoming_edit in incoming_edits:
        identical = next((edit for edit in merged_edits if edit == incoming_edit), None)
        if identical is not None:
            continue
        if any(_edits_overlap(incoming_edit, edit) for edit in merged_edits):
            return None
        merged_edits.append(incoming_edit)

    result = list(base_lines)
    for edit in sorted(merged_edits, key=lambda item: (item.start, item.end), reverse=True):
        result[edit.start : edit.end] = edit.replacement
    return "".join(result)


def _line_edits(base: tuple[str, ...], updated: tuple[str, ...]) -> list[_LineEdit]:
    matcher = SequenceMatcher(a=base, b=updated, autojunk=False)
    return [
        _LineEdit(start=i1, end=i2, replacement=updated[j1:j2])
        for tag, i1, i2, j1, j2 in matcher.get_opcodes()
        if tag != "equal"
    ]


def _edits_overlap(left: _LineEdit, right: _LineEdit) -> bool:
    if left.start == left.end and right.start == right.end:
        return left.start == right.start
    if left.start == left.end:
        return right.start <= left.start <= right.end
    if right.start == right.end:
        return left.start <= right.start <= left.end
    return max(left.start, right.start) < min(left.end, right.end)


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file_obj:
            file_obj.write(content)
            file_obj.flush()
            os.fsync(file_obj.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _manifest_key(prompt_id: str, locale: str) -> str:
    return f"{locale}:{prompt_id}"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _revision_payload(revision: PromptAssetRevision | None) -> dict[str, str] | None:
    if revision is None:
        return None
    return {"version": revision.version, "sha256": revision.sha256}


__all__ = [
    "KNOWN_BUILTIN_PROMPT_HASHES",
    "PromptAssetRevision",
    "PromptAssetSynchronizer",
    "PromptAssetSyncResult",
    "PromptSyncStatus",
]
