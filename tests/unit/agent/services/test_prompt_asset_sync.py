from __future__ import annotations

import hashlib
import json
from pathlib import Path

from shinbot.agent.services.prompt_engine.prompt_assets import (
    PromptAssetSynchronizer,
    PromptSyncStatus,
)


def _prompt(body: str, *, version: str = "1.0.0") -> str:
    return (
        "---\n"
        "id: test.prompt\n"
        "stage: instructions\n"
        "kind: static_text\n"
        f"version: {version}\n"
        "priority: 100\n"
        "---\n\n"
        f"{body}\n"
    )


def _sync(
    tmp_path: Path,
    *,
    source_content: str,
    known_hashes: dict[tuple[str, str], frozenset[str]] | None = None,
):
    source_path = tmp_path / "package" / "test.prompt.md"
    runtime_path = tmp_path / "data" / "prompts" / "en-US" / "test.prompt.md"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text(source_content, encoding="utf-8")
    synchronizer = PromptAssetSynchronizer(
        tmp_path / "data" / "prompts",
        known_hashes=known_hashes,
    )
    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )
    return synchronizer, source_path, runtime_path, result


def test_prompt_asset_first_sync_creates_runtime_baseline_and_manifest(tmp_path: Path) -> None:
    source = _prompt("Initial source.")

    _synchronizer, _source_path, runtime_path, result = _sync(
        tmp_path,
        source_content=source,
    )

    assert result.status == PromptSyncStatus.SYNCED
    assert runtime_path.read_text(encoding="utf-8") == source
    baseline = (
        tmp_path
        / "data"
        / "prompts"
        / ".shinbot"
        / "baselines"
        / "en-US"
        / "test.prompt.md"
    )
    assert baseline.read_text(encoding="utf-8") == source
    manifest = json.loads(
        (tmp_path / "data" / "prompts" / ".shinbot" / "assets.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["assets"]["en-US:test.prompt"]["status"] == "synced"


def test_prompt_asset_auto_updates_unmodified_runtime(tmp_path: Path) -> None:
    synchronizer, source_path, runtime_path, _result = _sync(
        tmp_path,
        source_content=_prompt("Initial source."),
    )
    incoming = _prompt("Updated source.", version="1.1.0")
    source_path.write_text(incoming, encoding="utf-8")

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    assert result.status == PromptSyncStatus.AUTO_UPDATED
    assert runtime_path.read_text(encoding="utf-8") == incoming
    assert result.runtime_revision is not None
    assert result.runtime_revision.version == "1.1.0"


def test_prompt_asset_preserves_user_change_when_source_is_unchanged(tmp_path: Path) -> None:
    synchronizer, source_path, runtime_path, _result = _sync(
        tmp_path,
        source_content=_prompt("Initial source."),
    )
    local = _prompt("User override.")
    runtime_path.write_text(local, encoding="utf-8")

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    assert result.status == PromptSyncStatus.USER_MODIFIED
    assert runtime_path.read_text(encoding="utf-8") == local
    assert result.pending_path is None


def test_prompt_asset_three_way_merges_non_overlapping_changes(tmp_path: Path) -> None:
    base = _prompt("First line.\nMiddle line.\nLast line.")
    synchronizer, source_path, runtime_path, _result = _sync(
        tmp_path,
        source_content=base,
    )
    runtime_path.write_text(
        _prompt("User first line.\nMiddle line.\nLast line."),
        encoding="utf-8",
    )
    source_path.write_text(
        _prompt("First line.\nMiddle line.\nUpdated last line.", version="1.1.0"),
        encoding="utf-8",
    )

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    merged = runtime_path.read_text(encoding="utf-8")
    assert result.status == PromptSyncStatus.MERGED
    assert "User first line." in merged
    assert "Updated last line." in merged
    assert "version: 1.1.0" in merged


def test_prompt_asset_keeps_local_and_stages_incoming_on_conflict(tmp_path: Path) -> None:
    synchronizer, source_path, runtime_path, _result = _sync(
        tmp_path,
        source_content=_prompt("Original line."),
    )
    local = _prompt("User line.")
    incoming = _prompt("Package line.", version="1.1.0")
    runtime_path.write_text(local, encoding="utf-8")
    source_path.write_text(incoming, encoding="utf-8")

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    assert result.status == PromptSyncStatus.CONFLICT
    assert runtime_path.read_text(encoding="utf-8") == local
    assert result.pending_path is not None
    assert result.pending_path.read_text(encoding="utf-8") == incoming


def test_prompt_asset_upgrades_known_legacy_runtime_without_baseline(tmp_path: Path) -> None:
    legacy = _prompt("Legacy built-in.")
    incoming = _prompt("Current built-in.", version="2.0.0")
    digest = hashlib.sha256(legacy.encode("utf-8")).hexdigest()
    source_path = tmp_path / "package" / "test.prompt.md"
    runtime_path = tmp_path / "data" / "prompts" / "en-US" / "test.prompt.md"
    source_path.parent.mkdir(parents=True)
    runtime_path.parent.mkdir(parents=True)
    source_path.write_text(incoming, encoding="utf-8")
    runtime_path.write_text(legacy, encoding="utf-8")
    synchronizer = PromptAssetSynchronizer(
        tmp_path / "data" / "prompts",
        known_hashes={("en-US", "test.prompt"): frozenset({digest})},
    )

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    assert result.status == PromptSyncStatus.AUTO_UPDATED
    assert runtime_path.read_text(encoding="utf-8") == incoming


def test_prompt_asset_adopts_current_runtime_without_manifest(tmp_path: Path) -> None:
    current = _prompt("Current built-in.", version="2.0.0")
    source_path = tmp_path / "package" / "test.prompt.md"
    runtime_path = tmp_path / "data" / "prompts" / "en-US" / "test.prompt.md"
    source_path.parent.mkdir(parents=True)
    runtime_path.parent.mkdir(parents=True)
    source_path.write_text(current, encoding="utf-8")
    runtime_path.write_text(current, encoding="utf-8")
    synchronizer = PromptAssetSynchronizer(
        tmp_path / "data" / "prompts",
        known_hashes={},
    )

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    assert result.status == PromptSyncStatus.SYNCED
    assert result.base_revision == result.source_revision
    assert result.pending_path is None


def test_prompt_asset_preserves_unknown_legacy_runtime(tmp_path: Path) -> None:
    local = _prompt("Possibly user-authored legacy content.")
    source_path = tmp_path / "package" / "test.prompt.md"
    runtime_path = tmp_path / "data" / "prompts" / "en-US" / "test.prompt.md"
    source_path.parent.mkdir(parents=True)
    runtime_path.parent.mkdir(parents=True)
    source_path.write_text(_prompt("Current source.", version="2.0.0"), encoding="utf-8")
    runtime_path.write_text(local, encoding="utf-8")
    synchronizer = PromptAssetSynchronizer(
        tmp_path / "data" / "prompts",
        known_hashes={},
    )

    result = synchronizer.sync(
        prompt_id="test.prompt",
        locale="en-US",
        source_path=source_path,
        runtime_path=runtime_path,
    )

    assert result.status == PromptSyncStatus.UNTRACKED_RUNTIME
    assert runtime_path.read_text(encoding="utf-8") == local
    assert result.pending_path is not None
