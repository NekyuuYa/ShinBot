from __future__ import annotations

from pathlib import Path

import pytest

from shinbot.agent.services.prompt_engine import (
    PromptComponentKind,
    PromptRegistry,
    PromptStage,
)
from shinbot.agent.services.prompt_engine.files import (
    PromptFileError,
    load_prompt_component,
    parse_prompt_markdown,
    register_prompt_files,
)

PROMPT_SOURCE_ROOT = Path(__file__).resolve().parents[4] / "shinbot" / "agent"
PROMPT_LOCALES = {"zh-CN", "en-US"}
PREFIX_PROMPT_IDS = {
    "active_chat.fast_mode.conversation_summary",
    "active_chat.handoff.digest",
    "active_chat.handoff.legacy",
    "active_chat.handoff.overflow",
    "builtin.context.active_alias",
    "builtin.context.compressed_memory",
    "builtin.context.compressed_memory_alias",
    "builtin.context.compressed_memory_source",
    "builtin.context.inactive_alias",
    "builtin.context.long_term_memory",
}
IDLE_REVIEW_PLANNING_PROMPT_IDS = {
    "review.idle_review_planning.task",
    "review.idle_review_planning.constraints",
}


def test_load_prompt_component_from_markdown(tmp_path: Path) -> None:
    path = tmp_path / "test.sample.md"
    path.write_text(
        """---
id: test.sample
stage: instructions
kind: static_text
priority: 42
enabled: true
tags:
  - test
metadata:
  builtin: true
  display_name: Test Sample
---

Hello from a prompt file.
""",
        encoding="utf-8",
    )

    component = load_prompt_component(path, locale="zh-CN")

    assert component.id == "test.sample"
    assert component.stage == PromptStage.INSTRUCTIONS
    assert component.priority == 42
    assert component.tags == ["test"]
    assert component.metadata["locale"] == "zh-CN"
    assert component.metadata["display_name"] == "Test Sample"
    assert component.content == "Hello from a prompt file."


def test_builtin_prompt_files_have_zh_cn_and_en_us_sources() -> None:
    prompt_files = sorted(PROMPT_SOURCE_ROOT.glob("**/prompts/*/*.md"))
    assert prompt_files

    locales_by_id: dict[str, set[str]] = {}
    for path in prompt_files:
        front_matter, _body = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
        prompt_id = str(front_matter["id"])
        locales_by_id.setdefault(prompt_id, set()).add(path.parent.name)

    missing_locales = {
        prompt_id: sorted(PROMPT_LOCALES - locales)
        for prompt_id, locales in locales_by_id.items()
        if locales != PROMPT_LOCALES
    }

    assert missing_locales == {}


def test_prefix_prompt_files_are_marked_internal() -> None:
    failures: list[str] = []
    for path in sorted(PROMPT_SOURCE_ROOT.glob("**/prompts/*/*.md")):
        front_matter, _body = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
        prompt_id = str(front_matter["id"])
        if prompt_id not in PREFIX_PROMPT_IDS:
            continue

        tags = {str(tag) for tag in front_matter.get("tags") or []}
        metadata = dict(front_matter.get("metadata") or {})
        if not {"prefix", "internal"}.issubset(tags):
            failures.append(f"{path}: missing prefix/internal tags")
        if metadata.get("internal") is not True:
            failures.append(f"{path}: missing metadata.internal=true")
        if metadata.get("prompt_role") != "prefix":
            failures.append(f"{path}: missing metadata.prompt_role=prefix")

    assert failures == []


def test_idle_review_planning_prompt_defines_conservative_time_scale() -> None:
    failures: list[str] = []
    for path in sorted(PROMPT_SOURCE_ROOT.glob("**/prompts/*/*.md")):
        front_matter, body = parse_prompt_markdown(path.read_text(encoding="utf-8"), path=path)
        prompt_id = str(front_matter["id"])
        if prompt_id not in IDLE_REVIEW_PLANNING_PROMPT_IDS:
            continue

        if "900" not in body:
            failures.append(f"{path}: missing settled/default review scale")
        if "60-120" not in body:
            failures.append(f"{path}: missing very-short interval warning")
        if "null" not in body:
            failures.append(f"{path}: missing default-policy null guidance")

    assert failures == []


def test_load_prompt_component_supports_resolver_front_matter(tmp_path: Path) -> None:
    path = tmp_path / "test.dynamic.md"
    path.write_text(
        """---
id: test.dynamic
stage: constraints
kind: resolver
resolver_ref: builtin.test.dynamic
priority: 7
enabled: true
metadata:
  display_name: Dynamic Test
---

This body is documentation only.
""",
        encoding="utf-8",
    )

    component = load_prompt_component(path)

    assert component.kind == PromptComponentKind.RESOLVER
    assert component.resolver_ref == "builtin.test.dynamic"
    assert component.content == ""
    assert component.priority == 7
    assert component.metadata["display_name"] == "Dynamic Test"


def test_load_prompt_component_rejects_filename_id_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "wrong.name.md"
    path.write_text(
        """---
id: test.sample
stage: instructions
kind: static_text
---

Hello.
""",
        encoding="utf-8",
    )

    with pytest.raises(PromptFileError, match="name must be"):
        load_prompt_component(path)


def test_register_prompt_files_reads_source_by_default(tmp_path: Path) -> None:
    registry = PromptRegistry()
    data_root = tmp_path / "data-prompts"

    components = register_prompt_files(
        registry,
        package="shinbot.agent.runners.review_scan",
        prompt_ids=["review.review_scan.task"],
        locale="en-US",
        data_root=data_root,
    )

    runtime_path = data_root / "en-US" / "review.review_scan.task.md"
    assert not runtime_path.exists()
    assert components[0].id == "review.review_scan.task"
    assert registry.get_component("review.review_scan.task") is not None
    assert "Review the supplied unread messages" in components[0].content
    assert components[0].metadata["prompt_file"].endswith(
        "prompts/en-US/review.review_scan.task.md"
    )


def test_register_prompt_files_ignores_existing_runtime_copy_without_sync(tmp_path: Path) -> None:
    registry = PromptRegistry()
    runtime_dir = tmp_path / "data-prompts" / "en-US"
    runtime_dir.mkdir(parents=True)
    runtime_path = runtime_dir / "review.review_scan.task.md"
    runtime_path.write_text(
        """---
id: review.review_scan.task
stage: instructions
kind: static_text
priority: 100
enabled: true
---

User edited prompt.
""",
        encoding="utf-8",
    )

    components = register_prompt_files(
        registry,
        package="shinbot.agent.runners.review_scan",
        prompt_ids=["review.review_scan.task"],
        locale="en-US",
        data_root=tmp_path / "data-prompts",
    )

    assert "Review the supplied unread messages" in components[0].content
    assert components[0].metadata["prompt_file"] != str(runtime_path)


def test_register_prompt_files_syncs_runtime_copy_when_requested(tmp_path: Path) -> None:
    registry = PromptRegistry()
    data_root = tmp_path / "data-prompts"

    components = register_prompt_files(
        registry,
        package="shinbot.agent.runners.review_scan",
        prompt_ids=["review.review_scan.task"],
        locale="en-US",
        data_root=data_root,
        sync_to_data=True,
    )

    runtime_path = data_root / "en-US" / "review.review_scan.task.md"
    assert runtime_path.exists()
    assert components[0].metadata["prompt_file"] == str(runtime_path)
    manifest = registry.prompt_file_catalog.get(
        prompt_id="review.review_scan.task",
        locale="en-US",
    )
    assert manifest is not None
    assert manifest.prompt_id == "review.review_scan.task"
    assert manifest.runtime_path == runtime_path
    assert manifest.runtime_exists is True
    assert manifest.loaded_path == runtime_path
    assert manifest.loaded_from == "runtime"


def test_prompt_file_catalog_refreshes_manifest_file_status(tmp_path: Path) -> None:
    registry = PromptRegistry()
    data_root = tmp_path / "data-prompts"

    register_prompt_files(
        registry,
        package="shinbot.agent.runners.review_scan",
        prompt_ids=["review.review_scan.task"],
        locale="en-US",
        data_root=data_root,
        sync_to_data=True,
    )

    runtime_path = data_root / "en-US" / "review.review_scan.task.md"
    runtime_path.unlink()

    manifest = registry.prompt_file_catalog.get(
        prompt_id="review.review_scan.task",
        locale="en-US",
    )

    assert manifest is not None
    assert manifest.runtime_exists is False
    assert manifest.source_exists is True
    assert manifest.loaded_from == "source"
    assert manifest.loaded_path == manifest.source_path


def test_register_prompt_files_uses_existing_runtime_copy(tmp_path: Path) -> None:
    registry = PromptRegistry()
    runtime_dir = tmp_path / "data-prompts" / "en-US"
    runtime_dir.mkdir(parents=True)
    runtime_path = runtime_dir / "review.review_scan.task.md"
    runtime_path.write_text(
        """---
id: review.review_scan.task
stage: instructions
kind: static_text
priority: 100
enabled: true
---

User edited prompt.
""",
        encoding="utf-8",
    )

    components = register_prompt_files(
        registry,
        package="shinbot.agent.runners.review_scan",
        prompt_ids=["review.review_scan.task"],
        locale="en-US",
        data_root=tmp_path / "data-prompts",
        sync_to_data=True,
    )

    assert components[0].content == "User edited prompt."
    assert runtime_path.read_text(encoding="utf-8").endswith("User edited prompt.\n")


def test_register_prompt_files_uses_fallback_locale(tmp_path: Path) -> None:
    registry = PromptRegistry()

    components = register_prompt_files(
        registry,
        package="shinbot.agent.runners.review_scan",
        prompt_ids=["review.review_scan.task"],
        locale="ja-JP",
        fallback_locales=("en-US",),
        data_root=tmp_path / "data-prompts",
    )

    assert components[0].metadata["locale"] == "en-US"
    assert "Review the supplied unread messages" in components[0].content


def test_register_prompt_files_rejects_runtime_id_mismatch(tmp_path: Path) -> None:
    registry = PromptRegistry()
    runtime_dir = tmp_path / "data-prompts" / "en-US"
    runtime_dir.mkdir(parents=True)
    runtime_path = runtime_dir / "review.review_scan.task.md"
    runtime_path.write_text(
        """---
id: review.wrong.task
stage: instructions
kind: static_text
priority: 100
enabled: true
---

Bad id.
""",
        encoding="utf-8",
    )

    with pytest.raises(PromptFileError, match="expected 'review.review_scan.task'"):
        register_prompt_files(
            registry,
            package="shinbot.agent.runners.review_scan",
            prompt_ids=["review.review_scan.task"],
            locale="en-US",
            data_root=tmp_path / "data-prompts",
            sync_to_data=True,
        )


def test_load_prompt_component_rejects_bad_front_matter(tmp_path: Path) -> None:
    path = tmp_path / "bad.md"
    path.write_text("id: no-front-matter\n", encoding="utf-8")

    with pytest.raises(PromptFileError):
        load_prompt_component(path)
