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
    register_prompt_files,
)


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
