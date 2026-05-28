"""Tests for the main configuration reference file."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG_EXAMPLE = ROOT / "config.example.toml"
CONFIG_REFERENCE = ROOT / "config.reference.toml"


def _section_names(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return {
        match.group("name").strip()
        for match in re.finditer(r"^\s*\[{1,2}(?P<name>[^\]]+)\]{1,2}", text, re.MULTILINE)
    }


def test_config_schema_is_valid_toml() -> None:
    """Validate that the reference file remains valid TOML."""
    with CONFIG_REFERENCE.open("rb") as file_obj:
        tomllib.load(file_obj)


def test_config_schema_covers_example() -> None:
    """Ensure every section used by the example appears in the reference file."""
    example_sections = _section_names(CONFIG_EXAMPLE)
    reference_sections = _section_names(CONFIG_REFERENCE)

    missing = example_sections - reference_sections

    assert not missing
