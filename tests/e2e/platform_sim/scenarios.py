"""Scenario manifest helpers for platform simulation E2E tests."""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PLATFORM_SIM_DIR = Path(__file__).parent
FIXTURES_DIR = PLATFORM_SIM_DIR / "fixtures"
MANIFEST_PATH = PLATFORM_SIM_DIR / "manifest.json"


@dataclass(frozen=True, slots=True)
class PlatformScenarioEntry:
    name: str
    path: Path
    area: str
    tags: tuple[str, ...]
    purpose: str

    @property
    def fixture_stem(self) -> str:
        return self.path.stem


def load_scenario_entries() -> list[PlatformScenarioEntry]:
    payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if payload.get("version") != 1:
        raise ValueError("platform-sim manifest version must be 1")
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        raise ValueError("platform-sim manifest scenarios must be a list")

    entries: list[PlatformScenarioEntry] = []
    seen_names: set[str] = set()
    for index, raw_entry in enumerate(scenarios):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"platform-sim manifest scenarios[{index}] must be an object")
        entry = _parse_entry(raw_entry, index=index)
        if entry.name in seen_names:
            raise ValueError(f"duplicate platform-sim scenario name: {entry.name}")
        seen_names.add(entry.name)
        entries.append(entry)
    return entries


def select_scenario_entries(
    *,
    patterns: list[str] | tuple[str, ...] = (),
    tags: list[str] | tuple[str, ...] = (),
) -> list[PlatformScenarioEntry]:
    entries = load_scenario_entries()
    normalized_patterns = _split_filters(patterns)
    normalized_tags = set(_split_filters(tags))

    selected: list[PlatformScenarioEntry] = []
    for entry in entries:
        if normalized_patterns and not any(_matches_pattern(entry, item) for item in normalized_patterns):
            continue
        if normalized_tags and not normalized_tags.issubset(set(entry.tags)):
            continue
        selected.append(entry)

    if not selected:
        filters = []
        if normalized_patterns:
            filters.append(f"patterns={normalized_patterns}")
        if normalized_tags:
            filters.append(f"tags={sorted(normalized_tags)}")
        raise ValueError(f"no platform-sim E2E scenarios selected ({', '.join(filters)})")
    return selected


def fixture_paths_from_manifest() -> set[Path]:
    return {entry.path for entry in load_scenario_entries()}


def discover_fixture_paths() -> set[Path]:
    return set(FIXTURES_DIR.glob("*.json"))


def _parse_entry(raw_entry: dict[str, Any], *, index: int) -> PlatformScenarioEntry:
    name = _required_string(raw_entry, "name", index=index)
    path_text = _required_string(raw_entry, "path", index=index)
    area = _required_string(raw_entry, "area", index=index)
    purpose = _required_string(raw_entry, "purpose", index=index)
    raw_tags = raw_entry.get("tags")
    if not isinstance(raw_tags, list) or not raw_tags:
        raise ValueError(f"platform-sim manifest scenarios[{index}].tags must be a non-empty list")
    tags = tuple(_normalize_tag(tag, index=index) for tag in raw_tags)
    if len(tags) != len(set(tags)):
        raise ValueError(f"platform-sim manifest scenarios[{index}].tags contains duplicates")

    path = (PLATFORM_SIM_DIR / path_text).resolve()
    if not path.is_relative_to(PLATFORM_SIM_DIR.resolve()):
        raise ValueError(f"platform-sim scenario {name!r} path escapes platform_sim")
    if not path.is_file():
        raise ValueError(f"platform-sim scenario {name!r} fixture does not exist: {path_text}")
    if path.stem != name:
        raise ValueError(
            f"platform-sim scenario {name!r} must use a fixture with the same stem"
        )
    return PlatformScenarioEntry(
        name=name,
        path=path,
        area=area,
        tags=tags,
        purpose=purpose,
    )


def _required_string(raw_entry: dict[str, Any], key: str, *, index: int) -> str:
    value = raw_entry.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"platform-sim manifest scenarios[{index}].{key} must be a string")
    return value.strip()


def _normalize_tag(value: Any, *, index: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"platform-sim manifest scenarios[{index}].tags values must be strings")
    return value.strip()


def _split_filters(values: list[str] | tuple[str, ...]) -> list[str]:
    filters: list[str] = []
    for value in values:
        filters.extend(item.strip() for item in value.split(",") if item.strip())
    return filters


def _matches_pattern(entry: PlatformScenarioEntry, pattern: str) -> bool:
    return (
        fnmatch.fnmatch(entry.name, pattern)
        or fnmatch.fnmatch(entry.fixture_stem, pattern)
        or fnmatch.fnmatch(entry.path.name, pattern)
    )
