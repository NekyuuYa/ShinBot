"""Audit ShinBot's pytest suite for classification and maintainability."""

from __future__ import annotations

import argparse
import ast
import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = ROOT / "tests"
LAYER_MARKERS = {"unit", "api", "integration", "e2e"}
KNOWN_MARKERS = LAYER_MARKERS | {"slow", "asyncio", "parametrize", "skip", "skipif", "xfail"}
LARGE_FILE_LINE_LIMIT = 900

CRITICAL_AREAS = {
    "config providers": ("config_provider", "config_providers_api", "provider_config"),
    "boot lifecycle": ("boot", "boot_runtime"),
    "plugin lifecycle": ("plugin", "plugins_api"),
    "tool runtime": ("tool", "tools_api"),
    "message routing": ("routing", "ingress", "message_context"),
    "agent runtime": ("agent_runtime", "active_chat", "agent_review", "agent_scheduler"),
    "model runtime": ("model_runtime", "api/model_runtime"),
    "persona and prompts": ("persona", "prompt"),
}

@dataclass
class TestFileAudit:
    path: Path
    tests: list[str] = field(default_factory=list)
    markers: set[str] = field(default_factory=set)
    explicit_layers: set[str] = field(default_factory=set)
    directory_layer: str = ""
    inferred_layer: str = "unit"
    line_count: int = 0
    duplicate_tests: list[str] = field(default_factory=list)
    missing_reasons: list[tuple[int, str]] = field(default_factory=list)
    unknown_markers: list[tuple[int, str]] = field(default_factory=list)
    parse_error: str = ""


def classify_test_layer(path: Path) -> str:
    normalized = path.as_posix()
    name = path.name
    parts = set(path.parts)

    if "unit" in parts:
        return "unit"
    if "api" in parts:
        return "api"
    if "integration" in parts:
        return "integration"
    if "e2e" in parts:
        return "e2e"
    if name.endswith("_api.py") or "_api_" in name:
        return "api"
    if any(
        token in normalized
        for token in (
            "runtime",
            "boot",
            "plugin",
            "adapter",
            "ingress",
            "routing",
            "workflow",
            "media",
            "persistence",
            "operator_cli",
            "system_update",
        )
    ):
        return "integration"
    return "unit"


def directory_test_layer(path: Path) -> str:
    try:
        relative = path.relative_to(TESTS_DIR)
    except ValueError:
        return ""
    if not relative.parts:
        return ""
    first_part = relative.parts[0]
    return first_part if first_part in LAYER_MARKERS else ""


def dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    if isinstance(node, ast.Call):
        return dotted_name(node.func)
    return ""


def marker_name(node: ast.AST) -> str | None:
    name = dotted_name(node)
    marker_prefix = "pytest.mark."
    if name.startswith(marker_prefix):
        return name[len(marker_prefix) :].split(".", 1)[0]
    return None


def call_has_reason(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    for keyword in node.keywords:
        if keyword.arg == "reason" and literal_non_empty(keyword.value):
            return True
    return len(node.args) >= 1 and literal_non_empty(node.args[0])


def literal_non_empty(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and isinstance(node.value, str) and bool(node.value.strip())


def collect_pytestmark_markers(node: ast.Assign | ast.AnnAssign) -> set[str]:
    targets: list[ast.AST]
    if isinstance(node, ast.Assign):
        targets = list(node.targets)
        value = node.value
    else:
        targets = [node.target]
        value = node.value

    if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in targets):
        return set()
    if value is None:
        return set()

    candidates: list[ast.AST]
    if isinstance(value, (ast.List, ast.Tuple)):
        candidates = list(value.elts)
    else:
        candidates = [value]

    return {name for item in candidates if (name := marker_name(item))}


def audit_file(path: Path) -> TestFileAudit:
    audit = TestFileAudit(
        path=path,
        directory_layer=directory_test_layer(path),
        inferred_layer=classify_test_layer(path),
    )
    source = path.read_text(encoding="utf-8")
    audit.line_count = source.count("\n") + 1

    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        audit.parse_error = f"{exc.msg} at line {exc.lineno}"
        return audit

    def record_markers(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> None:
        for decorator in node.decorator_list:
            name = marker_name(decorator)
            if not name:
                continue
            audit.markers.add(name)
            if name not in KNOWN_MARKERS:
                audit.unknown_markers.append((decorator.lineno, name))
            if name in {"skip", "skipif", "xfail"} and not call_has_reason(decorator):
                audit.missing_reasons.append((decorator.lineno, name))

    test_name_counts: Counter[str] = Counter()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            audit.markers.update(collect_pytestmark_markers(node))

        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith(
            "test"
        ):
            audit.tests.append(node.name)
            test_name_counts[node.name] += 1
            record_markers(node)
            continue

        if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
            record_markers(node)
            for child in node.body:
                if not isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                if not child.name.startswith("test"):
                    continue
                qualified_name = f"{node.name}.{child.name}"
                audit.tests.append(qualified_name)
                test_name_counts[qualified_name] += 1
                record_markers(child)

    audit.explicit_layers = audit.markers & LAYER_MARKERS
    audit.duplicate_tests = sorted(name for name, count in test_name_counts.items() if count > 1)
    return audit


def discover_test_files() -> list[Path]:
    return sorted(TESTS_DIR.rglob("test_*.py"))


def build_summary(audits: list[TestFileAudit]) -> dict[str, Any]:
    layer_counts: Counter[str] = Counter()
    explicit_layer_files = 0
    inferred_layer_files = 0
    for audit in audits:
        layers = audit.explicit_layers or {audit.directory_layer or audit.inferred_layer}
        for layer in layers:
            layer_counts[layer] += len(audit.tests)
        if audit.explicit_layers:
            explicit_layer_files += 1
        else:
            inferred_layer_files += 1

    directory_layer_files = sum(
        1 for audit in audits if not audit.explicit_layers and audit.directory_layer
    )
    heuristic_layer_files = sum(
        1 for audit in audits if not audit.explicit_layers and not audit.directory_layer
    )

    return {
        "files": len(audits),
        "tests": sum(len(audit.tests) for audit in audits),
        "layers": dict(sorted(layer_counts.items())),
        "explicit_layer_files": explicit_layer_files,
        "directory_layer_files": directory_layer_files,
        "heuristic_layer_files": heuristic_layer_files,
        "inferred_layer_files": inferred_layer_files,
    }


def find_critical_area_gaps(audits: list[TestFileAudit]) -> list[str]:
    indexed = [audit.path.relative_to(ROOT).as_posix().lower() for audit in audits]
    missing: list[str] = []
    for area, tokens in CRITICAL_AREAS.items():
        if not any(any(token in path for token in tokens) for path in indexed):
            missing.append(area)
    return missing


def render_text_report(
    audits: list[TestFileAudit],
    *,
    errors: list[str],
    warnings: list[str],
) -> str:
    summary = build_summary(audits)
    lines = [
        "ShinBot test audit",
        "==================",
        f"Files: {summary['files']}",
        f"Test definitions: {summary['tests']}",
        f"Layer definition counts: {summary['layers']}",
        f"Files with explicit layer markers: {summary['explicit_layer_files']}",
        f"Files classified by layer directory: {summary['directory_layer_files']}",
        f"Files using heuristic layer inference: {summary['heuristic_layer_files']}",
        "",
    ]

    if errors:
        lines.append("Errors:")
        lines.extend(f"- {error}" for error in errors)
        lines.append("")

    if warnings:
        lines.append("Warnings:")
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")

    if not errors and not warnings:
        lines.append("No audit issues found.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args(argv)

    files = discover_test_files()
    audits = [audit_file(path) for path in files]
    errors: list[str] = []
    warnings: list[str] = []

    if not files:
        errors.append("no test files found")

    for audit in audits:
        rel = audit.path.relative_to(ROOT).as_posix()
        test_rel = audit.path.relative_to(TESTS_DIR)
        if audit.parse_error:
            errors.append(f"{rel}: parse error: {audit.parse_error}")
            continue
        if test_rel.parent == Path("."):
            errors.append(
                f"{rel}: test files must live under tests/unit, tests/api, "
                "tests/integration, or tests/e2e"
            )
        if not audit.tests:
            errors.append(f"{rel}: no test functions or classes discovered")
        if len(audit.explicit_layers) > 1:
            errors.append(f"{rel}: multiple layer markers declared: {sorted(audit.explicit_layers)}")
        if (
            audit.explicit_layers
            and audit.directory_layer
            and audit.directory_layer not in audit.explicit_layers
        ):
            errors.append(
                f"{rel}: explicit layer {sorted(audit.explicit_layers)} conflicts "
                f"with directory layer {audit.directory_layer!r}"
            )
        for line, marker in audit.missing_reasons:
            errors.append(f"{rel}:{line}: pytest.mark.{marker} requires a reason")
        for line, marker in audit.unknown_markers:
            errors.append(f"{rel}:{line}: unknown pytest marker {marker!r}")
        for name in audit.duplicate_tests:
            errors.append(f"{rel}: duplicate test name {name!r}")
        if not audit.explicit_layers and not audit.directory_layer:
            warnings.append(f"{rel}: using inferred layer marker {audit.inferred_layer!r}")
        if audit.line_count > LARGE_FILE_LINE_LIMIT:
            warnings.append(
                f"{rel}: {audit.line_count} lines; consider splitting for reviewability"
            )

    for area in find_critical_area_gaps(audits):
        errors.append(f"critical area has no matching tests: {area}")

    if args.json:
        payload = {
            "summary": build_summary(audits),
            "errors": errors,
            "warnings": warnings,
            "files": [
                {
                    "path": audit.path.relative_to(ROOT).as_posix(),
                    "tests": len(audit.tests),
                    "markers": sorted(audit.markers),
                    "explicit_layers": sorted(audit.explicit_layers),
                    "directory_layer": audit.directory_layer,
                    "inferred_layer": audit.inferred_layer,
                    "line_count": audit.line_count,
                }
                for audit in audits
            ],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(render_text_report(audits, errors=errors, warnings=warnings))

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
