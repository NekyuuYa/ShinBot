from __future__ import annotations

from pathlib import Path

import pytest

from scripts import test_audit
from scripts.test_audit import audit_file


def _audit_source(tmp_path: Path, source: str):
    path = tmp_path / "test_sample.py"
    path.write_text(source, encoding="utf-8")
    return audit_file(path)


def test_discover_test_files_audits_pytest_suffix_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix_path = tmp_path / "test_prefix.py"
    suffix_path = tmp_path / "suffix_test.py"
    overlap_path = tmp_path / "test_overlap_test.py"
    prefix_path.write_text("def test_prefix() -> None:\n    pass\n", encoding="utf-8")
    suffix_path.write_text(
        """import pytest

@pytest.mark.unknown_suffix_marker
def test_suffix() -> None:
    pass
""",
        encoding="utf-8",
    )
    overlap_path.write_text("def test_overlap() -> None:\n    pass\n", encoding="utf-8")
    monkeypatch.setattr(test_audit, "TESTS_DIR", tmp_path)

    files = test_audit.discover_test_files()
    audits = {path.name: test_audit.audit_file(path) for path in files}

    assert files == sorted([prefix_path, suffix_path, overlap_path])
    assert audits["suffix_test.py"].unknown_markers == [(3, "unknown_suffix_marker")]


def test_audit_reports_unknown_module_pytestmark(tmp_path: Path) -> None:
    audit = _audit_source(
        tmp_path,
        """import pytest

pytestmark = [pytest.mark.unit, pytest.mark.unknown_suite_marker]

def test_example() -> None:
    pass
""",
    )

    assert audit.explicit_layers == {"unit"}
    assert audit.unknown_markers == [(3, "unknown_suite_marker")]


def test_audit_requires_reasons_for_module_skip_and_xfail(tmp_path: Path) -> None:
    audit = _audit_source(
        tmp_path,
        """import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skip,
    pytest.mark.skipif(True),
    pytest.mark.xfail(),
]

def test_example() -> None:
    pass
""",
    )

    assert audit.missing_reasons == [
        (5, "skip"),
        (6, "skipif"),
        (7, "xfail"),
    ]


def test_audit_accepts_reasons_for_module_skip_and_xfail(tmp_path: Path) -> None:
    audit = _audit_source(
        tmp_path,
        """import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skip("platform unavailable"),
    pytest.mark.skipif(False, reason="supported platform"),
    pytest.mark.xfail(reason="known behavior"),
]

def test_example() -> None:
    pass
""",
    )

    assert audit.missing_reasons == []


def test_audit_does_not_treat_conditional_marker_args_as_reasons(tmp_path: Path) -> None:
    audit = _audit_source(
        tmp_path,
        """import pytest

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif("sys.platform == 'win32'"),
    pytest.mark.xfail("sys.version_info < (3, 12)"),
]

def test_example() -> None:
    pass
""",
    )

    assert audit.missing_reasons == [
        (5, "skipif"),
        (6, "xfail"),
    ]
