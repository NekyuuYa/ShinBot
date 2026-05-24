from __future__ import annotations

from typing import Any


def pytest_addoption(parser: Any) -> None:
    group = parser.getgroup("shinbot-e2e")
    group.addoption(
        "--e2e-scenario",
        action="append",
        default=[],
        metavar="PATTERN",
        help="Run platform-sim E2E scenarios whose name or fixture stem matches PATTERN.",
    )
    group.addoption(
        "--e2e-tag",
        action="append",
        default=[],
        metavar="TAG",
        help="Run platform-sim E2E scenarios containing TAG. Repeat for an AND filter.",
    )


def pytest_generate_tests(metafunc: Any) -> None:
    if "platform_scenario_path" not in metafunc.fixturenames:
        return

    from tests.e2e.platform_sim.scenarios import select_scenario_entries

    entries = select_scenario_entries(
        patterns=metafunc.config.getoption("--e2e-scenario") or [],
        tags=metafunc.config.getoption("--e2e-tag") or [],
    )
    metafunc.parametrize(
        "platform_scenario_path",
        [entry.path for entry in entries],
        ids=[entry.name for entry in entries],
    )
