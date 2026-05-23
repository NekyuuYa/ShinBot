from pathlib import Path

import pytest

from tests.e2e.platform_sim.fixture_schema import (
    ScenarioValidationError,
    validate_scenario,
)
from tests.e2e.platform_sim.harness import load_scenario

pytestmark = pytest.mark.e2e

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _minimal_scenario() -> dict:
    return {
        "name": "schema_probe",
        "steps": [
            {
                "type": "message",
                "session": {"type": "private"},
                "sender": {"id": "user-1"},
                "content": "/ping",
            }
        ],
        "expect": {
            "sent": [],
        },
    }


@pytest.mark.parametrize(
    "scenario_path",
    sorted(FIXTURES_DIR.glob("*.json")),
    ids=lambda path: path.stem,
)
def test_platform_sim_fixtures_match_schema(scenario_path: Path) -> None:
    load_scenario(scenario_path)


def test_fixture_schema_rejects_unknown_top_level_key() -> None:
    scenario = _minimal_scenario()
    scenario["typo"] = True

    with pytest.raises(ScenarioValidationError, match=r"\$ has unsupported key"):
        validate_scenario(scenario, source="inline")


def test_fixture_schema_rejects_unknown_expect_key() -> None:
    scenario = _minimal_scenario()
    scenario["expect"]["messageLog"] = {}

    with pytest.raises(ScenarioValidationError, match=r"\$\.expect has unsupported key"):
        validate_scenario(scenario, source="inline")


def test_fixture_schema_rejects_invalid_command_kind() -> None:
    scenario = _minimal_scenario()
    scenario["commands"] = [{"name": "ping", "kind": "modle"}]

    with pytest.raises(ScenarioValidationError, match=r"\$\.commands\[0\]\.kind"):
        validate_scenario(scenario, source="inline")


def test_fixture_schema_rejects_string_booleans() -> None:
    scenario = _minimal_scenario()
    scenario["expect"]["agentEntrySignals"] = [
        {
            "sessionId": "sim-main:private:user-1",
            "isPrivate": "false",
        }
    ]

    with pytest.raises(ScenarioValidationError, match=r"\$\.expect\.agentEntrySignals"):
        validate_scenario(scenario, source="inline")


def test_fixture_schema_allows_supported_provider_config_fields() -> None:
    scenario = _minimal_scenario()
    scenario["modelRuntime"] = {
        "providers": [
            {
                "id": "stub-provider",
                "type": "openai",
                "auth": {"apiKey": "test"},
                "defaultParams": {"temperature": 0},
            }
        ],
        "models": [
            {
                "id": "stub-provider/stub-model",
                "providerId": "stub-provider",
            }
        ],
    }

    validate_scenario(scenario, source="inline")
