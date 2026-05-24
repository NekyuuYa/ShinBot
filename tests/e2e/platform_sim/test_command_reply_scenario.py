from pathlib import Path

import pytest

from tests.e2e.platform_sim.harness import load_scenario, run_platform_scenario

pytestmark = pytest.mark.e2e


async def test_platform_sim_scenario(tmp_path: Path, platform_scenario_path: Path) -> None:
    scenario = load_scenario(platform_scenario_path)

    _bot, adapter = await run_platform_scenario(scenario, data_dir=tmp_path)

    assert adapter.started is True
    assert adapter.stopped is True
