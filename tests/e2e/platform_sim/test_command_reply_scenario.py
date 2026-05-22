from pathlib import Path

import pytest

from tests.e2e.platform_sim.harness import load_scenario, run_platform_scenario

pytestmark = pytest.mark.e2e


async def test_command_reply_scenario(tmp_path: Path) -> None:
    scenario = load_scenario(Path(__file__).parent / "fixtures" / "command_reply.json")

    _bot, adapter = await run_platform_scenario(scenario, data_dir=tmp_path)

    assert adapter.started is True
    assert adapter.stopped is True
