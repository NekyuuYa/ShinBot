from __future__ import annotations

import logging
from pathlib import Path

import pytest
from fastapi import HTTPException

from shinbot.api.routers import system as system_router
from shinbot.utils import logger as logger_utils


class _BootStub:
    def __init__(self, tmp_path: Path, *, save_ok: bool = True) -> None:
        self.config: dict[str, object] = {"logging": {"level": "INFO"}}
        self.data_dir = tmp_path
        self.save_ok = save_ok
        self.save_config_calls = 0

    def save_config(self) -> bool:
        self.save_config_calls += 1
        return self.save_ok


@pytest.fixture
def restore_logging_runtime():
    root = logging.getLogger()
    original_level = root.level
    original_policy = logger_utils.runtime_log_manager.third_party_noise_policy()
    try:
        yield
    finally:
        root.setLevel(original_level)
        logger_utils.runtime_log_manager.set_third_party_noise_policy(original_policy)


@pytest.mark.asyncio
async def test_system_logging_state_exposes_runtime_snapshot(restore_logging_runtime):
    response = await system_router.get_logging_state()

    payload = response["data"]
    assert "level" in payload
    assert "thirdPartyNoise" in payload
    assert "sources" in payload
    assert "handlers" in payload


@pytest.mark.asyncio
async def test_system_logging_update_applies_and_persists_runtime_config(
    tmp_path: Path,
    restore_logging_runtime,
):
    boot = _BootStub(tmp_path)
    body = system_router.UpdateLoggingRuntimeRequest(
        level="DEBUG",
        thirdPartyNoise="off",
        persist=True,
    )

    response = await system_router.update_logging_state(body, boot=boot)

    payload = response["data"]
    assert payload["level"] == "DEBUG"
    assert payload["thirdPartyNoise"] == "off"
    assert boot.config["logging"] == {
        "level": "DEBUG",
        "third_party_noise": "off",
    }
    assert boot.save_config_calls == 1


@pytest.mark.asyncio
async def test_system_logging_update_rejects_invalid_values(
    tmp_path: Path,
    restore_logging_runtime,
):
    body = system_router.UpdateLoggingRuntimeRequest(thirdPartyNoise="loud")

    with pytest.raises(HTTPException) as exc_info:
        await system_router.update_logging_state(body, boot=_BootStub(tmp_path))

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail["code"] == "INVALID_ACTION"


@pytest.mark.asyncio
async def test_system_logging_update_reports_persist_failure(
    tmp_path: Path,
    restore_logging_runtime,
):
    body = system_router.UpdateLoggingRuntimeRequest(thirdPartyNoise="debug", persist=True)

    with pytest.raises(HTTPException) as exc_info:
        await system_router.update_logging_state(body, boot=_BootStub(tmp_path, save_ok=False))

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail["code"] == "CONFIG_WRITE_FAILED"
