"""Runtime process control for restart-oriented lifecycle actions."""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict, dataclass
from enum import IntEnum, StrEnum


class RestartReason(StrEnum):
    MANUAL = "manual"
    UPDATE = "update"


class ProcessExitCode(IntEnum):
    OK = 0
    RESTART_MANUAL = 20
    RESTART_UPDATE = 21


@dataclass(slots=True)
class RestartRequest:
    reason: RestartReason
    requested_at: int
    requested_by: str = ""
    source: str = ""

    def to_payload(self) -> dict[str, object]:
        return asdict(self)


class RuntimeControl:
    """Coordinate restart requests across API, CLI, and future command entrypoints."""

    def __init__(self) -> None:
        self._restart_event = asyncio.Event()
        self._restart_request: RestartRequest | None = None

    def request_restart(
        self,
        *,
        reason: RestartReason,
        requested_by: str = "",
        source: str = "",
    ) -> RestartRequest:
        if self._restart_request is not None:
            raise RuntimeError("A restart request is already pending")

        request = RestartRequest(
            reason=reason,
            requested_at=int(time.time()),
            requested_by=requested_by.strip(),
            source=source.strip(),
        )
        self._restart_request = request
        self._restart_event.set()
        return request

    @property
    def restart_request(self) -> RestartRequest | None:
        return self._restart_request

    @property
    def restart_requested(self) -> bool:
        return self._restart_request is not None

    async def wait_for_restart(self) -> RestartRequest:
        await self._restart_event.wait()
        assert self._restart_request is not None
        return self._restart_request

    def exit_code(self) -> int:
        request = self._restart_request
        if request is None:
            return int(ProcessExitCode.OK)
        if request.reason == RestartReason.UPDATE:
            return int(ProcessExitCode.RESTART_UPDATE)
        return int(ProcessExitCode.RESTART_MANUAL)

    def snapshot(self) -> dict[str, object] | None:
        request = self._restart_request
        if request is None:
            return None
        return request.to_payload()
