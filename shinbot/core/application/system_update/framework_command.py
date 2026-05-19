"""Configurable framework update command service."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from shinbot.core.application.runtime_control import RestartReason, RuntimeControl

from .common import DEFAULT_FRAMEWORK_UPDATE_TIMEOUT_SECONDS, MAX_OUTPUT_CHARS, SystemUpdateError


class FrameworkUpdateCommandService:
    """Run an operator-configured framework update command and request restart."""

    def __init__(
        self,
        *,
        config: dict[str, Any],
        config_path: Path | str | None = None,
    ) -> None:
        admin_cfg = config.get("admin", {})
        self._config_path = Path(config_path).resolve() if config_path is not None else None
        self._command = str(admin_cfg.get("framework_update_command", "")).strip()
        self._workdir = self._resolve_workdir(admin_cfg.get("framework_update_dir"))
        self._timeout = float(
            admin_cfg.get(
                "framework_update_timeout_seconds",
                DEFAULT_FRAMEWORK_UPDATE_TIMEOUT_SECONDS,
            )
        )
        self._restart_after_success = bool(admin_cfg.get("framework_update_restart", True))
        self._lock = asyncio.Lock()

    @property
    def update_in_progress(self) -> bool:
        return self._lock.locked()

    async def inspect(self) -> dict[str, Any]:
        return self._inspect(ignore_lock=False)

    async def run_and_request_restart(
        self,
        *,
        runtime_control: RuntimeControl,
        requested_by: str = "",
    ) -> dict[str, Any]:
        if self._lock.locked():
            raise SystemUpdateError(
                code="UPDATE_ALREADY_RUNNING",
                message="Another framework update command is already running",
                status_code=409,
            )

        async with self._lock:
            status = self._inspect(ignore_lock=True)
            if not status["canUpdate"]:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=status["blockMessage"] or "Framework update command is not available",
                    status_code=self._status_code_for_block(status["blockCode"]),
                )

            process = await asyncio.create_subprocess_shell(
                self._command,
                cwd=self._workdir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )

            try:
                stdout, _ = await asyncio.wait_for(process.communicate(), timeout=self._timeout)
            except TimeoutError as exc:
                process.kill()
                await process.communicate()
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Framework update command timed out",
                    status_code=500,
                ) from exc

            output = _trim_output(stdout.decode(errors="replace"))
            if process.returncode != 0:
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Framework update command failed",
                    status_code=500,
                    output=output,
                )

            restart_request = None
            if self._restart_after_success:
                if runtime_control.restart_requested:
                    restart_request = runtime_control.snapshot()
                else:
                    request = runtime_control.request_restart(
                        reason=RestartReason.UPDATE,
                        requested_by=requested_by.strip(),
                        source="operator.framework_update",
                    )
                    restart_request = request.to_payload()

            return {
                "accepted": True,
                "updated": True,
                "restartRequested": restart_request is not None,
                "restartRequest": restart_request,
                "workdir": str(self._workdir),
                "command": self._command,
                "output": output,
            }

    def _inspect(self, *, ignore_lock: bool) -> dict[str, Any]:
        payload = {
            "enabled": bool(self._command),
            "workdir": str(self._workdir),
            "command": self._command,
            "restartAfterSuccess": self._restart_after_success,
            "canUpdate": False,
            "blockCode": None,
            "blockMessage": None,
            "updateInProgress": self.update_in_progress,
        }

        if not self._command:
            payload["blockCode"] = "not_configured"
            payload["blockMessage"] = "Framework update command is not configured"
            return payload

        if not self._workdir.exists():
            payload["blockCode"] = "workdir_unavailable"
            payload["blockMessage"] = "Framework update working directory is unavailable"
            return payload

        if not self._workdir.is_dir():
            payload["blockCode"] = "workdir_invalid"
            payload["blockMessage"] = "Framework update working directory is not a directory"
            return payload

        if self._lock.locked() and not ignore_lock:
            payload["blockCode"] = "update_in_progress"
            payload["blockMessage"] = "Another framework update command is already running"
            return payload

        payload["canUpdate"] = True
        return payload

    def _resolve_workdir(self, raw: Any) -> Path:
        if isinstance(raw, str) and raw.strip():
            path = Path(raw.strip())
            if not path.is_absolute() and self._config_path is not None:
                path = self._config_path.parent / path
            return path.resolve()
        if self._config_path is not None:
            return self._config_path.parent.resolve()
        return Path.cwd().resolve()

    @staticmethod
    def _status_code_for_block(block_code: Any) -> int:
        if block_code in {"workdir_unavailable", "workdir_invalid"}:
            return 503
        return 409


def _trim_output(output: str) -> str:
    if len(output) <= MAX_OUTPUT_CHARS:
        return output
    omitted = len(output) - MAX_OUTPUT_CHARS
    return f"... omitted {omitted} chars ...\n{output[-MAX_OUTPUT_CHARS:]}"
