"""Dashboard local build service."""

from __future__ import annotations

import asyncio
import shlex
import shutil
from pathlib import Path
from typing import Any

from shinbot.core.application.paths import resolve_project_path

from .common import DEFAULT_DASHBOARD_BUILD_TIMEOUT_SECONDS, MAX_OUTPUT_CHARS, SystemUpdateError


class DashboardBuildService:
    """Build the local Dashboard source tree into its dist directory."""

    def __init__(
        self,
        *,
        config: dict[str, Any],
        config_path: Path | str | None = None,
        dashboard_dir: Path | str | None = None,
    ) -> None:
        admin_cfg = config.get("admin", {})
        self._config_path = Path(config_path).resolve() if config_path is not None else None
        self._dashboard_dir = self._resolve_dashboard_dir(
            configured_dir=admin_cfg.get("dashboard_build_dir"),
            override_dir=dashboard_dir,
        )
        self._command = str(admin_cfg.get("dashboard_build_command", "pnpm build")).strip()
        self._timeout = float(
            admin_cfg.get("dashboard_build_timeout_seconds", DEFAULT_DASHBOARD_BUILD_TIMEOUT_SECONDS)
        )
        self._lock = asyncio.Lock()

    @property
    def build_in_progress(self) -> bool:
        """Return ``True`` while a Dashboard build operation is running."""
        return self._lock.locked()

    async def inspect(self) -> dict[str, Any]:
        """Inspect the Dashboard build environment and return a status payload.

        Returns:
            A dictionary describing the dashboard source directory, build
            command, and whether a build can be started.
        """
        return self._inspect(ignore_lock=False)

    async def build(self) -> dict[str, Any]:
        """Run the configured Dashboard build command.

        Returns:
            A dictionary describing the outcome of the build, including
            the command output.

        Raises:
            SystemUpdateError: If a build is already running, prerequisites
                are not met, or the build command fails.
        """
        if self._lock.locked():
            raise SystemUpdateError(
                code="UPDATE_ALREADY_RUNNING",
                message="Another Dashboard build is already running",
                status_code=409,
            )

        async with self._lock:
            status = self._inspect(ignore_lock=True)
            if not status["canBuild"]:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=status["blockMessage"] or "Dashboard build is not available",
                    status_code=self._status_code_for_block(status["blockCode"]),
                )

            args = shlex.split(self._command)
            try:
                process = await asyncio.create_subprocess_exec(
                    *args,
                    cwd=self._dashboard_dir,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
            except FileNotFoundError as exc:
                raise SystemUpdateError(
                    code="UPDATE_NOT_ALLOWED",
                    message=f"Dashboard build executable is unavailable: {args[0]}",
                    status_code=503,
                ) from exc

            try:
                stdout, _ = await asyncio.wait_for(process.communicate(), timeout=self._timeout)
            except TimeoutError as exc:
                process.kill()
                await process.communicate()
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Dashboard build timed out",
                    status_code=500,
                ) from exc

            output = _trim_output(stdout.decode(errors="replace"))
            if process.returncode != 0:
                raise SystemUpdateError(
                    code="UPDATE_FAILED",
                    message="Dashboard build command failed",
                    status_code=500,
                    output=output,
                )

            return {
                "accepted": True,
                "built": True,
                "dashboardPath": str(self._dashboard_dir),
                "distPath": str(self._dashboard_dir / "dist"),
                "command": self._command,
                "output": output,
            }

    def _inspect(self, *, ignore_lock: bool) -> dict[str, Any]:
        args = shlex.split(self._command) if self._command else []
        payload = {
            "enabled": True,
            "dashboardPath": str(self._dashboard_dir),
            "distPath": str(self._dashboard_dir / "dist"),
            "command": self._command,
            "canBuild": False,
            "blockCode": None,
            "blockMessage": None,
            "buildInProgress": self.build_in_progress,
        }

        if not self._dashboard_dir.exists():
            payload["blockCode"] = "dashboard_unavailable"
            payload["blockMessage"] = "Dashboard source directory is unavailable"
            return payload

        if not (self._dashboard_dir / "package.json").is_file():
            payload["blockCode"] = "package_json_missing"
            payload["blockMessage"] = "Dashboard package.json is missing"
            return payload

        if not args:
            payload["blockCode"] = "build_command_missing"
            payload["blockMessage"] = "Dashboard build command is empty"
            return payload

        if shutil.which(args[0]) is None:
            payload["blockCode"] = "build_tool_unavailable"
            payload["blockMessage"] = f"Dashboard build executable is unavailable: {args[0]}"
            return payload

        if self._lock.locked() and not ignore_lock:
            payload["blockCode"] = "build_in_progress"
            payload["blockMessage"] = "Another Dashboard build is already running"
            return payload

        payload["canBuild"] = True
        return payload

    def _resolve_dashboard_dir(
        self,
        *,
        configured_dir: Any,
        override_dir: Path | str | None,
    ) -> Path:
        if override_dir is not None:
            return Path(override_dir).resolve()

        if isinstance(configured_dir, str) and configured_dir.strip():
            path = Path(configured_dir.strip())
            if not path.is_absolute() and self._config_path is not None:
                path = resolve_project_path(path, config_path=self._config_path)
            return path.resolve()

        if self._config_path is not None:
            return resolve_project_path("dashboard", config_path=self._config_path)
        return (Path.cwd() / "dashboard").resolve()

    @staticmethod
    def _status_code_for_block(block_code: Any) -> int:
        if block_code in {
            "dashboard_unavailable",
            "build_tool_unavailable",
        }:
            return 503
        return 409


def _trim_output(output: str) -> str:
    if len(output) <= MAX_OUTPUT_CHARS:
        return output
    omitted = len(output) - MAX_OUTPUT_CHARS
    return f"... omitted {omitted} chars ...\n{output[-MAX_OUTPUT_CHARS:]}"
