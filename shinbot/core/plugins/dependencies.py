"""Python dependency synchronization for user plugins."""

from __future__ import annotations

import asyncio
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

_SYNCED_DEPENDENCY_SETS: set[tuple[str, tuple[str, ...]]] = set()


@dataclass(slots=True)
class PluginDependencyError(RuntimeError):
    """Structured error raised while syncing plugin Python dependencies."""

    status_code: int
    code: str
    message: str

    def __str__(self) -> str:
        return self.message


async def sync_plugin_python_dependencies(plugin_id: str, plugin_root: Path) -> list[str]:
    """Install Python dependencies declared by a plugin pyproject.toml."""
    dependencies = plugin_python_dependencies(plugin_root)
    if not dependencies:
        return []
    cache_key = (str(plugin_root.resolve()), tuple(dependencies))
    if cache_key in _SYNCED_DEPENDENCY_SETS:
        return []
    stdout, stderr, returncode = await _run_dependency_installer(
        sys.executable,
        "-m",
        "pip",
        "install",
        *dependencies,
    )
    if returncode == 0:
        _SYNCED_DEPENDENCY_SETS.add(cache_key)
        return dependencies
    if "No module named pip" in _process_output(stdout, stderr):
        stdout, stderr, returncode = await _run_dependency_installer(
            "uv",
            "pip",
            "install",
            "--python",
            sys.executable,
            *dependencies,
        )
        if returncode == 0:
            _SYNCED_DEPENDENCY_SETS.add(cache_key)
            return dependencies
    detail = _process_output(stdout, stderr)
    raise PluginDependencyError(
        status_code=500,
        code="PLUGIN_INSTALL_DEPENDENCY_INSTALL_FAILED",
        message=f"Failed to install Python dependencies for plugin {plugin_id!r}: {detail}",
    )


def plugin_python_dependencies(plugin_root: Path) -> list[str]:
    """Return normalized project.dependencies from a plugin pyproject.toml."""
    pyproject_path = plugin_root / "pyproject.toml"
    if not pyproject_path.is_file():
        return []
    try:
        with pyproject_path.open("rb") as file_obj:
            payload = tomllib.load(file_obj)
    except tomllib.TOMLDecodeError as exc:
        raise PluginDependencyError(
            status_code=422,
            code="PLUGIN_INSTALL_PYPROJECT_INVALID",
            message=f"Invalid plugin pyproject.toml: {exc}",
        ) from exc
    project = payload.get("project")
    if not isinstance(project, dict):
        return []
    dependencies = project.get("dependencies", [])
    if dependencies is None:
        return []
    if not isinstance(dependencies, list) or not all(
        isinstance(item, str) for item in dependencies
    ):
        raise PluginDependencyError(
            status_code=422,
            code="PLUGIN_INSTALL_PYPROJECT_INVALID",
            message="plugin pyproject.toml project.dependencies must be a list of strings",
        )
    return [item.strip() for item in dependencies if item.strip()]


def _process_output(stdout: bytes, stderr: bytes) -> str:
    output = b"\n".join(part for part in (stdout, stderr) if part).decode(
        "utf-8",
        errors="replace",
    )
    stripped = output.strip()
    if not stripped:
        return "dependency installer exited without output"
    return stripped[-1000:]


async def _run_dependency_installer(*args: str) -> tuple[bytes, bytes, int]:
    try:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        return b"", str(exc).encode("utf-8", errors="replace"), 127
    stdout, stderr = await process.communicate()
    return stdout, stderr, process.returncode
