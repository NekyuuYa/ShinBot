"""Agent runtime profile configuration API."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import tomli_w
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shinbot.admin.persona_files import PersonaFileRepository
from shinbot.agent.runtime.config import validate_agent_runtime_config_references
from shinbot.agent.runtime.config_provider import AGENT_RUNTIME_CONFIG_PROVIDER_ID
from shinbot.api.deps import AuthRequired, BootDep, BotDep
from shinbot.api.models import EC, Envelope, ok
from shinbot.core.config_provider import ConfigProviderKind, ConfigValidationIssue

router = APIRouter(
    prefix="/agent-configs",
    tags=["agent-configs"],
    dependencies=AuthRequired,
)

_PROFILE_FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*\.toml$")


class SaveAgentConfigRequest(BaseModel):
    fileName: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    validateBeforeSave: bool = True


class AgentConfigIssue(BaseModel):
    """Validation issue attached to an agent config profile."""

    path: str = ""
    message: str = ""
    code: str = ""


class AgentConfigProfile(BaseModel):
    """An agent configuration profile loaded from a TOML file."""

    fileName: str = ""
    path: str = ""
    agentId: str = ""
    mode: str = ""
    personaId: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    lastModified: int = 0
    issues: list[AgentConfigIssue] = Field(default_factory=list)


class AgentConfigDeleteResult(BaseModel):
    """Result returned after successfully deleting an agent config."""

    deleted: bool = True
    fileName: str = ""


def _agents_dir(boot: Any) -> Path:
    return Path(boot.data_dir) / "agents"


def _issue_payloads(issues: list[ConfigValidationIssue]) -> list[dict[str, str]]:
    return [issue.to_dict() for issue in issues]


def _toml_decode_issue(message: str) -> ConfigValidationIssue:
    return ConfigValidationIssue(
        path="",
        message=f"invalid TOML: {message}",
        code="toml",
    )


def _agent_section(config: dict[str, Any]) -> dict[str, Any]:
    agent = config.get("agent")
    return agent if isinstance(agent, dict) else {}


def _profile_payload(path: Path, config: dict[str, Any], issues: list[ConfigValidationIssue]) -> dict[str, Any]:
    agent = _agent_section(config)
    return {
        "fileName": path.name,
        "path": f"agents/{path.name}",
        "agentId": str(agent.get("id") or ""),
        "mode": str(agent.get("mode") or ""),
        "personaId": str(agent.get("persona_id") or ""),
        "config": config,
        "lastModified": int(path.stat().st_mtime * 1000) if path.exists() else 0,
        "issues": _issue_payloads(issues),
    }


def _load_profile(path: Path, bot: Any) -> dict[str, Any]:
    try:
        with path.open("rb") as file_obj:
            config = tomllib.load(file_obj)
    except tomllib.TOMLDecodeError as exc:
        return _profile_payload(path, {}, [_toml_decode_issue(str(exc))])

    issues = _validate_profile_config(config, bot)
    return _profile_payload(path, config, issues)


def _validate_profile_config(config: dict[str, Any], bot: Any) -> list[ConfigValidationIssue]:
    issues = list(
        bot.config_provider_registry.validate(
            ConfigProviderKind.AGENT,
            AGENT_RUNTIME_CONFIG_PROVIDER_ID,
            config,
            strict=True,
        )
    )
    issues.extend(
        validate_agent_runtime_config_references(
            config,
            model_registry=bot.database.model_registry if bot.database is not None else None,
            persona_repository=PersonaFileRepository.from_data_dir(bot.data_dir),
        )
    )
    return issues


def _derive_file_name(config: dict[str, Any]) -> str:
    agent_id = str(_agent_section(config).get("id") or "").strip()
    if not agent_id:
        return ""
    safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", agent_id).strip(".-")
    return f"{safe_name}.toml" if safe_name else ""


def _normalize_file_name(file_name: str, config: dict[str, Any] | None = None) -> str:
    candidate = file_name.strip() or (_derive_file_name(config or {}) if config is not None else "")
    if candidate and not candidate.endswith(".toml"):
        candidate = f"{candidate}.toml"
    if not candidate or not _PROFILE_FILE_RE.fullmatch(candidate) or Path(candidate).name != candidate:
        raise HTTPException(
            status_code=422,
            detail={
                "code": EC.CONFIG_VALIDATION_FAILED,
                "message": "Agent profile file name must be a safe .toml file name",
                "issues": [
                    {
                        "path": "fileName",
                        "message": "must be a safe .toml file name",
                        "code": "filename",
                    }
                ],
            },
        )
    return candidate


def _profile_path(boot: Any, file_name: str) -> Path:
    return _agents_dir(boot) / _normalize_file_name(file_name)


def _write_profile(path: Path, config: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file_obj:
        tomli_w.dump(config, file_obj)


@router.get("", response_model=Envelope[list[AgentConfigProfile]])
async def list_agent_configs(bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """List all agent configuration profiles from the agents directory."""
    directory = _agents_dir(boot)
    if not directory.is_dir():
        return ok([])
    profiles = [
        _load_profile(path, bot)
        for path in sorted(directory.glob("*.toml"), key=lambda item: item.name)
        if path.is_file()
    ]
    return ok(profiles)


@router.get("/{file_name}", response_model=Envelope[AgentConfigProfile])
async def get_agent_config(file_name: str, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Retrieve a single agent configuration profile by file name."""
    path = _profile_path(boot, file_name)
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.AGENT_NOT_FOUND,
                "message": f"Agent profile {path.name!r} was not found",
            },
        )
    return ok(_load_profile(path, bot))


@router.post("", response_model=Envelope[AgentConfigProfile])
async def create_agent_config(body: SaveAgentConfigRequest, bot: Any = BotDep, boot: Any = BootDep) -> dict[str, Any]:
    """Create a new agent configuration profile from a TOML config."""
    file_name = _normalize_file_name(body.fileName, body.config)
    path = _agents_dir(boot) / file_name
    if path.exists():
        raise HTTPException(
            status_code=409,
            detail={
                "code": EC.AGENT_ALREADY_EXISTS,
                "message": f"Agent profile {file_name!r} already exists",
            },
        )

    issues = _validate_profile_config(body.config, bot)
    if body.validateBeforeSave and issues:
        raise HTTPException(
            status_code=422,
            detail={
                "code": EC.CONFIG_VALIDATION_FAILED,
                "message": "Agent profile validation failed",
                "issues": _issue_payloads(issues),
            },
        )

    _write_profile(path, body.config)
    return ok(_load_profile(path, bot))


@router.put("/{file_name}", response_model=Envelope[AgentConfigProfile])
async def update_agent_config(
    file_name: str,
    body: SaveAgentConfigRequest,
    bot: Any = BotDep,
    boot: Any = BootDep,
) -> dict[str, Any]:
    """Update an existing agent configuration profile."""
    path = _profile_path(boot, file_name)
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.AGENT_NOT_FOUND,
                "message": f"Agent profile {path.name!r} was not found",
            },
        )

    issues = _validate_profile_config(body.config, bot)
    if body.validateBeforeSave and issues:
        raise HTTPException(
            status_code=422,
            detail={
                "code": EC.CONFIG_VALIDATION_FAILED,
                "message": "Agent profile validation failed",
                "issues": _issue_payloads(issues),
            },
        )

    _write_profile(path, body.config)
    return ok(_load_profile(path, bot))


@router.delete("/{file_name}", response_model=Envelope[AgentConfigDeleteResult])
async def delete_agent_config(file_name: str, boot: Any = BootDep) -> dict[str, Any]:
    """Delete an agent configuration profile by file name."""
    path = _profile_path(boot, file_name)
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail={
                "code": EC.AGENT_NOT_FOUND,
                "message": f"Agent profile {path.name!r} was not found",
            },
        )
    path.unlink()
    return ok({"deleted": True, "fileName": path.name})
