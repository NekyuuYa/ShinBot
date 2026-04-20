"""Builtin plugin: web search tool powered by Tavily."""

from __future__ import annotations

import os
import sys
import tomllib
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field, ValidationError

from shinbot.agent.tools import ToolVisibility
from shinbot.core.plugins.context import Plugin

_TAVILY_SEARCH_URL = "https://api.tavily.com/search"


class SearchPluginConfig(BaseModel):
    tavily_api_key: str = Field(
        default="",
        description="Tavily API key",
        json_schema_extra={"ui_group": "credentials"},
    )
    timeout_seconds: float = Field(
        default=15.0,
        ge=1.0,
        le=60.0,
        description="HTTP timeout in seconds",
    )
    default_max_results: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Default number of search results",
    )
    default_search_depth: Literal["basic", "advanced"] = Field(
        default="basic",
        description="Tavily search depth",
    )
    include_answer: bool = Field(
        default=False,
        description="Include Tavily-generated answer in the tool output",
    )
    include_raw_content: bool = Field(
        default=False,
        description="Include raw_content from each result when available",
    )


__plugin_config_class__ = SearchPluginConfig


def _resolve_config_path(argv: Sequence[str] | None = None) -> Path:
    args = list(sys.argv[1:] if argv is None else argv)
    for index, value in enumerate(args):
        if value == "--config" and index + 1 < len(args):
            return Path(args[index + 1])
        if value.startswith("--config="):
            return Path(value.split("=", 1)[1])
    return Path("config.toml")


def _load_plugin_config(
    plugin_id: str,
    *,
    config_path: Path | None = None,
    argv: Sequence[str] | None = None,
) -> SearchPluginConfig:
    path = config_path or _resolve_config_path(argv)
    raw: dict[str, Any] = {}

    try:
        if path.exists():
            with path.open("rb") as file_obj:
                payload = tomllib.load(file_obj)
            plugin_configs = payload.get("plugin_configs", {})
            if isinstance(plugin_configs, dict):
                plugin_block = plugin_configs.get(plugin_id, {})
                if isinstance(plugin_block, dict):
                    raw = plugin_block
    except Exception:
        raw = {}

    try:
        return SearchPluginConfig.model_validate(raw)
    except ValidationError:
        return SearchPluginConfig()


def _coerce_max_results(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 1), 10)


def _coerce_search_depth(value: Any, default: str) -> str:
    normalized = str(value).strip().lower() if value is not None else ""
    if normalized in {"basic", "advanced"}:
        return normalized
    return default


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _to_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_tavily_results(
    items: Any,
    *,
    max_results: int,
    include_raw_content: bool,
) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []

    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        entry: dict[str, Any] = {
            "title": str(item.get("title", "") or ""),
            "url": str(item.get("url", "") or ""),
            "content": str(item.get("content", "") or ""),
        }
        score = _to_float(item.get("score"))
        if score is not None:
            entry["score"] = score

        if include_raw_content:
            raw_content = item.get("raw_content")
            if isinstance(raw_content, str):
                entry["raw_content"] = raw_content

        normalized.append(entry)
        if len(normalized) >= max_results:
            break

    return normalized


async def _tavily_search(
    *,
    api_key: str,
    query: str,
    max_results: int,
    timeout_seconds: float,
    search_depth: str,
    include_answer: bool,
    include_raw_content: bool,
) -> dict[str, Any]:
    payload = {
        "query": query,
        "max_results": max_results,
        "search_depth": search_depth,
        "include_answer": include_answer,
        "include_raw_content": include_raw_content,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(
                _TAVILY_SEARCH_URL,
                headers=headers,
                json=payload,
            )
    except httpx.TimeoutException as exc:
        raise RuntimeError("Tavily request timed out") from exc
    except httpx.HTTPError as exc:
        raise RuntimeError(f"Tavily request failed: {exc}") from exc

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        body = exc.response.text if exc.response is not None else ""
        status = exc.response.status_code if exc.response is not None else "unknown"
        raise RuntimeError(f"Tavily request failed ({status}): {body[:200]}") from exc

    try:
        data = response.json()
    except ValueError as exc:
        raise RuntimeError("Tavily returned a non-JSON response") from exc
    if not isinstance(data, dict):
        raise RuntimeError("Tavily returned an invalid response payload")
    return data


def setup(plg: Plugin) -> None:
    @plg.tool(
        name="tavily_search",
        description="Search the web with Tavily and return ranked snippets.",
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Number of results to return (1-10)",
                },
                "search_depth": {
                    "type": "string",
                    "description": "Search depth: basic or advanced",
                },
                "include_answer": {
                    "type": "boolean",
                    "description": "Whether to include Tavily answer in output",
                },
                "include_raw_content": {
                    "type": "boolean",
                    "description": "Whether to include raw_content from results",
                },
            },
            "required": ["query"],
        },
        visibility=ToolVisibility.SCOPED,
        tags=["attention"],
        metadata={"provider": "tavily"},
    )
    async def tavily_search(arguments: dict[str, Any], _runtime: Any) -> dict[str, Any]:
        query = str(arguments.get("query", "") or "").strip()
        if not query:
            raise ValueError("query is required")

        config = _load_plugin_config(plg.plugin_id)

        api_key = config.tavily_api_key.strip() or os.getenv("TAVILY_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "Missing Tavily API key. Configure tavily_api_key in plugin settings or set "
                "TAVILY_API_KEY."
            )

        max_results = _coerce_max_results(arguments.get("max_results"), config.default_max_results)
        search_depth = _coerce_search_depth(
            arguments.get("search_depth"),
            config.default_search_depth,
        )
        include_answer = _coerce_bool(arguments.get("include_answer"), config.include_answer)
        include_raw_content = _coerce_bool(
            arguments.get("include_raw_content"),
            config.include_raw_content,
        )

        payload = await _tavily_search(
            api_key=api_key,
            query=query,
            max_results=max_results,
            timeout_seconds=config.timeout_seconds,
            search_depth=search_depth,
            include_answer=include_answer,
            include_raw_content=include_raw_content,
        )

        result: dict[str, Any] = {
            "provider": "tavily",
            "query": query,
            "results": _normalize_tavily_results(
                payload.get("results"),
                max_results=max_results,
                include_raw_content=include_raw_content,
            ),
        }

        if include_answer:
            result["answer"] = str(payload.get("answer", "") or "")

        response_time = _to_float(payload.get("response_time"))
        if response_time is not None:
            result["response_time"] = response_time

        return result
