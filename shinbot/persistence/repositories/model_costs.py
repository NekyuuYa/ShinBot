"""Cost and usage helpers for model execution repositories."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _parse_utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _hour_bucket_start(value: str) -> str:
    return _parse_utc_datetime(value).replace(minute=0, second=0, microsecond=0).isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _iter_cost_metadata_maps(cost_metadata: dict[str, Any]) -> list[dict[str, Any]]:
    maps = [cost_metadata]
    for key in ("pricing", "prices", "costs"):
        nested = cost_metadata.get(key)
        if isinstance(nested, dict):
            maps.append(nested)
    return maps


def _cost_rate_per_token(cost_metadata: dict[str, Any], candidate_groups: list[list[str]]) -> float:
    metadata_maps = _iter_cost_metadata_maps(cost_metadata)

    for keys in candidate_groups:
        for metadata in metadata_maps:
            for key in keys:
                value = _safe_float(metadata.get(key))
                if value is not None:
                    if "PerMillion" in key or "_per_million_" in key:
                        return value / 1_000_000
                    if "Per1k" in key or "_per_1k_" in key:
                        return value / 1_000
                    return value
    return 0.0


def _estimate_cost_from_metadata(
    cost_metadata: dict[str, Any],
    *,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int,
    cache_write_tokens: int,
) -> float:
    if not isinstance(cost_metadata, dict) or not cost_metadata:
        return 0.0

    billable_input_tokens = max(
        int(input_tokens) - int(cache_read_tokens) - int(cache_write_tokens),
        0,
    )

    input_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["inputPerToken", "promptPerToken", "input_per_token", "prompt_per_token"],
            [
                "inputPer1kTokens",
                "promptPer1kTokens",
                "input_per_1k_tokens",
                "prompt_per_1k_tokens",
            ],
            [
                "inputPerMillionTokens",
                "promptPerMillionTokens",
                "input_per_million_tokens",
                "prompt_per_million_tokens",
            ],
        ],
    )
    output_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["outputPerToken", "completionPerToken", "output_per_token", "completion_per_token"],
            [
                "outputPer1kTokens",
                "completionPer1kTokens",
                "output_per_1k_tokens",
                "completion_per_1k_tokens",
            ],
            [
                "outputPerMillionTokens",
                "completionPerMillionTokens",
                "output_per_million_tokens",
                "completion_per_million_tokens",
            ],
        ],
    )
    cache_read_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["cacheReadPerToken", "cache_read_per_token"],
            ["cacheReadPer1kTokens", "cache_read_per_1k_tokens"],
            ["cacheReadPerMillionTokens", "cache_read_per_million_tokens"],
        ],
    )
    cache_write_rate = _cost_rate_per_token(
        cost_metadata,
        [
            ["cacheWritePerToken", "cache_write_per_token"],
            ["cacheWritePer1kTokens", "cache_write_per_1k_tokens"],
            ["cacheWritePerMillionTokens", "cache_write_per_million_tokens"],
        ],
    )

    total = (
        billable_input_tokens * input_rate
        + output_tokens * output_rate
        + cache_read_tokens * cache_read_rate
        + cache_write_tokens * cache_write_rate
    )
    return round(total, 6)


def _zero_usage_metrics() -> dict[str, Any]:
    return {
        "total_calls": 0,
        "successful_calls": 0,
        "failed_calls": 0,
        "cache_hits": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "estimated_cost": 0.0,
    }


def _add_usage_metrics(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key in (
        "total_calls",
        "successful_calls",
        "failed_calls",
        "cache_hits",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "estimated_cost",
    ):
        target[key] = target.get(key, 0) + source.get(key, 0)
