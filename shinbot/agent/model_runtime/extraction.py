"""Response extraction helpers for LiteLLM-backed runtime calls."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


def provider_type_for_litellm(provider_type: str) -> str | None:
    if provider_type == "custom_openai":
        return "openai"
    if provider_type == "dashscope":
        return "dashscope"
    if provider_type == "azure_openai":
        return "azure"
    return None


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def maybe_get(mapping: Any, key: str, default: Any = None) -> Any:
    if mapping is None:
        return default
    if isinstance(mapping, dict):
        return mapping.get(key, default)
    return getattr(mapping, key, default)


def response_to_dict(response: Any) -> dict[str, Any]:
    if hasattr(response, "model_dump"):
        return response.model_dump()  # type: ignore[no-any-return]
    if isinstance(response, dict):
        return response
    if hasattr(response, "__dict__"):
        return dict(response.__dict__)
    return {}


def extract_text(response: Any) -> str:
    payload = response_to_dict(response)
    choices = payload.get("choices") or []
    if not choices:
        return ""

    message = (choices[0] or {}).get("message") or {}
    content = message.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "".join(parts)
    return ""


def extract_embedding(response: Any) -> list[float]:
    payload = response_to_dict(response)
    data = payload.get("data") or []
    if not data:
        return []
    embedding = (data[0] or {}).get("embedding")
    if isinstance(embedding, list):
        return [float(item) for item in embedding]
    return []


def extract_rerank_results(response: Any) -> list[dict[str, Any]]:
    payload = response_to_dict(response)
    results = payload.get("results") or []
    normalized = []
    for item in results:
        normalized.append(
            {
                "index": int(item.get("index", 0)),
                "relevance_score": float(item.get("relevance_score", 0.0)),
                "document": item.get("document"),
            }
        )
    return normalized


def extract_speech_bytes(response: Any) -> bytes:
    if hasattr(response, "read"):
        return bytes(response.read())
    if hasattr(response, "content") and isinstance(response.content, bytes):
        return response.content
    if isinstance(response, bytes):
        return response
    return b""


def extract_transcription_text(response: Any) -> str:
    if hasattr(response, "text"):
        return str(response.text)
    payload = response_to_dict(response)
    text = payload.get("text")
    if isinstance(text, str):
        return text
    return ""


def extract_image_urls(response: Any) -> list[str]:
    payload = response_to_dict(response)
    data = payload.get("data") or []
    urls = []
    for item in data:
        if isinstance(item, dict):
            url = item.get("url") or item.get("b64_json")
            if url:
                urls.append(str(url))
    return urls


def extract_usage(response: Any) -> dict[str, Any]:
    payload = response_to_dict(response)
    usage = payload.get("usage") or {}
    prompt_details = usage.get("prompt_tokens_details") or {}
    return {
        "input_tokens": int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0),
        "output_tokens": int(usage.get("completion_tokens") or usage.get("output_tokens") or 0),
        "cache_read_tokens": int(
            prompt_details.get("cached_tokens")
            or usage.get("cache_read_input_tokens")
            or usage.get("cache_read_tokens")
            or 0
        ),
        "cache_write_tokens": int(
            prompt_details.get("cache_creation_input_tokens")
            or usage.get("cache_creation_input_tokens")
            or usage.get("cache_write_input_tokens")
            or usage.get("cache_write_tokens")
            or 0
        ),
    }


def extract_estimated_cost(response: Any) -> float | None:
    payload = response_to_dict(response)
    if isinstance(payload.get("response_cost"), (int, float)):
        return float(payload["response_cost"])

    hidden = payload.get("_hidden_params")
    if isinstance(hidden, dict) and isinstance(hidden.get("response_cost"), (int, float)):
        return float(hidden["response_cost"])

    hidden_attr = maybe_get(response, "_hidden_params")
    if isinstance(hidden_attr, dict) and isinstance(hidden_attr.get("response_cost"), (int, float)):
        return float(hidden_attr["response_cost"])
    return None


def extract_think_text(response: Any) -> str:
    """Extract model reasoning/thinking text, handling multi-provider response shapes."""

    payload = response_to_dict(response)
    choices = payload.get("choices") or []
    if not choices:
        return ""
    message = (choices[0] or {}).get("message") or {}

    for key in ("reasoning_content", "reasoning"):
        value = message.get(key)
        if isinstance(value, str) and value:
            return value

    content = message.get("content")
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "thinking":
                text = block.get("thinking") or block.get("text") or ""
                if text:
                    parts.append(str(text))
        if parts:
            return "\n".join(parts)

    for key in ("think", "thought"):
        value = message.get(key)
        if isinstance(value, str) and value:
            return value

    return ""


def extract_tool_calls_list(response: Any) -> list[dict[str, Any]]:
    """Extract tool calls from the model response as a list of plain dicts."""

    payload = response_to_dict(response)
    choices = payload.get("choices") or []
    if not choices:
        return []
    message = (choices[0] or {}).get("message") or {}
    tool_calls = message.get("tool_calls")
    if not isinstance(tool_calls, list):
        return []
    result: list[dict[str, Any]] = []
    for tc in tool_calls:
        if isinstance(tc, dict):
            result.append(tc)
        elif hasattr(tc, "__dict__"):
            result.append(dict(tc.__dict__))
    return result


def extract_injected_context(messages: list[dict[str, Any]]) -> str:
    """Return the content array of the last user message as a JSON string."""

    for msg in reversed(messages):
        if isinstance(msg, dict) and msg.get("role") == "user":
            content = msg.get("content")
            if content is None:
                content = []
            elif isinstance(content, str):
                content = [{"type": "text", "text": content}]
            return json.dumps(content, ensure_ascii=False)
    return "[]"
