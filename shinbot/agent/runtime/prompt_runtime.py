"""Runtime-owned dynamic prompt resolvers."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from shinbot.agent.prompt_manager.schema import (
        PromptAssemblyRequest,
        PromptComponent,
        PromptSource,
    )


_WEEKDAY_NAMES = (
    "星期一",
    "星期二",
    "星期三",
    "星期四",
    "星期五",
    "星期六",
    "星期日",
)


def resolve_current_time_prompt(
    request: PromptAssemblyRequest,
    _component: PromptComponent,
    _source: PromptSource,
) -> dict[str, Any]:
    """Render a dynamic local-time hint for the current prompt assembly."""

    now = datetime.now().astimezone()
    offset = now.utcoffset()
    total_minutes = int(offset.total_seconds() // 60) if offset is not None else 0
    sign = "+" if total_minutes >= 0 else "-"
    abs_minutes = abs(total_minutes)
    offset_text = f"UTC{sign}{abs_minutes // 60:02d}:{abs_minutes % 60:02d}"
    timezone_name = now.tzname() or offset_text

    text = (
        "### 当前时间\n"
        f"- 现在的本地时间：{now.strftime('%Y-%m-%d %H:%M:%S')} {timezone_name} ({offset_text})\n"
        f"- 今天是：{now.strftime('%Y-%m-%d')}，{_WEEKDAY_NAMES[now.weekday()]}\n"
        "- 当你需要理解“今天 / 明天 / 昨天 / 刚刚 / 现在”等相对时间时，请以上述时间为准；"
        "若用户可能混淆日期，优先给出绝对日期。"
    )

    return {
        "text": text,
        "now_iso": now.isoformat(),
        "timezone": timezone_name,
        "utc_offset": offset_text,
    }


def resolve_message_text_prompt(
    request: PromptAssemblyRequest,
    _component: PromptComponent,
    _source: PromptSource,
) -> dict[str, Any]:
    """Render the caller-provided message batch as one instruction block."""

    text = str(request.template_inputs.get("message_text", "") or "").strip()
    content_blocks = request.template_inputs.get("message_blocks")
    return {
        "text": text,
        "content_blocks": content_blocks,
        "has_message_text": bool(text),
    }
