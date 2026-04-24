"""Approximate token estimation utilities for context packing."""

from __future__ import annotations

import math
import re

_CJK_RANGES = (
    "\u4e00-\u9fff"
    "\u3400-\u4dbf"
    "\uf900-\ufaff"
    "\U00020000-\U0002a6df"
    "\U0002a700-\U0002b73f"
)
_CJK_PATTERN = re.compile(f"[{_CJK_RANGES}]")
_EN_WORD_PATTERN = re.compile(r"[A-Za-z]+(?:'[A-Za-z]+)?")


def estimate_text_tokens(text: str) -> int:
    """Estimate token count using the context-stage heuristic.

    Formula:
    - Chinese chars x 1.8
    - English words x 1.2
    - Remaining non-whitespace symbols x 1.0
    """

    if not text:
        return 0

    cjk_count = len(_CJK_PATTERN.findall(text))
    english_words = _EN_WORD_PATTERN.findall(text)
    english_count = len(english_words)

    remainder = _CJK_PATTERN.sub("", text)
    remainder = _EN_WORD_PATTERN.sub("", remainder)
    special_symbols = sum(1 for char in remainder if not char.isspace())

    estimate = (cjk_count * 1.8) + (english_count * 1.2) + special_symbols
    return int(math.ceil(estimate))


def estimate_role_content_tokens(role: str, content: str) -> int:
    text = f"{role}: {content}" if role else content
    return estimate_text_tokens(text.strip())
