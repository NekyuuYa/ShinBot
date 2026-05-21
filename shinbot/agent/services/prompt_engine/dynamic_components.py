"""Shared ids for built-in dynamic prompt components."""

from __future__ import annotations

REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS = {
    "overflow_compression": "review.overflow_compression.instruction",
    "review_scan": "review.review_scan.instruction",
    "block_digest": "review.block_digest.instruction",
    "reply_decision": "review.reply_decision.instruction",
    "active_chat_bootstrap": "review.active_chat_bootstrap.instruction",
    "idle_review_planning": "review.idle_review_planning.instruction",
}

ACTIVE_CHAT_FAST_MODE_BATCH_COMPONENT_ID = "active_chat.fast_mode.batch"

MEDIA_INSPECTION_INSTRUCTION_COMPONENT_ID = "media.media_inspection.instruction"
STICKER_SUMMARY_INSTRUCTION_COMPONENT_ID = "media.sticker_summary.instruction"
MEDIA_REANALYSIS_INSTRUCTION_COMPONENT_ID = "media.media_reanalysis.instruction"


def review_stage_instruction_component_id(purpose: str) -> str:
    """Return the dynamic instruction component id for one review stage."""

    normalized = str(purpose or "").strip()
    return REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS.get(
        normalized,
        f"review.{normalized or 'stage'}.instruction",
    )


def media_instruction_component_id(trigger: str) -> str:
    """Return the dynamic instruction component id for one media stage."""

    normalized = str(trigger or "").strip()
    if normalized == "media_reanalysis":
        return MEDIA_REANALYSIS_INSTRUCTION_COMPONENT_ID
    if normalized == "sticker_summary":
        return STICKER_SUMMARY_INSTRUCTION_COMPONENT_ID
    return MEDIA_INSPECTION_INSTRUCTION_COMPONENT_ID


def active_chat_instruction_component_id(stage_id: str) -> str:
    """Return the dynamic instruction component id for active chat."""

    return ACTIVE_CHAT_FAST_MODE_BATCH_COMPONENT_ID if str(stage_id or "").strip() == "fast_mode" else ""


__all__ = [
    "MEDIA_INSPECTION_INSTRUCTION_COMPONENT_ID",
    "MEDIA_REANALYSIS_INSTRUCTION_COMPONENT_ID",
    "ACTIVE_CHAT_FAST_MODE_BATCH_COMPONENT_ID",
    "REVIEW_STAGE_INSTRUCTION_COMPONENT_IDS",
    "STICKER_SUMMARY_INSTRUCTION_COMPONENT_ID",
    "active_chat_instruction_component_id",
    "media_instruction_component_id",
    "review_stage_instruction_component_id",
]
