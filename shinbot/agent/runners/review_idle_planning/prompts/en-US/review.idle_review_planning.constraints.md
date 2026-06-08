---
id: review.idle_review_planning.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Idle Review Planning Constraints
  description: Constraints prompt for active chat idle review planning.
---

This stage runs immediately before ACTIVE_CHAT returns to IDLE. Choose the next review start parameters from the current conversation state. next_review_after_seconds is counted from now, not from review start. Use null for fields that should keep policy defaults. Do not write a user-facing reply. Important: `trace_message_count` is a retained trace-entry count, not the true interaction count. When deciding whether the session had little or no interaction, prefer `observed_message_count` and also consider `message_log_ids` and `conversation_summary`.

Choose intervals by actual time scale: for no interaction, no suspense, and settled topics, do not return short intervals such as 60, 90, or 120 seconds; return null or at least 900 seconds. Use 180-300 seconds only for topics that may continue developing soon. Reserve 60-120 seconds for rare cases that are clearly urgent or actively moving.
