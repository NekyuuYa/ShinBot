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
