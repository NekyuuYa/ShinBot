---
id: review.idle_review_planning.task
stage: instructions
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
  - task
metadata:
  builtin: true
  display_name: Idle Review Planning Task
  description: Built-in task prompt for active chat idle review planning.
---

Review the supplied active chat tail and metadata. Return JSON with next_review_after_seconds, reason, optional mention_sensitivity, optional mention_wake_count, and optional mention_wake_window_seconds. `observed_message_count` is the number of real messages observed during this active-chat session. `trace_message_count` is only the number of retained conversation-trace entries. Do not treat `trace_message_count = 0` as meaning “no active conversation” by itself; if `observed_message_count > 0`, `message_log_ids` is non-empty, or `conversation_summary` is present, then a real interaction happened.

Use conservative real-world timing. next_review_after_seconds is not a heartbeat; it is the start time for the next low-frequency review pass:
- If the conversation has naturally settled, had little or no interaction, has no new messages, and has no unresolved topic, return 900-1800 seconds. If the policy default is appropriate, return null.
- If there is a mildly unresolved topic but no need to follow up soon, return 600-900 seconds.
- Return 180-300 seconds only when the topic is genuinely fast-moving, waiting for user follow-up, or worth checking again soon.
- 60-120 seconds is an extremely short interval. Use it only for clearly urgent or actively developing conversations where checking again within the next minute is necessary. Do not map “low-frequency observation”, “check later”, “settled”, or “no interaction” to 60-120 seconds.
