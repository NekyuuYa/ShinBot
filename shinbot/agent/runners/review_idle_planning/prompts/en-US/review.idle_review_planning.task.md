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

Review the supplied active chat tail and metadata. Return JSON with next_review_after_seconds, reason, optional mention_sensitivity, optional mention_wake_count, and optional mention_wake_window_seconds. Short intervals are for unresolved or fast-moving topics; longer intervals are for settled conversations.
