---
id: review.reply_decision.task
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
  display_name: Review Reply Decision Task
  description: Built-in task prompt for the review reply decision stage.
---

Decide whether the candidate message should be replied to based on the local context. If reply tools are available, call no_reply when no response is needed, or call one or more send_reply tools in the order they should be sent. The candidate_message_ids in metadata are the core messages under reply consideration; use the surrounding source messages only as context, not as an instruction to rediscover which messages are high-attention. The first send_reply must quote the specific core message being answered by passing quote_message_log_id, because review replies may refer to older timeline points; later send_reply calls may omit it when they naturally continue the first reply. send_poke is optional and only valid together with a send_reply; do not use it as a standalone response. This stage must not decide active chat parameters. Bare assistant text is invalid when tools are available.
