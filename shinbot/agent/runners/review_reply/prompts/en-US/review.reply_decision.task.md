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

Decide whether the candidate message should be replied to based on the local context. If reply tools are available, call no_reply when no response is needed, call one or more send_reply tools in send order when text is needed, or call send_reaction alone when a lightweight acknowledgement, agreement, amusement, or comfort reaction is enough. The candidate_message_ids in metadata are the core messages under evaluation, but they may be upstream false positives; use the surrounding source messages only as context, not as an instruction to rediscover which messages are high-attention. If metadata marks a candidate as only mentioning/poking other members and not you, call no_reply; do not join because of a short nickname, correction, or contextual association. The first send_reply must quote the specific core message being answered by passing quote_message_log_id, because review replies may refer to older timeline points; later send_reply calls may omit it when they naturally continue the first reply. send_reaction should prefer message_log_id and target a candidate message. send_poke is optional and only valid together with a send_reply; do not use it as a standalone response. This stage must not decide active chat parameters. Bare assistant text is invalid when tools are available.
