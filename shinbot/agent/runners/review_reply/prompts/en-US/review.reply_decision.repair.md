---
id: review.reply_decision.repair
stage: instructions
kind: static_text
priority: 9000
enabled: true
tags:
  - review
  - workflow
  - repair
metadata:
  builtin: true
  display_name: Review Reply Decision Repair
  description: Repair prompt when the reply decision stage produces bare text instead of tool calls.
---

The previous reply_decision round produced bare text or did not call any tools, but the review reply stage does not send bare text to users.
Please re-decide and must call tools:
- When a reply is needed, call one or more send_reply in send order.
- The first send_reply must include quote_message_log_id pointing to a core message from candidate_message_ids.
- Later send_reply calls may omit quote_message_log_id to continue the first reply.
- For lightweight acknowledgement only, call send_reaction alone and prefer a message_log_id from candidate_message_ids.
- When no reply is needed, call no_reply.
- send_poke is optional and only valid together with at least one send_reply in the same tool call batch.
Do not output bare text as a final reply again.
