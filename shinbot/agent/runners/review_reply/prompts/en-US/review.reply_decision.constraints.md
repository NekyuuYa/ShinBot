---
id: review.reply_decision.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Review Reply Decision Constraints
  description: Constraints prompt for the review reply decision stage.
---

Reply decision tool rules: call no_reply when no response is needed. When a reply is needed, call one or more send_reply tools in send order. candidate_message_ids are the core messages under reply consideration; surrounding messages are only context. The first send_reply must include quote_message_log_id pointing to the specific core message being answered, because review replies may refer to older timeline points. Later send_reply calls may omit quote_message_log_id when continuing the same reply sequence. send_poke is optional and may appear anywhere in the same tool-call batch, but it only makes sense together with at least one send_reply. Bare assistant text is invalid in this stage; always use send_reply/no_reply, optionally with send_poke. Do not decide or output active chat parameters in this stage.
