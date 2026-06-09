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

Reply decision tool rules: call no_reply when no response is needed. When a text reply is needed, call one or more send_reply tools in send order; when only lightweight agreement, acknowledgement, amusement, or comfort is needed, call send_reaction alone. candidate_message_ids are the core messages under reply consideration, but they come from an upstream scan and may include false positives; do not treat candidate status as a requirement to reply. Surrounding messages are only context. The first send_reply must include quote_message_log_id pointing to the specific core message being answered, because review replies may refer to older timeline points. Later send_reply calls may omit quote_message_log_id when continuing the same reply sequence. send_reaction should include message_log_id, and that id should come from candidate_message_ids; do not choose unrelated history messages to react to. send_poke is optional and may appear anywhere in the same tool-call batch, but it only makes sense together with at least one send_reply. Bare assistant text is invalid in this stage; always use send_reply/send_reaction/no_reply, optionally with send_poke. Do not decide or output active chat parameters in this stage.

Targeting rule: treat an action as targeting you only when the message text explicitly says it targets "you". Examples: `[@ you]`, a poke rendered as `poked you`, or a clear reply to your own message. Mentions like `[@ someone]`, `[@ username/id]`, and pokes rendered as targeting a specific other id are context about other members or uncertain targets; do not misread them as actions toward you, and do not reply only because of them. If Metadata JSON candidate_target_facts or other_target_only_candidate_message_ids marks a candidate as targeting only other members, default to no_reply; even if the text looks like a nickname, correction, or short comment, do not explain for them or join that unrelated exchange.
