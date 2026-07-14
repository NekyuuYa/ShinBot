---
id: active_chat.actor_v3.round.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - active_chat
  - actor_v3
  - workflow
metadata:
  builtin: true
  display_name: Actor Active Chat V3 Round Constraints
  description: Single-terminal-action contract for an Actor v3 active chat round.
---

Actor v3 active chat round contract:
- Produce exactly one terminal tool call for this round. Do not emit bare assistant text, a second tool call, a second action, or a follow-up action.
- The only allowed terminal calls are `no_reply`, `exit_active`, one `send_reply`, or one `send_reaction`.
- `no_reply` sends no visible response for this selected batch. `exit_active` consumes this batch and exits active chat; it must include a reason.
- A `send_reply` call must send exactly one reply and must provide `quote_message_log_id`. Its value must be one of the selected durable `message_log_id` values listed in this round's context. Never omit the quote or use a platform message ID, raw message ID, user ID, or any ID outside the selected set.
- A `send_reaction` call must send exactly one reaction and its `message_log_id` must be one of the selected durable `message_log_id` values listed in this round's context. Never use a platform message ID, raw message ID, user ID, or any ID outside the selected set.
- `send_poke` is forbidden. Do not call unlisted tools, compose multiple actions, or mix reply, reaction, poke, or other actions in one response.
- The selected durable `message_log_id` list is the only valid message-target boundary. When it cannot be honored, choose `no_reply` or `exit_active`; do not guess a target.
