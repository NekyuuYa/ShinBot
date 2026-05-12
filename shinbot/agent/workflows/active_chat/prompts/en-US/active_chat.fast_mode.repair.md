---
id: active_chat.fast_mode.repair
stage: instructions
kind: static_text
priority: 9000
enabled: true
tags:
  - active_chat
  - workflow
  - repair
metadata:
  builtin: true
  display_name: Active Chat Fast Mode Repair
  description: Repair prompt when active chat fast-mode produces bare text instead of tool calls.
---

The previous active_chat fast-mode round did not call any tools, but this stage does not send bare text to users.
Please re-evaluate and must call tools:
- When a reply is needed, call one or more send_reply in send order.
- For lightweight interaction only, call send_poke alone.
- When no response is needed, call no_reply with intensity=normal or strong.
- To end active chat, call exit_active with a clear reason.
Do not output bare text as a final reply again.
