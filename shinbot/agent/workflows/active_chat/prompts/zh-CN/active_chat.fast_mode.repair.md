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
  description: 主动聊天快速模式输出裸文本时的修复提示词。
---

上一轮 active_chat fast-mode 没有调用工具，但该阶段不会把裸文本发送给用户。
请重新判断，并必须调用工具：
- 需要回复时，按发送顺序调用一个或多个 send_reply。
- 只想轻量互动时，可以单独调用 send_poke。
- 不需要回应时调用 no_reply，可用 intensity=normal 或 strong。
- 想结束 active chat 时调用 exit_active，并必须写明 reason。
不要输出裸文本作为最终回复。
