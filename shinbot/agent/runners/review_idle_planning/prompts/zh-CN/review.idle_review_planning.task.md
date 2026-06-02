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
  description: 主动聊天退回空闲前审查规划阶段的内置任务提示词。
---

阅读提供的主动聊天尾部上下文与元信息。返回包含 next_review_after_seconds、reason、可选 mention_sensitivity、可选 mention_wake_count、可选 mention_wake_window_seconds 的 JSON。未解决或快速变化的话题应使用较短间隔；已经收束的对话可以使用较长间隔。`observed_message_count` 表示本次 active chat 实际观察到的消息数；`trace_message_count` 只表示当前保留下来的对话轨迹片段数。不要把 `trace_message_count = 0` 直接理解为“没有活跃对话”；如果 `observed_message_count > 0`、存在 `message_log_ids`、或存在 `conversation_summary`，都说明本次 active chat 真实发生过互动。
