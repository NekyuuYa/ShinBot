---
id: review.idle_review_planning.task
stage: instructions
kind: static_text
version: 1.2.0
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

阅读提供的主动聊天尾部上下文与元信息。返回包含 next_review_after_seconds、reason、可选 mention_sensitivity、可选 mention_wake_count、可选 mention_wake_window_seconds 的 JSON。`observed_message_count` 表示本次 active chat 实际观察到的消息数；`trace_message_count` 只表示当前保留下来的对话轨迹片段数。不要把 `trace_message_count = 0` 直接理解为“没有活跃对话”；如果 `observed_message_count > 0`、存在 `message_log_ids`、或存在 `conversation_summary`，都说明本次 active chat 真实发生过互动。

时间尺度请保守处理。next_review_after_seconds 不是“下一次心跳”，而是下一轮低频 review 的启动时间：
- 对话已经自然收束、几乎没有互动、没有新消息、没有未解决话题时，返回 900-1800 秒；如果只是想沿用默认间隔，返回 null。
- 有轻微未解决话题但不需要马上跟进时，返回 600-900 秒。
- 只有确实存在快速变化、正在等待用户补充、或短时间内继续观察很有价值的话题，才返回 180-300 秒。
- 60-120 秒属于非常短的间隔，只能用于明显紧急、正在快速推进、且下一分钟内复查确实必要的情况；不要把“低频观察”“稍后看看”“无互动”映射成 60-120 秒。
