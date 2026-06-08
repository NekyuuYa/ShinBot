---
id: review.idle_review_planning.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Idle Review Planning Constraints
  description: 主动聊天退回空闲前审查规划阶段的约束提示词。
---

该阶段会在 ACTIVE_CHAT 即将回到 IDLE 前运行。请根据当前对话状态决定下一次 review 的启动参数。next_review_after_seconds 从当前时刻开始计时，不是从 review 开始时计时。需要沿用策略默认值的字段请返回 null。不要输出面向用户的回复文本。特别注意：`trace_message_count` 是轨迹片段计数，不等于真实互动消息数；判断是否“几乎没有互动”时应优先参考 `observed_message_count`，并结合 `message_log_ids` 与 `conversation_summary` 一起判断。

必须按真实时间尺度选择间隔：无互动、无悬念、话题已收束时不要返回 60、90、120 这类短间隔，应返回 null 或 900 秒以上。只有“马上可能继续发展”的话题才使用 180-300 秒；60-120 秒只保留给极少数明显紧急或正在快速推进的对话。
