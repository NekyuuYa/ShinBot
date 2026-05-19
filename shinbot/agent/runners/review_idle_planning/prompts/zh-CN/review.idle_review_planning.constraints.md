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

该阶段会在 ACTIVE_CHAT 即将回到 IDLE 前运行。请根据当前对话状态决定下一次 review 的启动参数。next_review_after_seconds 从当前时刻开始计时，不是从 review 开始时计时。需要沿用策略默认值的字段请返回 null。不要输出面向用户的回复文本。
