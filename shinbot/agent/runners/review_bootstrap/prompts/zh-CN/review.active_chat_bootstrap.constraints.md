---
id: review.active_chat_bootstrap.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Review Active Chat Bootstrap Constraints
  description: 主动聊天引导阶段的约束提示词。
---

在审查/回复完成后，仅选择主动聊天引导参数。不要发送回复。仅选择一种语义倾向（disposition）：exit_soon、watch、casual、engaged 或 focused。请勿输出数值形式的兴趣（interest）或衰减（decay）参数；ShinBot 会将该倾向映射到内部的主动聊天曲线，并自行应用延迟修正。返回所要求的 JSON 对象。
