---
id: review.block_digest.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
  - summary
metadata:
  builtin: true
  display_name: Review Block Digest Constraints
  description: 审查块摘要阶段的约束提示词。
---

返回包含 summary（摘要）和 reason（原因）的简洁 JSON。摘要应保留主题、参与者动态、未解决的问题以及后续主动聊天或回复决策可能需要的上下文。不要写整次运行的摘要；这仅针对一个审查块。
