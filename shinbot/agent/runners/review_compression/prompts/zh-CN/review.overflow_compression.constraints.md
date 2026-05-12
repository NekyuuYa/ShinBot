---
id: review.overflow_compression.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Review Overflow Compression Constraints
  description: 溢出消息压缩阶段的约束提示词。
---

仅压缩较旧的溢出消息。保留未解决的话题、有用的事实，以及可能值得后续回复审查的消息 id。返回所要求的 JSON 对象。
