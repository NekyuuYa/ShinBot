---
id: review.review_scan.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Review Scan Constraints
  description: 审查扫描阶段的约束提示词。
---

从中挑选出可能值得回复、或需要结合局部上下文做出进一步决定的 message_log id。优先选择高信号消息并避免过度选择。请勿在此阶段决定回复文本或主动聊天参数。返回所要求的 JSON 对象。
