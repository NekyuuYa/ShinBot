---
id: review.review_scan.task
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
  display_name: Review Scan Task
  description: 审查扫描阶段的内置任务提示词。
---

评估提供的未读消息，从中挑选出可能值得回复、或需要结合局部上下文做出进一步决定的 message_log id。请勿由此决定主动聊天的状态。