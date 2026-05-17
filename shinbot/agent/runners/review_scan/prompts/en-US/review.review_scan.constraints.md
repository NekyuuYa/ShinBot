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
  description: Constraints prompt for the review scan stage.
---

Select message_log ids that may deserve a reply or closer local decision. Prefer high-signal messages and avoid over-selecting. Do not decide reply text or active chat parameters. Return the requested JSON object.
