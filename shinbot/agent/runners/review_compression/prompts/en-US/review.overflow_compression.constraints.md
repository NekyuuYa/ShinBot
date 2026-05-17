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
  description: Constraints prompt for the review overflow compression stage.
---

Only compress older overflow messages. Preserve unresolved topics, useful facts, and message ids that may deserve later reply review. Return the requested JSON object.
