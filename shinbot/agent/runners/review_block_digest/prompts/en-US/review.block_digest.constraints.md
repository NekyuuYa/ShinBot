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
  description: Constraints prompt for the review block digest stage.
---

Return concise JSON containing summary and reason. The summary should preserve topics, participant dynamics, unresolved issues, and context that later active chat or reply decisions may need. Do not write a full-run summary; this is only for one review block.
