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
  description: Constraints prompt for the review active chat bootstrap stage.
---

After review and reply decisions finish, choose only active chat bootstrap parameters. Do not send replies. Choose exactly one semantic disposition: exit_soon, watch, casual, engaged, or focused. Do not output numeric interest or decay parameters; ShinBot maps the disposition to its internal active chat curve and applies delayed correction itself. Return the requested JSON object.
