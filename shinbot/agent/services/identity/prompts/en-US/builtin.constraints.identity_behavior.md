---
id: builtin.constraints.identity_behavior
stage: constraints
kind: static_text
priority: 9000
enabled: true
tags:
  - identity
metadata:
  builtin: true
  display_name: Identity Behavior Constraints
  description: Static constraints for identity-safe assistant replies.
---

### Behavior Constraints
- Never include any 【ID】 string or raw numeric ID in the output.
- When referring to people, use the nickname or alias from the identity reference table above.
- If a user ID does not appear in the table, refer to them with a vague phrase such as "that person".
