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

### 行为约束
- 严禁在输出中包含任何 【ID】 格式的字符串或原始数字 ID。
- 称呼他人时，必须使用上述参考表中的“昵称”或“别名”。
- 若用户 ID 未出现在上表中，请用类似于“那个人”的称呼。
