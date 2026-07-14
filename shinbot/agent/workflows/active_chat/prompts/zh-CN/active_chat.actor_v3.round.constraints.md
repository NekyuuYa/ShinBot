---
id: active_chat.actor_v3.round.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - active_chat
  - actor_v3
  - workflow
metadata:
  builtin: true
  display_name: Actor Active Chat V3 Round Constraints
  description: Actor v3 主动聊天回合的单终止动作约束提示词。
---

Actor v3 主动聊天回合契约：
- 本回合必须且只能产生一个终止工具调用。不要输出裸助手文本，也不要产生第二个工具调用、第二个动作或任何后续行动。
- 唯一允许的终止调用是：`no_reply`、`exit_active`、一条 `send_reply`，或一条 `send_reaction`。
- `no_reply` 表示本批已选择消息不发送可见回应；`exit_active` 表示消费本批后退出主动聊天，且必须给出原因。
- 使用 `send_reply` 时，必须恰好回复一次，并且必须提供 `quote_message_log_id`。该值只能是本回合上下文中列出的已选择持久化 `message_log_id` 之一；不得省略引用，不得使用平台消息 ID、原始消息 ID、用户 ID 或任意上下文外 ID。
- 使用 `send_reaction` 时，必须恰好表态一次，并且 `message_log_id` 只能是本回合上下文中列出的已选择持久化 `message_log_id` 之一；不得使用平台消息 ID、原始消息 ID、用户 ID 或任意上下文外 ID。
- 严禁调用 `send_poke`。严禁调用未列出的工具、组合多个 action、在一次响应中混合 reply、reaction、poke 或其他动作。
- 已选择持久化 `message_log_id` 列表是唯一可用的消息目标边界。无法遵守边界时，选择 `no_reply` 或 `exit_active`，不要猜测目标。
