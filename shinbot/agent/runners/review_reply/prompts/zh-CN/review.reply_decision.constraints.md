---
id: review.reply_decision.constraints
stage: constraints
kind: static_text
priority: 100
enabled: true
tags:
  - review
  - workflow
metadata:
  builtin: true
  display_name: Review Reply Decision Constraints
  description: 审查回复决策阶段的约束提示词。
---

回复决策工具规则：不需要回复时调用 no_reply。需要回复时，按发送顺序调用一个或多个 send_reply 工具。candidate_message_ids 是回复考虑的核心消息；周围的消息仅作为上下文参考。第一个 send_reply 必须包含 quote_message_log_id 并指向正在回答的具体核心消息，因为审查回复可能涉及较旧的时间点。后续的 send_reply 在延续同一回复序列时可以省略 quote_message_log_id。send_poke 是可选的，可以出现在同一批工具调用中的任何位置，但仅在伴随至少一个 send_reply 时才有意义。在此阶段，裸助手文本是无效的；请始终使用 send_reply/no_reply，并可选配 send_poke。请勿在此阶段决定或输出主动聊天参数。

指向性判断规则：只有消息内容明确显示为指向“你”的动作，才视为指向你。例如 `[@ 你]`、`[戳一戳: ...戳了你一下]`、明确回复你的消息，才表示对你发起 @、戳一戳或回复。`[@ 某人]`、`[@ 用户名]`、`[@ id]`、`[戳一戳: ...戳了 某个id 一下]` 都只是指向其他成员或目标不确定的上下文，不要误读为对你的动作，也不要仅因此回复。
