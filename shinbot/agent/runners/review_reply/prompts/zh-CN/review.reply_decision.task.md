---
id: review.reply_decision.task
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
  display_name: Review Reply Decision Task
  description: 审查回复决策阶段的内置任务提示词。
---

根据局部上下文决定是否回复候选消息。如果回复工具可用，不需要回复时调用 no_reply，需要回复时按发送顺序调用一个或多个 send_reply。metadata 中的 candidate_message_ids 是核心待评估消息，但可能是上游误选；仅将周围的 source messages 作为上下文，不要将其视为需要重新发现高关注消息的指令。若 metadata 标出候选消息只 @/poke 了其他成员且没有指向你，请调用 no_reply，不要因为短文本昵称、纠错或上下文联想而插话。第一条 send_reply 必须通过 quote_message_log_id 引用具体的核心消息，因为审查回复可能涉及较早的时间线；后续 send_reply 调用在自然延续第一条回复时可以省略。send_poke 是可选互动，只能与至少一个 send_reply 出现在同一批 tool call 中。此阶段不得决定主动聊天参数。工具可用时，裸文本无效。
