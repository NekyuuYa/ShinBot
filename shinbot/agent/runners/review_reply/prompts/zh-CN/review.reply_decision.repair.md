---
id: review.reply_decision.repair
stage: instructions
kind: static_text
priority: 9000
enabled: true
tags:
  - review
  - workflow
  - repair
metadata:
  builtin: true
  display_name: Review Reply Decision Repair
  description: 回复决策阶段输出裸文本时的修复提示词。
---

上一轮 reply_decision 输出了裸文本或没有调用工具，但 review reply 阶段不会把裸文本发送给用户。
请重新决策，并必须调用工具：
- 需要回复时，按发送顺序调用一个或多个 send_reply。
- 第一条 send_reply 必须带 quote_message_log_id，且必须指向 candidate_message_ids 中的核心消息。
- 后续 send_reply 可以不带 quote_message_log_id，用于延续第一条回复。
- 不需要回复时调用 no_reply。
- send_poke 是可选互动，只能与至少一个 send_reply 出现在同一批 tool call 中。
不要再输出裸文本作为最终回复。
