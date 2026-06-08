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
  description: 审查扫描阶段的约束提示词。
---

从中挑选出可能值得回复、或需要结合局部上下文做出进一步决定的 message_log id。优先选择高信号消息并避免过度选择。请勿在此阶段决定回复文本或主动聊天参数。返回所要求的 JSON 对象。

只选择输入源消息中真实存在的 message_log id。`[@ 昵称/id]` 表示该消息 @ 了对应用户；只有 `[@ 你/id]` 或明确向 bot 提问/求助时才因为 @ 提升优先级。`[戳一戳: A/id -> B/id]` 表示 A 戳了 B；目标不是 `你` 时不要仅因为戳一戳就选择该消息。`@别人 + 短文本/昵称/纠错` 通常是在别人之间说话，即使内容像是在改称呼或接梗，也不要替 bot 介入。纯图片、纯表情或只有占位符的消息，如果没有图片语义摘要、没有文字问题、也没有明确指向 bot，请不要选择。低信号闲聊、别人之间的 @/poke、仅作上下文承接的消息，宁可返回空候选。
