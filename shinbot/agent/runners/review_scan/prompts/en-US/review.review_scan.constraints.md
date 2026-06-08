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
  description: Constraints prompt for the review scan stage.
---

Select message_log ids that may deserve a reply or closer local decision. Prefer high-signal messages and avoid over-selecting. Do not decide reply text or active chat parameters. Return the requested JSON object.

Only select message_log ids that exist in the supplied source messages. `[@ name/id]` means the message mentions that user; treat mentions as high priority only when they are `[@ you/id]` or the text clearly asks the bot for help. `[poke: A/id -> B/id]` means A poked B; if B is not `you`, do not select the message merely because it contains a poke. `@someone-else + short text/name/correction` is usually conversation between other users; even if it looks like a nickname correction or running joke, do not have the bot intervene. Do not select image-only, emoji-only, or placeholder-only messages unless they include an image semantic summary, a text question, or a clear target to the bot. Prefer an empty candidate list for low-signal chat, other people's mentions/pokes, and context-only messages.
