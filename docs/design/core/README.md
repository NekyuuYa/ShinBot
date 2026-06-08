# Core Design Docs

`docs/design/core/` 记录 ShinBot 最底层的消息、资源和交互语义。这里的文档描述跨平台归一化后的核心模型。

## 当前文档

- `core_philosophy.md`
  - 核心层设计哲学和边界。
- `message_workflow.md`
  - 消息从接入、归一化、路由到发送的设计流转。
- `message_element_spec.md`
  - 统一消息元素 AST 规格。
- `message_egress_spec.md`
  - 消息发送与 egress 语义。
- `resource_schema_spec.md`
  - 用户、频道、群组、成员等统一资源 schema。

## 放置规则

- 会影响所有 adapter、route、formatter 或 persistence 的基础语义放在这里。
- 单个平台的扩展能力放在 `../extensibility/`。
- 当前代码实现细节放在 `../../internals/`。
