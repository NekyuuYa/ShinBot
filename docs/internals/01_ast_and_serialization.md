# ShinBot 实现内幕：AST 与序列化 (AST & Serialization)

本文档剖析了 ShinBot 如何将复杂的富文本消息抽象为 AST，并实现与 Satori XML 的健壮转换。

## 1. 核心模型：MessageElement 与 Message
位于 `shinbot/schema/elements.py`。

### 1.1 实现方法
- **递归 Pydantic 模型**: `MessageElement` 类通过 `children: list[MessageElement]` 实现了递归嵌套。利用 Pydantic v2 的 `model_config = {"extra": "forbid"}` 确保了 AST 结构的严谨性。
- **工厂构造器**: 提供了 `.text()`, `.at()`, `.img()` 等静态方法，使得通过代码构造 AST 极其简洁。
- **双视图 API**: `Message` 类持有元素序列，并通过 `.text` 属性（递归提取纯文本）和 `.to_xml()` 方法（序列化）提供不同的视图。

### 1.2 健壮性分析
- **类型安全**: Pydantic 强制校验了 `attrs` 和 `type` 的合法性。
- **递归限制**: 在处理极大嵌套深度的消息时，递归提取文本可能面临栈溢出风险（目前常规消息无此担忧，但需注意风控）。

---

## 2. 解析器实现：SatoriParser
位于 `shinbot/utils/satori_parser.py`。

### 2.1 实现方法
- **基于 lxml 的流式解析**: 放弃了脆弱的正则表达式，采用 `lxml.etree` 处理 XML。这确保了它能完美处理 `&lt;` 等 XML 实体转义。
- **混合内容支持**: 能够正确解析 `文本 <at /> 文本` 这种混合结构（通过处理节点的 `tail` 属性实现）。
- **容错与扩展**: 对未知标签（如 `llonebot:ark`）采取宽容处理，将其保留为 `MessageElement` 节点而非直接报错，保证了对非标 Satori 实现的兼容。

### 2.2 解耦性
- 解析器完全独立于网络 I/O，仅负责 **String ⇌ AST** 的纯逻辑转换，可被适配器、核心引擎或插件单独引用。
