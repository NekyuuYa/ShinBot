# ShinBot 技术规范：消息发送与外发流程 (Message Egress)

## 1. 响应内容 (Message Content)

插件执行完成后，返回的内容可以是字符串（会被自动转换为 `text` 类型的 **MessageElement**）或结构化的 **Message** 对象。

### 1.1 核心原则：元素化发送
- **动态构造**: 插件应优先利用 `MessageElement` 构造富文本内容。
- **XML 解析**: 插件可以返回包含 Satori 标签的字符串，框架会自动解析为 `MessageElement` AST。

## 2. 资源预处理 (Resource Pre-processing)
- 框架检索 **MessageElement** 序列中的 `img`, `audio`, `video`, `file`。
- 若 `src` 是本地路径，由适配器在发送前上传。

## 3. 消息句柄 (Message Handles)
发送成功后，适配器返回 `MessageHandle` 以支持后续的 `edit()` 和 `recall()` 操作。
