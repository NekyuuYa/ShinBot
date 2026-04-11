# ShinBot 名词解释 (Glossary)

## 1. 实例 (Instance / instance_id)
- **定义**: 每一个独立的 Bot 账号或连接端点。
- **作用**: 用于会话隔离和路由。用户在配置中指定其使用的 **适配器类型**。

## 2. 适配器 (Adapter)
- **定义**: **协议翻译器插件**。负责将特定平台（如 OneBot）的原始消息转换为 ShinBot 内部的 `UnifiedEvent` (AST Elements)。
- **存在形式**: 它不是核心组件，而是由驱动插件向框架注册的一种 **“接入能力”**。

## 3. 会话 (Session / session_id)
- **定义**: 消息流转的逻辑单位。由适配器提供的 `channel_id` 与 `guild_id` 结合生成的复合 ID。

## 4. 消息元素 (MessageElement)
- **定义**: 消息内容的最小语义单元。ShinBot 内部统一采用 Satori 规范的 AST 表示。

## 5. 消息工作流 (Message Workflow)
- **定义**: 消息在系统内的非线性处理流程。由拦截器、分发器和业务处理器组成的异步执行链。
