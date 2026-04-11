# ShinBot 规范：特化操作注册表 (Internal API Registry)

本文档定义了官方适配器对各 IM 平台非标功能（Satori 标准之外）的统一封装规范。

## 1. 封装原则

### 1.1 命名空间化
非标 API 必须遵循 `internal.{platform_type}.{action}` 的格式。
- 示例: `internal.qq.poke`, `internal.discord.create_thread`。

### 1.2 事件补全 (Event Enrichment)
官方适配器有责任修复原生协议上报不全的问题：
- **合并转发**: 适配器应自动处理 `resid` 换取内容的过程，确保核心接收到的是完整的 `<message forward>` 元素树。
- **Poke**: 将所有平台的“戳一戳/抖动”统一映射为 `UnifiedEvent` 中的 `<sb:poke />` 元素。

## 2. 官方适配器：QQ 生态封装 (基于 OneBot v11)

### 2.1 主动 API (Actions)
| 方法名 | 参数 | 描述 |
| :--- | :--- | :--- |
| `internal.qq.poke` | `user_id`, `group_id` | 发送戳一戳。 |
| `internal.qq.set_group_card` | `group_id`, `user_id`, `card` | 修改群名片。 |
| `internal.qq.send_like` | `user_id`, `times` | 点赞。 |

### 2.2 接收元素 (Elements)
| 标签 | 属性 | 描述 |
| :--- | :--- | :--- |
| `<sb:poke />` | `target`, `type` | 接收到的戳一戳。 |
| `<sb:ark />` | `data` (JSON) | QQ 专有的 Ark 气泡消息。 |

## 3. 官方适配器：标准 Satori 增强

对于遵循标准 Satori 协议的接入端，ShinBot 适配器将作为“协议补全层”，在握手后通过 `login.features` 探测平台能力，并自动映射其 `internal` 路由。
