# ShinBot 技术规范：统一事件与消息元素标准 (UnifiedEvent & MessageElement)

本规范基于 Satori 协议设计，在系统内部采用 **AST (抽象语法树)** 形式表示消息内容。

## 1. 核心理念：消息即元素序列
消息 (Message) 是一个包含语义标签的 **消息元素 (MessageElement)** 序列。

## 2. 标准消息元素集 (Standard MessageElements)

| 标签 (Tag) | 核心属性 (Attributes) | 说明 |
| :--- | :--- | :--- |
| **`text`** | `content` | 纯文本。内容存放在 `attrs["content"]`。 |
| **`at`** | `id`, `name`, `type` | 提及。`type` 可为 `all`, `here`。 |
| **`sharp`** | `id`, `name` | 提及频道或群组。 |
| **`img`** | `src`, `title`, `width`, `height` | 图片。支持 URL、Base64 或本地路径。 |
| **`emoji`** | `id`, `name` | 表情。 |
| **`quote`** | `id` | 引用回复。指向先前的消息 ID。 |
| **`audio`** | `src`, `duration` | 音频/语音。 |
| **`video`** | `src`, `duration` | 视频。 |
| **`file`** | `src`, `title`, `size` | 文件。 |
| **`br`** | (无) | 换行。 |

## 3. 交互性与容器元素 (Advanced Elements)

| 标签 (Tag) | 属性 | 说明 |
| :--- | :--- | :--- |
| **`message`** | `forward`, `id`, `name` | **容器**: 用于表示聊天记录或嵌套消息。 |
| **`sb:poke`** | `target`, `type` | **交互**: 戳一戳/抖动（由适配器补全）。 |
| **`sb:ark`** | `data` (JSON) | **容器**: 平台专有的 Ark/卡片消息（保留原始结构）。 |

## 4. 数据模型定义 (Data Model)

```python
class MessageElement:
    type: str                         # 元素类型
    attrs: Dict[str, Any]             # 属性集合
    children: List['MessageElement']  # 子元素列表 (支持嵌套)

class Message:
    elements: List[MessageElement]    # AST 数组
    
    def get_text(self) -> str:        # 提取拼接后的纯文本
    def to_xml(self) -> str:          # 序列化为 Satori XML (含转义处理)
```

## 5. 开发者双视图 API
- **`.elements`**: 获取 AST 数组，用于逻辑判断。
- **`.text`**: 获取 Satori XML 字符串，用于正则匹配或 LLM 提示词。
