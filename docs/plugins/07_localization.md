# 本地化

ShinBot 插件本地化目前主要用于 **插件列表展示** 和 **配置 schema 文本翻译**。

## 1. 两种 locale 来源

按优先级：

1. 插件目录下 `locales/*.json`
2. 模块变量 `__plugin_locales__`

如果 `locales/` 目录存在，系统优先使用文件，不再回退到 `__plugin_locales__`。

## 2. 文件格式

每个 locale 文件是扁平键值对象，且 key/value 都应为字符串。

示例 `locales/zh-CN.json`：

```json
{
  "meta.name": "演示插件",
  "meta.description": "演示描述",
  "config.title": "演示配置",
  "config.description": "插件配置说明",
  "config.fields.api_key.label": "接口密钥",
  "config.fields.api_key.description": "调用外部 API 使用"
}
```

## 3. 当前生效的翻译键

### 插件元信息

- `meta.name`
- `meta.description`

### 配置 schema

- `config.title`
- `config.description`
- `config.fields.<field_path>.label`
- `config.fields.<field_path>.description`

`field_path` 支持嵌套路径，例如 `retry.timeout`。

## 4. 语言协商规则

系统会根据 `Accept-Language` 选择翻译：

1. 先找完全匹配（如 `zh-CN`）
2. 再找基础语言匹配（如 `zh`）
3. 默认回退列表包含 `zh-CN` 与 `en-US`

当前实现是按请求头顺序做匹配，不会解析 `q=` 权重优先级。

## 5. 当前不应假设的能力

下面这些行为在当前实现里并不存在自动支持：

- `bot.send("messages.xxx")` 自动翻译
- 命令描述自动国际化
- 任意业务文本自动按用户语言切换

如果你需要业务消息国际化，请在插件内部自行实现字典和选择逻辑。

下一步：阅读 [生命周期](./10_lifecycle.md)。
