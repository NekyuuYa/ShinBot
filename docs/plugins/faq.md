# 常见问题（FAQ）

## Q1: 命令处理器到底该怎么写参数？

当前命令 dispatcher 调用方式是：

```python
await handler(bot, raw_args)
```

所以处理器应写成：

```python
async def handler(bot, args: str):
    ...
```

## Q2: 我还能用 `on_message` 或 `on_event("message-created")` 吗？

不能。消息事件已经从 EventBus 迁移到 RouteTable，`on_message` 已移除，`on_event("message-*")` 会报错。

按场景选择：

- 命令：`@plg.on_command(...)`
- 简单文本触发：`@plg.on_keyword(...)`
- 复杂结构化条件：`@plg.on_route(...)`

## Q3: `pattern` 会把正则分组自动注入函数参数吗？

不会。你需要通过 `bot.command_match.regex_match` 自己读取分组。

## Q4: 插件数据应该存哪里？

放在 `plg.data_dir`（`data/plugin_data/<plugin_id>/`）。

## Q5: `teardown` 能写成 `teardown(ctx)` 吗？

不建议。当前 `PluginManager` 按无参数调用 `teardown()`。

## Q6: 声明了 `__plugin_config_class__` 后，配置会自动注入到 `ctx` 吗？

不会。当前实现主要用于：

- 生成 schema
- 接收和验证配置更新
- 存储到 `plugin_configs`

运行时读取配置需要你自己实现。

## Q7: locale 不生效怎么办？

先检查：

1. `locales/<locale>.json` 是否是扁平字符串字典
2. 键名是否使用 `meta.*` / `config.*` 约定
3. 请求头是否带了 `Accept-Language`

## Q8: `register_adapter_factory` 报错 “no AdapterManager is available”

说明当前上下文没有注入适配器管理器。通常应在由应用正常加载的适配器插件里调用，而不是在独立测试上下文里直接调用。

## Q9: 插件重扫后没有出现？

检查：

1. `metadata.json` 是否存在
2. `entry` 是否是插件目录内的相对路径
3. 入口文件是否存在 `setup(plg)`
4. 日志里是否有导入异常

## Q10: 如何快速定位官方用法？

优先阅读这些源码：

- `shinbot/core/plugins/context.py`
- `shinbot/core/plugins/manager.py`
- `shinbot/core/dispatch/ingress.py`
- `shinbot/core/dispatch/routing.py`
- `shinbot/builtin_plugins/`
