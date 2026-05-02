# 核心能力速览

本页是插件开发常用能力的快速索引。

## 1. 插件入口能力（Plugin）

在 `setup(plg)` 中可使用：

- `plg.plugin_id`
- `plg.data_dir`
- `plg.logger`
- `plg.on_command(...)`
- `plg.on_keyword(...)`
- `plg.on_route(...)`
- `plg.on_event(...)`
- `plg.tool(...)`
- `plg.register_adapter_factory(...)`（仅适配器插件场景）

## 2. 处理器运行态能力（MessageContext）

在命令、关键词和自定义消息路由处理器中常用：

- `bot.event`
- `bot.message`
- `bot.session`
- `bot.text`
- `bot.elements`
- `bot.user_id`
- `bot.command_match`
- `bot.has_permission(...)`
- `bot.stop()`

## 3. 发送与管理动作

- `await bot.send(...)`
- `await bot.reply(...)`
- `await bot.kick(...)`
- `await bot.mute(...)`
- `await bot.poke(...)`
- `await bot.approve_friend(...)`
- `await bot.get_member_list(...)`
- `await bot.set_group_name(...)`
- `await bot.delete_msg(...)`

## 4. 交互式输入

- `await bot.wait_for_input(prompt="...", timeout=60.0)`

用于多轮交互命令。

## 5. 数据与状态

- 插件持久化目录：`plg.data_dir`
- 会话状态：`bot.session.state`、`bot.session.plugin_data`

## 6. API 深入文档

- 详细签名见 [API 参考](./16_api_reference.md)
- 使用示例见 [示例代码](./17_examples.md)
