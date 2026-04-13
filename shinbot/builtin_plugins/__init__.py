"""ShinBot built-in plugins package.

Built-in plugins are first-party, high-priority plugins shipped with the
ShinBot core distribution. They are loaded before any user plugins from
``data/plugins/``.

Plugin layout (naming is mandatory):
  shinbot/builtin_plugins/
    shinbot_adapter_<name>/   ← adapter drivers (protocol translators)
      metadata.json
      __init__.py             ← must expose setup(ctx: PluginContext)
    shinbot_plugin_<name>/    ← built-in logic plugins
      metadata.json
      __init__.py
"""
