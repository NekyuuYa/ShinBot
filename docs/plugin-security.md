# Plugin Security and Sandboxing

## Current Limitations

ShinBot plugins run in the same process as the main application. This means:

- **No isolation**: Plugins have full access to the Python runtime
- **Shared resources**: Plugins can access files, network, memory, and environment
- **Crash risk**: A buggy plugin can crash the entire bot
- **Security risk**: Malicious plugins can access sensitive data

## Plugin Security Model

### What Plugins Can Do

Plugins registered via `Plugin` context have access to:

1. **Command registration** - Add new bot commands
2. **Keyword triggers** - Respond to specific text patterns
3. **Route handlers** - Custom message routing
4. **Event handlers** - React to system events
5. **Tool registration** - Add LLM-callable tools
6. **Adapter factories** - Add platform adapters
7. **Cron jobs** - Schedule recurring tasks

### What Plugins Should NOT Do

For security, plugins should avoid:

- Direct file system access outside their data directory
- Direct database access (use the provided APIs)
- Network requests to untrusted endpoints
- Modifying system configuration
- Importing untrusted third-party packages

## Best Practices for Plugin Users

### 1. Review Plugin Code

Before installing a plugin:

```bash
# Check the plugin source code
cat shinbot_plugin_example/__init__.py

# Look for suspicious patterns
grep -r "os.system\|subprocess\|eval\|exec" shinbot_plugin_example/
```

### 2. Use Trusted Sources

- Only install plugins from trusted repositories
- Check plugin author reputation
- Review recent commit history

### 3. Limit Plugin Permissions

In `data/config.toml`:

```toml
[[plugins]]
id = "untrusted_plugin"
module = "shinbot_plugin_untrusted"
enabled = false  # Disable by default
```

### 4. Monitor Plugin Behavior

- Check logs for unexpected activity
- Monitor resource usage
- Review plugin tool calls

## For Plugin Developers

### Security Checklist

- [ ] Validate all user inputs
- [ ] Use parameterized queries (not string concatenation)
- [ ] Sanitize file paths
- [ ] Limit network requests to trusted endpoints
- [ ] Handle errors gracefully
- [ ] Don't store sensitive data in plain text
- [ ] Use the provided Plugin API, not direct system access

### Safe Plugin Pattern

```python
from shinbot.core.plugins.context import Plugin

def setup(plg: Plugin) -> None:
    """Initialize plugin with minimal permissions."""
    
    # Register commands using the Plugin API
    @plg.on_command("example")
    async def handle_example(ctx):
        await ctx.send([MessageElement.text("Safe response")])
    
    # DON'T:
    # - Access files directly
    # - Make network requests to untrusted URLs
    # - Modify system configuration
    # - Import and execute untrusted code
```

## Future Improvements

Potential security enhancements:

1. **Permission System**: Explicit capability grants per plugin
2. **API Whitelist**: Restrict which system APIs plugins can access
3. **Resource Limits**: CPU/memory quotas per plugin
4. **Process Isolation**: Run plugins in separate processes
5. **Container Sandboxing**: Docker-based plugin isolation

## Reporting Security Issues

If you discover a security vulnerability in a plugin:

1. Do NOT publish the vulnerability publicly
2. Contact the plugin author directly
3. If the plugin is bundled with ShinBot, report to the ShinBot maintainers
4. Include steps to reproduce and potential impact

## References

- [OWASP Plugin Security Guidelines](https://owasp.org/www-project-application-security-verification-standard/)
- [Python Security Best Practices](https://docs.python.org/3/library/security.html)
