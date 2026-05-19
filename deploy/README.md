# ShinBot Deploy Hooks

This directory contains operator-owned helper scripts for runtime maintenance.

ShinBot does not run a built-in framework updater. The operator console command
`update framework` only executes the command explicitly configured in
`[admin].framework_update_command`, then requests a process restart when the
command exits successfully.

Example configuration:

```toml
[admin]
framework_update_dir = "/opt/shinbot/ShinBot"
framework_update_command = "./deploy/update-framework.sh"
framework_update_timeout_seconds = 300
framework_update_restart = true
```

Before enabling the command in production, review the script and adapt it to the
deployment manager that owns your checkout, virtual environment, service unit,
and dependency cache.
