"""Admin API endpoints — now superseded by the /api/v1/ router suite.

This module is preserved for backward compatibility.
The canonical plugin and instance management endpoints are:

  GET    /api/v1/plugins
  POST   /api/v1/plugins/reload        (rescan)
  PATCH  /api/v1/plugins/{id}/config   (hot-reload)
  GET    /api/v1/instances
  POST   /api/v1/instances
  PATCH  /api/v1/instances/{id}
  POST   /api/v1/instances/{id}/control

All endpoints require JWT Bearer authentication (see POST /api/v1/auth/login).
"""

from __future__ import annotations

from shinbot.api.routers.plugins import router as _plugin_router

# Re-export the plugin router so any code that previously did
#   from shinbot.api.admin import create_admin_router
# can migrate at their own pace.
plugin_router = _plugin_router
