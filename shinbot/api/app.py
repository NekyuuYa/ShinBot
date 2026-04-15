"""FastAPI application factory for the ShinBot Management Control Plane.

Implements the communication contract defined in 16_api_communication_spec.md:
  - Unified Envelope response format for all HTTP endpoints
  - JWT-based authentication on all /api/v1/* routes (except /auth/login)
  - WebSocket streams: /ws/logs (real-time log push), /ws/system (status broadcast)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from shinbot.api.auth import AuthConfig
from shinbot.api.models import EC, Envelope, ErrorBody
from shinbot.api.routers import auth as auth_router
from shinbot.api.routers import instances as instances_router
from shinbot.api.routers import model_runtime as model_runtime_router
from shinbot.api.routers import plugins as plugins_router
from shinbot.api.ws_manager import (
    install_log_handler,
    log_broadcaster,
    log_manager,
    status_manager,
)
from shinbot.utils.logger import register_log_handler_installer

# Push the WebSocket log handler installer into utils so that
# setup_logging() can call it without importing shinbot.api.
# This keeps the dependency arrow pointing downward (api → utils).
register_log_handler_installer(install_log_handler)

if TYPE_CHECKING:
    from shinbot.core.application.app import ShinBot
    from shinbot.core.application.boot import BootController

logger = logging.getLogger(__name__)


def create_api_app(bot: ShinBot, boot: BootController) -> FastAPI:
    """Create and configure the ShinBot management API FastAPI application.

    Args:
        bot: The running ShinBot core instance.
        boot: The BootController providing config access and persistence.

    Returns:
        Configured FastAPI application ready for uvicorn.
    """

    # ── Lifespan (startup / shutdown hooks) ──────────────────────────

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        install_log_handler()
        broadcaster_task = asyncio.create_task(log_broadcaster())
        logger.info("Management API ready")
        yield
        broadcaster_task.cancel()
        try:
            await broadcaster_task
        except asyncio.CancelledError:
            pass

    # ── App creation ─────────────────────────────────────────────────

    app = FastAPI(
        title="ShinBot Management API",
        version="0.1.1",
        docs_url="/api/docs",
        redoc_url=None,
        openapi_url="/api/openapi.json",
        lifespan=lifespan,
    )

    # ── State injection ───────────────────────────────────────────────

    app.state.bot = bot
    app.state.boot_controller = boot
    app.state.auth_config = AuthConfig(boot.config, boot.data_dir)

    # ── CORS ─────────────────────────────────────────────────────────

    admin_cfg = boot.config.get("admin", {})
    cors_origins: list[str] = admin_cfg.get("cors_origins", ["*"])
    cors_allow_credentials = bool(admin_cfg.get("cors_allow_credentials", False))
    if cors_allow_credentials and cors_origins == ["*"]:
        # Browsers reject wildcard ACAO when credentials are allowed.
        logger.warning(
            "cors_allow_credentials=true with wildcard origins is invalid; disabling credentials"
        )
        cors_allow_credentials = False
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Exception handlers (Envelope wrapping) ───────────────────────

    @app.exception_handler(Exception)
    async def _generic_exc_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled API exception: %s %s", request.method, request.url.path)
        body = Envelope(
            success=False,
            error=ErrorBody(code=EC.INTERNAL_ERROR, message="An internal server error occurred"),
            timestamp=int(time.time()),
        )
        return JSONResponse(status_code=500, content=body.model_dump())

    # FastAPI raises HTTPException internally; wrap it in Envelope too.
    from fastapi import HTTPException

    @app.exception_handler(HTTPException)
    async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail
        if isinstance(detail, dict) and "code" in detail:
            error = ErrorBody(code=detail["code"], message=detail.get("message", ""))
        else:
            error = ErrorBody(code="HTTP_ERROR", message=str(detail))
        body = Envelope(success=False, error=error, timestamp=int(time.time()))
        return JSONResponse(status_code=exc.status_code, content=body.model_dump())

    # ── API routers ───────────────────────────────────────────────────

    api_prefix = "/api/v1"
    app.include_router(auth_router.router, prefix=api_prefix)
    app.include_router(instances_router.router, prefix=api_prefix)
    app.include_router(model_runtime_router.router, prefix=api_prefix)
    app.include_router(plugins_router.router, prefix=api_prefix)

    # ── WebSocket: /ws/logs ───────────────────────────────────────────
    # The log_broadcaster() task fans out records queued by AsyncLogHandler
    # to all connected clients. Each handler here just keeps the socket alive.

    @app.websocket("/ws/logs")
    async def ws_logs(websocket: WebSocket) -> None:
        await log_manager.connect(websocket)
        try:
            while True:
                # Receive to detect client disconnect; we ignore the data.
                await websocket.receive_text()
        except WebSocketDisconnect:
            log_manager.disconnect(websocket)
        except Exception:
            log_manager.disconnect(websocket)

    # ── WebSocket: /ws/status ─────────────────────────────────────────

    @app.websocket("/ws/status")
    async def ws_status(websocket: WebSocket) -> None:
        await status_manager.connect(websocket)
        try:
            while True:
                payload = _build_system_status(bot, boot)
                # 包装为标准 Envelope
                await websocket.send_json(
                    {"success": True, "data": payload, "timestamp": int(time.time())}
                )
                await asyncio.sleep(3.0)
        except WebSocketDisconnect:
            status_manager.disconnect(websocket)
        except Exception:
            status_manager.disconnect(websocket)

    # ── Static dashboard hosting + SPA fallback ──────────────────────

    dashboard_dist = _resolve_dashboard_dist(boot)
    if dashboard_dist is not None:
        assets_dir = dashboard_dist / "assets"
        if assets_dir.is_dir():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="dashboard-assets")

        @app.get("/", include_in_schema=False)
        async def dashboard_root() -> FileResponse:
            assert boot.dashboard_index_file is not None
            return FileResponse(boot.dashboard_index_file)

        @app.get("/{full_path:path}", include_in_schema=False)
        async def dashboard_spa_fallback(full_path: str) -> FileResponse:
            # Keep API and WS paths isolated from SPA fallback.
            if full_path.startswith("api/") or full_path.startswith("ws/"):
                from fastapi import HTTPException

                raise HTTPException(status_code=404, detail="Not Found")

            assert boot.dashboard_dist_dir is not None
            assert boot.dashboard_index_file is not None

            if full_path:
                candidate = (boot.dashboard_dist_dir / full_path).resolve()
                if candidate.is_file() and candidate.is_relative_to(boot.dashboard_dist_dir):
                    return FileResponse(candidate)

            # SPA history fallback: always return index.html.
            return FileResponse(boot.dashboard_index_file)
    else:
        logger.warning("Dashboard dist not available; only API/WS routes are active")

    return app


# ── System status snapshot ────────────────────────────────────────────


def _build_system_status(bot: ShinBot, boot: BootController | None = None) -> dict[str, Any]:
    cpu = 0.0
    mem_mb = 0.0
    try:
        import psutil

        # 使用当前进程的内存快照
        process = psutil.Process(os.getpid())
        cpu = process.cpu_percent(interval=None)
        # 转换为 MB 并保留两位小数
        mem_mb = round(process.memory_info().rss / (1024 * 1024), 2)
    except Exception:
        pass

    mgr = bot.adapter_manager
    configured_instances = boot.config.get("instances", []) if boot is not None else []
    instances: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for item in configured_instances:
        instance_id = item.get("id")
        if not instance_id:
            continue
        seen_ids.add(instance_id)
        instances.append(
            {
                "id": instance_id,
                "running": mgr.is_running(instance_id),
            }
        )

    for adapter in mgr.all_instances:
        if adapter.instance_id in seen_ids:
            continue
        instances.append(
            {
                "id": adapter.instance_id,
                "running": mgr.is_running(adapter.instance_id),
            }
        )

    total_instances = len(instances)
    running_instances = sum(1 for instance in instances if instance["running"])

    return {
        "totalInstances": total_instances,
        "runningInstances": running_instances,
        "stoppedInstances": total_instances - running_instances,
        "totalPlugins": len(bot.plugin_manager.all_plugins),
        "enabledPlugins": len(bot.plugin_manager.all_plugins),
        "cpuUsage": cpu,
        "memoryUsage": mem_mb,  # 现在是 MB 单位
        "online": True,
        "instances": instances,
        "timestamp": int(time.time()),
    }


def _resolve_dashboard_dist(boot: BootController) -> Path | None:
    """Resolve dashboard dist folder from boot-initialized static config."""
    dist_dir = boot.dashboard_dist_dir
    index_file = boot.dashboard_index_file
    if dist_dir is None or index_file is None:
        return None
    if not dist_dir.is_dir() or not index_file.is_file():
        return None
    return dist_dir
