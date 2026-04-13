"""ShinBot entry point based on BootController lifecycle."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import uvicorn

from shinbot.core.boot import BootController

logger = logging.getLogger("shinbot.main")


async def _run(config_path: str, log_level: str, api_host: str, api_port: int) -> None:
    controller = BootController(
        config_path=config_path,
        data_dir="data",
        log_level=log_level,
    )
    await controller.boot()

    api_app = controller.create_api_app()

    uv_cfg = uvicorn.Config(
        api_app,
        host=api_host,
        port=api_port,
        log_config=None,  # use our own logging config from BootController
        access_log=False,
    )
    server = uvicorn.Server(uv_cfg)

    logger.info("Management API starting on http://%s:%d", api_host, api_port)

    try:
        await server.serve()
    finally:
        await controller.shutdown()
        logger.info("Goodbye.")


def main() -> None:
    parser = argparse.ArgumentParser(description="ShinBot - modular bot framework")
    parser.add_argument(
        "--config",
        default="config.toml",
        metavar="FILE",
        help="Path to the TOML config file (default: config.toml)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        metavar="LEVEL",
        help="Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)",
    )
    parser.add_argument(
        "--api-host",
        default="0.0.0.0",
        metavar="HOST",
        help="Management API listen host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--api-port",
        default=3945,
        type=int,
        metavar="PORT",
        help="Management API listen port (default: 3945)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(_run(args.config, args.log_level, args.api_host, args.api_port))
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Fatal error in ShinBot")
        sys.exit(1)


if __name__ == "__main__":
    main()
