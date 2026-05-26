"""ShinBot entry point based on BootController lifecycle."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import uvicorn

from shinbot.core.application.boot import BootController
from shinbot.core.application.runtime_control import ProcessExitCode, RuntimeControl
from shinbot.core.cli import serve_with_operator_cli
from shinbot.utils.logger import get_logger

logger = get_logger("shinbot.main", source="main", color="bright_cyan")


async def _run(
    config_path: str,
    data_dir: str,
    log_level: str,
    api_host: str,
    api_port: int,
    operator_cli: bool | None,
) -> int:
    runtime_control = RuntimeControl()
    controller = BootController(
        config_path=config_path,
        data_dir=data_dir,
        log_level=log_level,
    )
    await controller.boot()

    api_app = controller.create_api_app(runtime_control)

    uv_cfg = uvicorn.Config(
        api_app,
        host=api_host,
        port=api_port,
        log_config=None,  # use our own logging config from BootController
        access_log=False,
    )
    server = uvicorn.Server(uv_cfg)

    async def _watch_restart_requests() -> None:
        request = await runtime_control.wait_for_restart()
        logger.warning(
            "Restart requested: reason=%s requested_by=%s source=%s",
            request.reason,
            request.requested_by or "-",
            request.source or "-",
        )
        server.should_exit = True

    restart_task = asyncio.create_task(_watch_restart_requests())

    logger.info("Management API starting on http://%s:%d", api_host, api_port)

    attach_operator_cli = (
        operator_cli
        if operator_cli is not None
        else sys.stdin.isatty() and sys.stdout.isatty()
    )

    try:
        if attach_operator_cli:
            await serve_with_operator_cli(
                boot=controller,
                api_host=api_host,
                api_port=api_port,
                server=server,
            )
        else:
            await server.serve()
    finally:
        restart_task.cancel()
        try:
            await restart_task
        except asyncio.CancelledError:
            pass
        await controller.shutdown()
        logger.info("Goodbye.")

    return runtime_control.exit_code()


def main() -> None:
    parser = argparse.ArgumentParser(description="ShinBot - modular bot framework")
    parser.add_argument(
        "--config",
        default="config.toml",
        metavar="FILE",
        help="Path to the TOML config file (default: config.toml)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        metavar="DIR",
        help="Path to the ShinBot data directory (default: data)",
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
    cli_group = parser.add_mutually_exclusive_group()
    cli_group.add_argument(
        "--operator-cli",
        action="store_true",
        dest="operator_cli",
        default=None,
        help="Attach the interactive operator shell",
    )
    cli_group.add_argument(
        "--no-operator-cli",
        action="store_false",
        dest="operator_cli",
        help="Run only the API server without the operator shell",
    )
    args = parser.parse_args()

    try:
        exit_code = asyncio.run(
            _run(
                args.config,
                args.data_dir,
                args.log_level,
                args.api_host,
                args.api_port,
                args.operator_cli,
            )
        )
        if exit_code in {
            int(ProcessExitCode.RESTART_MANUAL),
            int(ProcessExitCode.RESTART_UPDATE),
        }:
            logger.info("Restarting ShinBot process.")
            os.execv(sys.executable, [sys.executable, *sys.argv])
        if exit_code:
            sys.exit(exit_code)
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Fatal error in ShinBot")
        sys.exit(1)


if __name__ == "__main__":
    main()
