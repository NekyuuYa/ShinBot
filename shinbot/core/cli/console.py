"""Interactive operator shell for live ShinBot runtime control."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

from prompt_toolkit import HTML, PromptSession, print_formatted_text
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.history import FileHistory, InMemoryHistory
from prompt_toolkit.patch_stdout import StdoutProxy
from prompt_toolkit.shortcuts import clear
from prompt_toolkit.styles import Style

from shinbot.core.cli.commands import CommandOutcome, OperatorCommandRouter
from shinbot.utils.logger import replace_console_handler

_STYLE = Style.from_dict(
    {
        "banner": "ansicyan bold",
        "muted": "ansibrightblack",
        "prompt": "ansigreen bold",
        "toolbar": "ansibrightblack",
        "result": "ansiwhite",
        "warning": "ansiyellow",
    }
)


class OperatorCliSession:
    """Line-oriented operator shell that runs beside the API server."""

    def __init__(
        self,
        *,
        boot: Any,
        api_host: str,
        api_port: int,
        server: Any,
    ) -> None:
        """Initialize the operator CLI session.

        Args:
            boot: The BootController instance holding runtime state.
            api_host: Hostname for the management API endpoint display.
            api_port: Port number for the management API endpoint display.
            server: The Uvicorn server instance for lifecycle control.
        """
        self._boot = boot
        self._server = server
        self._router = OperatorCommandRouter(
            boot=boot,
            api_host=api_host,
            api_port=api_port,
        )
        self._session: PromptSession[str] = PromptSession(
            history=_history_for_boot(boot),
            completer=WordCompleter(
                self._router.command_words,
                ignore_case=True,
                sentence=True,
            ),
            auto_suggest=AutoSuggestFromHistory(),
            bottom_toolbar=self._bottom_toolbar,
            complete_while_typing=True,
            style=_STYLE,
        )

    async def run(self) -> None:
        """Run the shell until the operator exits or the server stops."""

        with StdoutProxy(raw=True) as log_stream:
            replace_console_handler(stream=log_stream, use_color=True)
            self._print_banner()
            try:
                await self._read_loop()
            finally:
                replace_console_handler(use_color=True)

    async def _read_loop(self) -> None:
        while not self._server.should_exit:
            try:
                line = await self._session.prompt_async(
                    [("class:prompt", "shinbot"), ("class:muted", "> ")],
                )
            except KeyboardInterrupt:
                print_formatted_text(
                    HTML("<warning>Use 'exit' or Ctrl-D to stop ShinBot.</warning>"),
                    style=_STYLE,
                )
                continue
            except EOFError:
                print_formatted_text("Stopping ShinBot.")
                self._server.should_exit = True
                return

            outcome = await self._router.execute(line)
            self._apply_outcome(outcome)

    def _apply_outcome(self, outcome: CommandOutcome) -> None:
        if outcome.clear_screen:
            clear()

        if outcome.message:
            print_formatted_text(outcome.message, style=_STYLE)

        if outcome.exit_requested:
            self._server.should_exit = True

    def _print_banner(self) -> None:
        print_formatted_text(
            HTML(
                "\n"
                "<banner>ShinBot Operator</banner>\n"
                "<muted>Type 'help' for commands. Logs stay live while you work.</muted>\n"
            ),
            style=_STYLE,
        )

    def _bottom_toolbar(self) -> HTML:
        return HTML(
            "<toolbar>status | instances | plugins | restart | loglevel | exit</toolbar>"
        )


async def run_operator_cli(*, boot: Any, api_host: str, api_port: int, server: Any) -> None:
    """Run the operator shell until the user requests shutdown."""
    session = OperatorCliSession(
        boot=boot,
        api_host=api_host,
        api_port=api_port,
        server=server,
    )
    await session.run()


async def serve_with_operator_cli(*, boot: Any, api_host: str, api_port: int, server: Any) -> None:
    """Run uvicorn and the operator shell concurrently."""
    server_task = asyncio.create_task(server.serve())
    cli_task = asyncio.create_task(
        run_operator_cli(
            boot=boot,
            api_host=api_host,
            api_port=api_port,
            server=server,
        )
    )

    done, pending = await asyncio.wait(
        {server_task, cli_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    if cli_task in done and not server_task.done():
        server.should_exit = True
        await server_task
    elif server_task in done and not cli_task.done():
        cli_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await cli_task

    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    for task in done:
        await task


def _history_for_boot(boot: Any) -> Any:
    data_dir = getattr(boot, "data_dir", None)
    if data_dir is None:
        return InMemoryHistory()
    history_path = Path(data_dir) / "operator_history"
    try:
        history_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return InMemoryHistory()
    return FileHistory(str(history_path))
