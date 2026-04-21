"""Interactive operator console for live ShinBot runtime control."""

from __future__ import annotations

import asyncio
import contextlib
import io
from typing import Any

from prompt_toolkit import Application
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.document import Document
from prompt_toolkit.formatted_text import ANSI
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.dimension import D
from prompt_toolkit.widgets import TextArea

from shinbot.core.cli.commands import CommandOutcome, OperatorCommandRouter
from shinbot.utils.logger import replace_console_handler


class _UiLogStream(io.TextIOBase):
    """File-like stream that appends logging output into the log pane."""

    def __init__(self, session: OperatorCliSession) -> None:
        self._session = session

    @property
    def encoding(self) -> str:
        return "utf-8"

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        self._session.append_log(text)
        return len(text)

    def flush(self) -> None:
        return None


class OperatorCliSession:
    """Run an interactive terminal session alongside the API server."""

    def __init__(
        self,
        *,
        boot: Any,
        api_host: str,
        api_port: int,
        server: Any,
    ) -> None:
        self._boot = boot
        self._server = server
        self._router = OperatorCommandRouter(
            boot=boot,
            api_host=api_host,
            api_port=api_port,
        )
        self._history = InMemoryHistory()
        self._completer = WordCompleter(
            self._router.command_words,
            ignore_case=True,
            sentence=True,
        )
        self._log_lines: list[str] = []
        self._max_log_lines = 2000
        self._command_lock = asyncio.Lock()
        self._log_stream = _UiLogStream(self)
        self._log_control = FormattedTextControl(self._formatted_log_text, focusable=False)
        self._log_view = Window(
            content=self._log_control,
            wrap_lines=True,
            always_hide_cursor=True,
            right_margins=[],
        )
        self._input = TextArea(
            prompt="shinbot> ",
            multiline=True,
            wrap_lines=True,
            height=D(min=1, max=4),
            history=self._history,
            completer=self._completer,
            auto_suggest=AutoSuggestFromHistory(),
        )
        self._status_bar = Window(
            content=FormattedTextControl(self._status_fragments),
            height=1,
        )
        self._separator = Window(char="─", height=1)
        self._layout = Layout(
            HSplit(
                [
                    self._log_view,
                    self._separator,
                    self._status_bar,
                    self._input,
                ]
            ),
            focused_element=self._input,
        )
        self._app = Application(
            layout=self._layout,
            key_bindings=self._build_key_bindings(),
            full_screen=True,
        )

    async def run(self) -> None:
        replace_console_handler(stream=self._log_stream, use_color=True)
        self.append_log("Operator CLI attached. Type 'help' for commands.")
        await self._app.run_async()

    def append_log(self, text: str) -> None:
        normalized = text.replace("\r\n", "\n").replace("\r", "\n")
        parts = normalized.split("\n")

        if normalized.endswith("\n"):
            trailing_blank = True
            parts = parts[:-1]
        else:
            trailing_blank = False

        for part in parts:
            if part:
                self._log_lines.append(part)
        if trailing_blank:
            self._log_lines.append("")

        if len(self._log_lines) > self._max_log_lines:
            self._log_lines = self._log_lines[-self._max_log_lines :]

        if self._app.is_running:
            self._log_view.vertical_scroll = max(0, len(self._log_lines) - 1)
            self._app.invalidate()

    def _formatted_log_text(self) -> ANSI:
        return ANSI("\n".join(self._log_lines))

    def _build_key_bindings(self) -> KeyBindings:
        bindings = KeyBindings()

        @bindings.add("enter")
        def _submit(event: Any) -> None:
            if event.app.current_buffer is not self._input.buffer:
                return
            event.app.create_background_task(self._on_submit())

        @bindings.add("c-c")
        def _interrupt(_event: Any) -> None:
            self.append_log("Use 'exit' to stop ShinBot.")

        @bindings.add("c-l")
        def _clear(_event: Any) -> None:
            self._clear_logs()

        return bindings

    async def _on_submit(self) -> None:
        if self._command_lock.locked():
            return

        async with self._command_lock:
            line = self._input.buffer.text
            if not line.strip():
                self._input.buffer.document = Document("", 0)
                return

            self._history.append_string(line)
            self._input.buffer.document = Document("", 0)
            outcome = await self._router.execute(line)
            self._apply_outcome(outcome)

    def _apply_outcome(self, outcome: CommandOutcome) -> None:
        if outcome.clear_screen:
            self._clear_logs()

        if outcome.message:
            self.append_log(outcome.message)

        if outcome.exit_requested:
            self._server.should_exit = True
            self._app.exit()

    def _clear_logs(self) -> None:
        self._log_lines.clear()
        self._log_view.vertical_scroll = 0
        if self._app.is_running:
            self._app.invalidate()

    def _status_fragments(self) -> list[tuple[str, str]]:
        return [
            ("class:status", " Logs above | Commands below | Ctrl-L clear | Ctrl-C hint "),
        ]


async def run_operator_cli(*, boot: Any, api_host: str, api_port: int, server: Any) -> None:
    """Run the operator console until the user requests shutdown."""
    session = OperatorCliSession(
        boot=boot,
        api_host=api_host,
        api_port=api_port,
        server=server,
    )
    await session.run()


async def serve_with_operator_cli(*, boot: Any, api_host: str, api_port: int, server: Any) -> None:
    """Run uvicorn and the operator console concurrently."""
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
