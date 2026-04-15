"""Runtime manager for tool filtering and execution."""

from __future__ import annotations

import asyncio
import inspect
from datetime import UTC, datetime
from typing import Any

from shinbot.core.security.audit import AuditLogger
from shinbot.core.security.permission import PermissionEngine

from .registry import ToolRegistry
from .schema import (
    ToolCallRequest,
    ToolCallResult,
    ToolDefinition,
    ToolExecutionContext,
    ToolVisibility,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ToolManager:
    """Coordinates visible-tool export and runtime tool execution."""

    def __init__(
        self,
        registry: ToolRegistry,
        *,
        permission_engine: PermissionEngine | None = None,
        audit_logger: AuditLogger | None = None,
    ) -> None:
        self._registry = registry
        self._permission_engine = permission_engine
        self._audit_logger = audit_logger

    def list_visible_tools(
        self,
        *,
        caller: str,
        instance_id: str = "",
        session_id: str = "",
        user_id: str = "",
        include_private: bool = False,
        tags: set[str] | None = None,
    ) -> list[ToolDefinition]:
        definitions = self._registry.list_tools(enabled=True, tags=tags)
        visible: list[ToolDefinition] = []
        for definition in definitions:
            if definition.visibility == ToolVisibility.PRIVATE and not include_private:
                continue
            if not self._is_allowed(definition, instance_id, session_id, user_id):
                continue
            visible.append(definition)
        return visible

    def export_model_tools(
        self,
        *,
        caller: str,
        instance_id: str = "",
        session_id: str = "",
        user_id: str = "",
        tags: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        return [
            {
                "type": "function",
                "function": {
                    "name": definition.name,
                    "description": definition.description,
                    "parameters": definition.input_schema,
                },
            }
            for definition in self.list_visible_tools(
                caller=caller,
                instance_id=instance_id,
                session_id=session_id,
                user_id=user_id,
                tags=tags,
            )
            if definition.visibility != ToolVisibility.PRIVATE
        ]

    async def execute(self, call: ToolCallRequest) -> ToolCallResult:
        started = _utc_now()
        definition = self._registry.get_tool_by_name(call.tool_name)
        if definition is None:
            return self._error_result(
                call, started, "tool_not_found", f"Unknown tool: {call.tool_name}"
            )
        if not definition.enabled:
            return self._error_result(
                call,
                started,
                "tool_disabled",
                f"Tool {call.tool_name!r} is disabled",
                definition=definition,
            )
        if not self._is_allowed(definition, call.instance_id, call.session_id, call.user_id):
            return self._error_result(
                call,
                started,
                "permission_denied",
                f"Permission denied for tool {call.tool_name!r}",
                definition=definition,
            )
        try:
            self._validate_arguments(definition.input_schema, call.arguments)
        except ValueError as exc:
            return self._error_result(
                call,
                started,
                "invalid_arguments",
                str(exc),
                definition=definition,
            )
        if call.dry_run:
            return self._success_result(
                call,
                started,
                {"dry_run": True},
                definition=definition,
            )

        runtime = ToolExecutionContext(
            caller=call.caller,
            instance_id=call.instance_id,
            session_id=call.session_id,
            user_id=call.user_id,
            trace_id=call.trace_id,
            run_id=call.run_id,
            metadata=dict(call.metadata),
        )
        try:
            output = await asyncio.wait_for(
                self._invoke_handler(definition.handler, call.arguments, runtime),
                timeout=definition.timeout_seconds,
            )
        except TimeoutError:
            return self._error_result(
                call,
                started,
                "tool_timeout",
                f"Tool {call.tool_name!r} timed out",
                definition=definition,
            )
        except Exception as exc:
            return self._error_result(
                call,
                started,
                "tool_execution_failed",
                str(exc),
                definition=definition,
            )
        return self._success_result(call, started, output, definition=definition)

    async def _invoke_handler(
        self,
        handler: Any,
        arguments: dict[str, Any],
        runtime: ToolExecutionContext,
    ) -> Any:
        result = handler(arguments, runtime)
        if inspect.isawaitable(result):
            return await result
        return result

    def _is_allowed(
        self,
        definition: ToolDefinition,
        instance_id: str,
        session_id: str,
        user_id: str,
    ) -> bool:
        if not definition.permission:
            return True
        if self._permission_engine is None:
            return False
        if not instance_id or not session_id or not user_id:
            return False
        return self._permission_engine.check(
            definition.permission,
            instance_id,
            session_id,
            user_id,
        )

    def _success_result(
        self,
        call: ToolCallRequest,
        started: datetime,
        output: Any,
        *,
        definition: ToolDefinition,
    ) -> ToolCallResult:
        finished = _utc_now()
        result = ToolCallResult(
            tool_name=call.tool_name,
            success=True,
            output=output,
            started_at=started.isoformat(),
            finished_at=finished.isoformat(),
            latency_ms=(finished - started).total_seconds() * 1000,
            metadata={"owner_id": definition.owner_id, "owner_type": definition.owner_type},
        )
        result.audit_id = self._log_audit(call, result, definition)
        return result

    def _error_result(
        self,
        call: ToolCallRequest,
        started: datetime,
        error_code: str,
        error_message: str,
        *,
        definition: ToolDefinition | None = None,
    ) -> ToolCallResult:
        finished = _utc_now()
        metadata = {}
        if definition is not None:
            metadata = {"owner_id": definition.owner_id, "owner_type": definition.owner_type}
        result = ToolCallResult(
            tool_name=call.tool_name,
            success=False,
            error_code=error_code,
            error_message=error_message,
            started_at=started.isoformat(),
            finished_at=finished.isoformat(),
            latency_ms=(finished - started).total_seconds() * 1000,
            metadata=metadata,
        )
        result.audit_id = self._log_audit(call, result, definition)
        return result

    def _log_audit(
        self,
        call: ToolCallRequest,
        result: ToolCallResult,
        definition: ToolDefinition | None,
    ) -> str:
        if self._audit_logger is None:
            return ""
        entry = self._audit_logger.log_command(
            command_name=f"tool:{call.tool_name}",
            plugin_id=definition.owner_id if definition is not None else "",
            user_id=call.user_id,
            session_id=call.session_id,
            instance_id=call.instance_id,
            permission_required=definition.permission if definition is not None else "",
            permission_granted=result.success or result.error_code != "permission_denied",
            execution_time_ms=result.latency_ms,
            success=result.success,
            error=result.error_message,
            metadata={
                "entry_type": "tool",
                "caller": call.caller,
                "trace_id": call.trace_id,
                "run_id": call.run_id,
                "arguments": call.arguments,
            },
        )
        return entry.timestamp

    def _validate_arguments(self, schema: dict[str, Any], arguments: dict[str, Any]) -> None:
        self._validate_value(schema, arguments, path="$")

    def _validate_value(self, schema: dict[str, Any], value: Any, *, path: str) -> None:
        expected_type = schema.get("type")
        if expected_type == "object":
            if not isinstance(value, dict):
                raise ValueError(f"{path} must be an object")
            properties = schema.get("properties") or {}
            required = schema.get("required") or []
            for key in required:
                if key not in value:
                    raise ValueError(f"{path}.{key} is required")
            for key, item in value.items():
                child_schema = properties.get(key)
                if isinstance(child_schema, dict):
                    self._validate_value(child_schema, item, path=f"{path}.{key}")
            return
        if expected_type == "array":
            if not isinstance(value, list):
                raise ValueError(f"{path} must be an array")
            item_schema = schema.get("items")
            if isinstance(item_schema, dict):
                for index, item in enumerate(value):
                    self._validate_value(item_schema, item, path=f"{path}[{index}]")
            return
        if expected_type == "string" and not isinstance(value, str):
            raise ValueError(f"{path} must be a string")
        if expected_type == "integer" and (not isinstance(value, int) or isinstance(value, bool)):
            raise ValueError(f"{path} must be an integer")
        if expected_type == "number" and (
            not isinstance(value, (int, float)) or isinstance(value, bool)
        ):
            raise ValueError(f"{path} must be a number")
        if expected_type == "boolean" and not isinstance(value, bool):
            raise ValueError(f"{path} must be a boolean")
