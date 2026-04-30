"""Model route member registry methods."""

from __future__ import annotations

from typing import Any

from shinbot.persistence.records import ModelRouteMemberRecord

from .base import Repository


class ModelRouteMemberRepositoryMixin(Repository):
    """Route-member CRUD methods for ModelRegistryRepository."""

    _ROUTE_MEMBER_JSON_FIELDS = {"conditions": ("conditions_json", {})}

    def _replace_route_members(
        self,
        conn: Any,
        route_id: str,
        members: list[ModelRouteMemberRecord],
    ) -> None:
        conn.execute("DELETE FROM model_route_members WHERE route_id = ?", (route_id,))
        for member in members:
            conn.execute(
                """
                INSERT INTO model_route_members (
                    route_id, model_id, priority, weight, conditions_json, timeout_override, enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    member.route_id,
                    member.model_id,
                    member.priority,
                    member.weight,
                    self.json_dumps(member.conditions),
                    member.timeout_override,
                    1 if member.enabled else 0,
                ),
            )

    def _rename_route_members(
        self,
        conn: Any,
        *,
        route_id: str,
        new_route_id: str,
    ) -> None:
        conn.execute(
            "UPDATE model_route_members SET route_id = ? WHERE route_id = ?",
            (new_route_id, route_id),
        )

    def list_route_members(self, route_id: str) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_route_members
                WHERE route_id = ?
                ORDER BY priority ASC, id ASC
                """,
                (route_id,),
            ).fetchall()
        return self.rows_to_dicts(
            rows,
            exclude=("id", "route_id"),
            bool_fields=("enabled",),
            json_fields=self._ROUTE_MEMBER_JSON_FIELDS,
        )
