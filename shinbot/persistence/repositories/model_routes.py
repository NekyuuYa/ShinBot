"""Model route registry methods."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from shinbot.persistence.records import ModelRouteRecord

from .base import Repository
from .model_route_members import ModelRouteMemberRepositoryMixin

if TYPE_CHECKING:
    from shinbot.persistence.records import ModelRouteMemberRecord


class ModelRouteRepositoryMixin(ModelRouteMemberRepositoryMixin, Repository):
    """Route CRUD methods for ModelRegistryRepository."""

    _ROUTE_JSON_FIELDS = {"metadata": ("metadata_json", {})}

    def upsert_route(
        self,
        record: ModelRouteRecord,
        *,
        members: list[ModelRouteMemberRecord] | None = None,
    ) -> None:
        payload = asdict(record)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO model_routes (
                    id, purpose, strategy, enabled, sticky_sessions, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    purpose = excluded.purpose,
                    strategy = excluded.strategy,
                    enabled = excluded.enabled,
                    sticky_sessions = excluded.sticky_sessions,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["id"],
                    payload["purpose"],
                    payload["strategy"],
                    1 if payload["enabled"] else 0,
                    1 if payload["sticky_sessions"] else 0,
                    self.json_dumps(payload["metadata"]),
                    payload["created_at"],
                    payload["updated_at"],
                ),
            )
            if members is not None:
                self._replace_route_members(conn, record.id, members)

    def list_routes(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM model_routes
                ORDER BY id ASC
                """
            ).fetchall()
        return [self._route_row_to_dict(row) for row in rows]

    def get_route(self, route_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM model_routes
                WHERE id = ?
                """,
                (route_id,),
            ).fetchone()
        if row is None:
            return None
        return self._route_row_to_dict(row)

    def delete_route(self, route_id: str) -> int:
        with self.connect() as conn:
            cursor = conn.execute("DELETE FROM model_routes WHERE id = ?", (route_id,))
            return int(cursor.rowcount)

    def rename_route(self, route_id: str, new_route_id: str) -> None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM model_routes WHERE id = ?",
                (route_id,),
            ).fetchone()
            if row is None:
                return
            conn.execute(
                """
                INSERT INTO model_routes (
                    id, purpose, strategy, enabled, sticky_sessions, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    new_route_id,
                    row["purpose"],
                    row["strategy"],
                    row["enabled"],
                    row["sticky_sessions"],
                    row["metadata_json"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
            self._rename_route_members(
                conn,
                route_id=route_id,
                new_route_id=new_route_id,
            )
            conn.execute("DELETE FROM model_routes WHERE id = ?", (route_id,))

    def _route_row_to_dict(self, row: Any) -> dict[str, Any]:
        return self.row_to_dict(
            row,
            bool_fields=("enabled", "sticky_sessions"),
            json_fields=self._ROUTE_JSON_FIELDS,
        )
