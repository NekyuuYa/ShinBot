"""Lossless SQLite value projection for corruption-safe migrations."""

from __future__ import annotations

import base64
import hashlib
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass

RawSQLiteScalar = bytes | int | float | None


class RawSQLiteProjectionError(ValueError):
    """Raised when a raw projection no longer matches its SQLite storage class."""


class RawSQLiteValueTruncatedError(RawSQLiteProjectionError):
    """Raised when a bounded projection cannot be decoded losslessly."""


@dataclass(slots=True, frozen=True)
class RawSQLiteValue:
    """One SQLite value before TEXT decoding or affinity coercion."""

    storage_class: str
    raw: RawSQLiteScalar
    byte_length: int | None = None
    sha256: str | None = None
    projection_truncated: bool = False

    def decode(self) -> object:
        """Decode valid UTF-8 TEXT while preserving every other storage class."""

        if self.projection_truncated:
            raise RawSQLiteValueTruncatedError("raw SQLite value exceeds read limit")
        if self.storage_class == "text":
            if not isinstance(self.raw, bytes):
                raise RawSQLiteProjectionError("TEXT projection is not bytes")
            return self.raw.decode("utf-8", errors="strict")
        if self.storage_class == "blob":
            if not isinstance(self.raw, bytes):
                raise RawSQLiteProjectionError("BLOB projection is not bytes")
            return self.raw
        if self.storage_class == "integer":
            if not isinstance(self.raw, int) or isinstance(self.raw, bool):
                raise RawSQLiteProjectionError("INTEGER projection is not int")
            return self.raw
        if self.storage_class == "real":
            if not isinstance(self.raw, float):
                raise RawSQLiteProjectionError("REAL projection is not float")
            return self.raw
        if self.storage_class == "null":
            if self.raw is not None:
                raise RawSQLiteProjectionError("NULL projection has a value")
            return None
        raise RawSQLiteProjectionError(
            f"unsupported SQLite storage class: {self.storage_class!r}"
        )

    def evidence(self, *, prefix_bytes: int = 192) -> dict[str, object]:
        """Return bounded, digest-addressed evidence for one persisted value."""

        if prefix_bytes < 0:
            raise ValueError("prefix_bytes must not be negative")
        encoded, encoding = self._evidence_bytes()
        prefix = encoded[:prefix_bytes]
        byte_length = len(encoded)
        digest = hashlib.sha256(encoded).hexdigest()
        if self.projection_truncated:
            if self.byte_length is None or self.byte_length < len(encoded):
                raise RawSQLiteProjectionError(
                    "truncated projection is missing its full byte length"
                )
            if self.sha256 is None:
                raise RawSQLiteProjectionError(
                    "truncated projection is missing its streamed digest"
                )
            byte_length = self.byte_length
            digest = self.sha256
        elif self.byte_length is not None and self.byte_length != len(encoded):
            raise RawSQLiteProjectionError(
                "complete projection byte length does not match its value"
            )
        return {
            "byte_length": byte_length,
            "encoding": encoding,
            "prefix_base64": base64.b64encode(prefix).decode("ascii"),
            "sha256": digest,
            "storage_class": self.storage_class,
            "truncated": len(prefix) < byte_length,
        }

    @property
    def logical_byte_length(self) -> int:
        """Return the complete evidence byte length without materializing the value."""

        if self.byte_length is not None:
            return self.byte_length
        encoded, _ = self._evidence_bytes()
        return len(encoded)

    def _evidence_bytes(self) -> tuple[bytes, str]:
        if self.storage_class == "text":
            if not isinstance(self.raw, bytes):
                raise RawSQLiteProjectionError("TEXT projection is not bytes")
            return self.raw, "utf-8-bytes"
        if self.storage_class == "blob":
            if not isinstance(self.raw, bytes):
                raise RawSQLiteProjectionError("BLOB projection is not bytes")
            return self.raw, "raw-bytes"
        if self.storage_class == "integer":
            if not isinstance(self.raw, int) or isinstance(self.raw, bool):
                raise RawSQLiteProjectionError("INTEGER projection is not int")
            return str(self.raw).encode("ascii"), "ascii-decimal"
        if self.storage_class == "real":
            if not isinstance(self.raw, float):
                raise RawSQLiteProjectionError("REAL projection is not float")
            return self.raw.hex().encode("ascii"), "ascii-float-hex"
        if self.storage_class == "null":
            if self.raw is not None:
                raise RawSQLiteProjectionError("NULL projection has a value")
            return b"", "null"
        raise RawSQLiteProjectionError(
            f"unsupported SQLite storage class: {self.storage_class!r}"
        )


def raw_sqlite_projection(
    table_alias: str,
    columns: tuple[str, ...],
    *,
    output_prefix: str = "",
) -> str:
    """Build a SELECT projection that never asks sqlite3 to decode TEXT."""

    projected: list[str] = []
    for column in columns:
        source = f"{table_alias}.{column}"
        output = f"{output_prefix}{column}"
        projected.extend(
            (
                f"typeof({source}) AS {output}__storage_class",
                (
                    f"CASE typeof({source}) "
                    f"WHEN 'text' THEN CAST({source} AS BLOB) "
                    f"WHEN 'blob' THEN CAST({source} AS BLOB) "
                    f"ELSE {source} END AS {output}__raw"
                ),
            )
        )
    return ",\n".join(projected)


def bounded_raw_sqlite_projection(
    table_alias: str,
    columns: tuple[str, ...],
    *,
    byte_limits: Mapping[str, int],
    output_prefix: str = "",
    prefix_bytes: int = 192,
) -> str:
    """Build a raw projection with a hard per-field Python allocation bound."""

    if prefix_bytes < 0:
        raise ValueError("prefix_bytes must not be negative")
    projected: list[str] = []
    for column in columns:
        try:
            byte_limit = int(byte_limits[column])
        except KeyError as exc:
            raise ValueError(f"missing byte limit for {column!r}") from exc
        if byte_limit < prefix_bytes:
            raise ValueError(
                f"byte limit for {column!r} must be at least prefix_bytes"
            )
        source = f"{table_alias}.{column}"
        output = f"{output_prefix}{column}"
        storage_class = f"typeof({source})"
        blob_value = f"CAST({source} AS BLOB)"
        blob_length = f"length({blob_value})"
        projected.extend(
            (
                f"{storage_class} AS {output}__storage_class",
                (
                    f"CASE {storage_class} "
                    f"WHEN 'text' THEN {blob_length} "
                    f"WHEN 'blob' THEN {blob_length} "
                    f"ELSE NULL END AS {output}__byte_length"
                ),
                (
                    f"CASE {storage_class} "
                    f"WHEN 'text' THEN CASE WHEN {blob_length} <= {byte_limit} "
                    f"THEN {blob_value} ELSE substr({blob_value}, 1, {prefix_bytes}) END "
                    f"WHEN 'blob' THEN CASE WHEN {blob_length} <= {byte_limit} "
                    f"THEN {blob_value} ELSE substr({blob_value}, 1, {prefix_bytes}) END "
                    f"ELSE {source} END AS {output}__raw"
                ),
                (
                    f"CASE {storage_class} "
                    f"WHEN 'text' THEN {blob_length} > {byte_limit} "
                    f"WHEN 'blob' THEN {blob_length} > {byte_limit} "
                    f"ELSE 0 END AS {output}__truncated"
                ),
            )
        )
    return ",\n".join(projected)


def raw_sqlite_values(
    row: sqlite3.Row,
    columns: tuple[str, ...],
    *,
    output_prefix: str = "",
) -> dict[str, RawSQLiteValue]:
    """Materialize values produced by :func:`raw_sqlite_projection`."""

    row_keys = set(row.keys())
    values: dict[str, RawSQLiteValue] = {}
    for column in columns:
        output = f"{output_prefix}{column}"
        byte_length_key = f"{output}__byte_length"
        truncated_key = f"{output}__truncated"
        byte_length_value = (
            row[byte_length_key] if byte_length_key in row_keys else None
        )
        values[column] = RawSQLiteValue(
            storage_class=str(row[f"{output}__storage_class"]),
            raw=row[f"{output}__raw"],
            byte_length=(
                int(byte_length_value) if byte_length_value is not None else None
            ),
            projection_truncated=(
                bool(row[truncated_key]) if truncated_key in row_keys else False
            ),
        )
    return values


def complete_truncated_raw_sqlite_value(
    value: RawSQLiteValue,
    *,
    chunk_reader: Callable[[int, int], object],
    chunk_bytes: int = 65_536,
) -> RawSQLiteValue:
    """Stream the digest of one bounded projection without loading it at once."""

    if not value.projection_truncated:
        return value
    if value.storage_class not in {"text", "blob"}:
        raise RawSQLiteProjectionError(
            "only TEXT and BLOB projections may be truncated"
        )
    if value.byte_length is None or value.byte_length < 0:
        raise RawSQLiteProjectionError("truncated projection has no byte length")
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be positive")
    digest = hashlib.sha256()
    consumed = 0
    while consumed < value.byte_length:
        requested = min(chunk_bytes, value.byte_length - consumed)
        chunk = chunk_reader(consumed + 1, requested)
        if not isinstance(chunk, bytes) or len(chunk) != requested:
            raise RawSQLiteProjectionError(
                "streamed SQLite value changed while evidence was collected"
            )
        digest.update(chunk)
        consumed += len(chunk)
    return RawSQLiteValue(
        storage_class=value.storage_class,
        raw=value.raw,
        byte_length=value.byte_length,
        sha256=digest.hexdigest(),
        projection_truncated=True,
    )


def decode_raw_sqlite_values(
    values: dict[str, RawSQLiteValue],
) -> tuple[dict[str, object], tuple[str, ...]]:
    """Decode a projection and return stable per-field corruption violations."""

    decoded: dict[str, object] = {}
    violations: list[str] = []
    for field_name, raw_value in values.items():
        try:
            decoded[field_name] = raw_value.decode()
        except RawSQLiteValueTruncatedError:
            decoded[field_name] = raw_value.raw
            violations.append(f"{field_name}_too_large")
        except UnicodeDecodeError:
            decoded[field_name] = raw_value.raw
            violations.append(f"{field_name}_invalid_utf8")
        except RawSQLiteProjectionError:
            decoded[field_name] = raw_value.raw
            violations.append(f"{field_name}_projection_invalid")
    return decoded, tuple(violations)


__all__ = [
    "RawSQLiteProjectionError",
    "RawSQLiteScalar",
    "RawSQLiteValue",
    "RawSQLiteValueTruncatedError",
    "bounded_raw_sqlite_projection",
    "complete_truncated_raw_sqlite_value",
    "decode_raw_sqlite_values",
    "raw_sqlite_projection",
    "raw_sqlite_values",
]
