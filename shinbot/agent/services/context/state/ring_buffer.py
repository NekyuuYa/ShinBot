"""Stable ring-based ID allocation helpers."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class StableRingIdAllocator:
    """Assign short stable IDs with bounded cardinality.

    Existing keys keep their assigned value until they are explicitly dropped
    or displaced by wrap-around when the capacity is exceeded.
    """

    capacity: int
    start: int = 1
    next_value: int = 1
    _key_to_value: dict[str, int] = field(default_factory=dict)
    _value_to_key: dict[int, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.capacity < 1:
            raise ValueError("capacity must be >= 1")
        if self.start < 0:
            raise ValueError("start must be >= 0")
        if self.next_value < self.start:
            self.next_value = self.start

    @property
    def stop(self) -> int:
        return self.start + self.capacity - 1

    def get(self, key: str) -> int | None:
        return self._key_to_value.get(key)

    def assign(self, key: str) -> int:
        existing = self._key_to_value.get(key)
        if existing is not None:
            return existing

        value = self.next_value
        displaced = self._value_to_key.get(value)
        if displaced is not None:
            del self._key_to_value[displaced]

        self._key_to_value[key] = value
        self._value_to_key[value] = key
        self.next_value = self.start if value >= self.stop else value + 1
        return value

    def drop(self, key: str) -> None:
        value = self._key_to_value.pop(key, None)
        if value is not None:
            self._value_to_key.pop(value, None)

    def to_dict(self) -> dict[str, object]:
        return {
            "capacity": self.capacity,
            "start": self.start,
            "next_value": self.next_value,
            "assignments": dict(self._key_to_value),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object] | None) -> StableRingIdAllocator:
        data = payload or {}
        allocator = cls(
            capacity=int(data.get("capacity", 1)),
            start=int(data.get("start", 1)),
            next_value=int(data.get("next_value", data.get("start", 1))),
        )
        assignments = data.get("assignments", {})
        if isinstance(assignments, dict):
            for key, value in assignments.items():
                if not isinstance(key, str):
                    continue
                int_value = int(value)
                allocator._key_to_value[key] = int_value
                allocator._value_to_key[int_value] = key
        return allocator
