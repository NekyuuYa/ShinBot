"""Process-local observation of legacy Agent session task shutdown.

This module deliberately does not coordinate ingress, durable ownership, or
external side effects. It only composes task reports from legacy owners that
live in the current asyncio process.
"""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Protocol

from shinbot.agent.runtime.task_manager import (
    AgentTaskQuiescence,
    AgentTaskQuiescenceStatus,
)


class SessionTaskQuiescenceOwner(Protocol):
    """Own a process-local set of asyncio tasks for one Agent session."""

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> AgentTaskQuiescence:
        """Cancel and observe the owner's known task snapshot."""


class ProfileSessionTaskQuiescer(Protocol):
    """Observe all known legacy task owners within one Agent profile."""

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalTaskQuiescence:
        """Cancel and observe the profile's known session-task snapshot."""


@dataclass(slots=True, frozen=True)
class LegacySessionLocalTaskObservation:
    """One owner report captured during a local legacy-session drain."""

    owner_name: str
    task_quiescence: AgentTaskQuiescence | None = None
    error_code: str | None = None

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether this owner observed no surviving known local task."""

        report = self.task_quiescence
        return (
            self.error_code is None
            and report is not None
            and report.status
            in {
                AgentTaskQuiescenceStatus.NO_LOCAL_TASKS,
                AgentTaskQuiescenceStatus.QUIESCENT,
            }
        )


@dataclass(slots=True, frozen=True)
class LegacySessionLocalTaskQuiescence:
    """Aggregate process-local task observation for one legacy session.

    ``locally_confirmed_quiescent`` is intentionally narrow: it means every
    configured owner finished the exact task objects it observed in this
    process. It is not a durable receipt and cannot prove that ingress, another
    process, an untracked coroutine, or an external model/tool effect stopped.
    """

    session_id: str
    observations: tuple[LegacySessionLocalTaskObservation, ...]

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether every configured local owner reported a clean snapshot."""

        return all(
            observation.locally_confirmed_quiescent
            for observation in self.observations
        )

    @property
    def remaining_task_names(self) -> tuple[str, ...]:
        """Return surviving task names qualified by their local owner."""

        names = [
            f"{observation.owner_name}:{task_name}"
            for observation in self.observations
            if observation.task_quiescence is not None
            for task_name in observation.task_quiescence.remaining_task_names
        ]
        return tuple(sorted(names))

    @property
    def failed_owner_names(self) -> tuple[str, ...]:
        """Return owners whose local drain operation raised unexpectedly."""

        return tuple(
            observation.owner_name
            for observation in self.observations
            if observation.error_code is not None
        )


@dataclass(slots=True, frozen=True)
class LegacySessionProfileTaskObservation:
    """One per-profile local task report for a shared legacy base session."""

    profile_id: str
    task_quiescence: LegacySessionLocalTaskQuiescence | None = None
    error_code: str | None = None

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether this profile produced a clean local task report."""

        return (
            self.error_code is None
            and self.task_quiescence is not None
            and self.task_quiescence.locally_confirmed_quiescent
        )


@dataclass(slots=True, frozen=True)
class LegacySessionAllProfilesTaskQuiescence:
    """Aggregate every configured legacy profile for one base session.

    Legacy signal serialization and ingress locking use the base session id,
    while task ownership is profile-local. A useful local drain must therefore
    observe every profile that could have created work for that shared base
    session, including the default fallback profile.
    """

    session_id: str
    observations: tuple[LegacySessionProfileTaskObservation, ...]

    @property
    def locally_confirmed_quiescent(self) -> bool:
        """Return whether every configured profile reported a clean snapshot."""

        return all(
            observation.locally_confirmed_quiescent
            for observation in self.observations
        )

    @property
    def remaining_task_names(self) -> tuple[str, ...]:
        """Return surviving local task names qualified by profile and owner."""

        names = [
            f"{observation.profile_id}:{task_name}"
            for observation in self.observations
            if observation.task_quiescence is not None
            for task_name in observation.task_quiescence.remaining_task_names
        ]
        return tuple(sorted(names))

    @property
    def failed_profile_ids(self) -> tuple[str, ...]:
        """Return profiles whose local drain operation raised unexpectedly."""

        return tuple(
            observation.profile_id
            for observation in self.observations
            if observation.error_code is not None
        )


class LegacySessionLocalTaskQuiescer:
    """Compose current-process task drains for one legacy Agent session.

    The quiescer intentionally excludes the global review-due poller, scheduler
    state, ingress, adapter processes, session locks, and external effects. A
    future cutover controller must add those independent protections before any
    durable ownership transition can be authorized.
    """

    def __init__(
        self,
        *,
        review_dispatcher: SessionTaskQuiescenceOwner,
        active_chat_workflow: SessionTaskQuiescenceOwner,
        active_chat_timer: SessionTaskQuiescenceOwner,
        review_coordinator: SessionTaskQuiescenceOwner | None = None,
        review_due_timer: SessionTaskQuiescenceOwner | None = None,
    ) -> None:
        self._owners: tuple[tuple[str, SessionTaskQuiescenceOwner], ...] = tuple(
            owner
            for owner in (
                ("active_chat_timer", active_chat_timer),
                ("review_due_timer", review_due_timer),
                ("review_dispatcher", review_dispatcher),
                (
                    "review_coordinator",
                    review_coordinator,
                ),
                ("active_chat_workflow", active_chat_workflow),
            )
            if owner[1] is not None
        )

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionLocalTaskQuiescence:
        """Cancel and observe each configured legacy owner in this process.

        ``timeout_seconds`` is a total budget shared by all owner observations.
        A timeout or an owner error remains visible in the returned report while
        later owners are still asked to stop their known tasks.
        """

        normalized_session_id = _normalize_session_id(session_id)
        timeout = _normalize_timeout(timeout_seconds)
        deadline = (
            None
            if timeout is None
            else asyncio.get_running_loop().time() + timeout
        )
        observations: list[LegacySessionLocalTaskObservation] = []
        for owner_name, owner in self._owners:
            remaining_timeout = (
                None
                if deadline is None
                else max(0.0, deadline - asyncio.get_running_loop().time())
            )
            try:
                task_quiescence = await owner.quiesce_session_tasks(
                    normalized_session_id,
                    timeout_seconds=remaining_timeout,
                )
            except Exception as exc:
                observations.append(
                    LegacySessionLocalTaskObservation(
                        owner_name=owner_name,
                        error_code=type(exc).__name__,
                    )
                )
                continue
            observations.append(
                LegacySessionLocalTaskObservation(
                    owner_name=owner_name,
                    task_quiescence=task_quiescence,
                )
            )
        return LegacySessionLocalTaskQuiescence(
            session_id=normalized_session_id,
            observations=tuple(observations),
        )


class LegacySessionAllProfilesTaskQuiescer:
    """Compose local task drains for all legacy Agent profiles of one session.

    The class stays process-local and unmounted. It does not freeze future
    signal admission itself; callers must use the separate signal-admission
    boundary before interpreting this fixed task snapshot as meaningful.
    """

    def __init__(
        self,
        profile_quiescers: tuple[tuple[str, ProfileSessionTaskQuiescer], ...],
    ) -> None:
        normalized: list[tuple[str, ProfileSessionTaskQuiescer]] = []
        seen_profile_ids: set[str] = set()
        for profile_id, quiescer in profile_quiescers:
            normalized_profile_id = str(profile_id or "").strip()
            if not normalized_profile_id:
                raise ValueError("profile_id must not be empty")
            if normalized_profile_id in seen_profile_ids:
                raise ValueError("profile_quiescers cannot repeat a profile_id")
            seen_profile_ids.add(normalized_profile_id)
            normalized.append((normalized_profile_id, quiescer))
        if not normalized:
            raise ValueError("profile_quiescers must not be empty")
        self._profile_quiescers = tuple(sorted(normalized, key=lambda item: item[0]))

    async def quiesce_session_tasks(
        self,
        session_id: str,
        *,
        timeout_seconds: float | None = None,
    ) -> LegacySessionAllProfilesTaskQuiescence:
        """Cancel and observe all configured profile snapshots for one session.

        The timeout is a total budget. Later profiles are still asked to
        quiesce after an earlier timeout or error so a caller receives a full
        local report instead of a partial success.
        """

        normalized_session_id = _normalize_session_id(session_id)
        timeout = _normalize_timeout(timeout_seconds)
        deadline = (
            None
            if timeout is None
            else asyncio.get_running_loop().time() + timeout
        )
        observations: list[LegacySessionProfileTaskObservation] = []
        for profile_id, quiescer in self._profile_quiescers:
            remaining_timeout = (
                None
                if deadline is None
                else max(0.0, deadline - asyncio.get_running_loop().time())
            )
            try:
                task_quiescence = await quiescer.quiesce_session_tasks(
                    normalized_session_id,
                    timeout_seconds=remaining_timeout,
                )
            except Exception as exc:
                observations.append(
                    LegacySessionProfileTaskObservation(
                        profile_id=profile_id,
                        error_code=type(exc).__name__,
                    )
                )
                continue
            observations.append(
                LegacySessionProfileTaskObservation(
                    profile_id=profile_id,
                    task_quiescence=task_quiescence,
                )
            )
        return LegacySessionAllProfilesTaskQuiescence(
            session_id=normalized_session_id,
            observations=tuple(observations),
        )


def _normalize_session_id(session_id: str) -> str:
    """Validate and normalize one local session identifier."""

    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        raise ValueError("session_id must not be empty")
    return normalized_session_id


def _normalize_timeout(timeout_seconds: float | None) -> float | None:
    """Validate an optional total local-observation timeout."""

    if timeout_seconds is None:
        return None
    timeout = float(timeout_seconds)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_seconds must be finite and non-negative")
    return timeout


__all__ = [
    "LegacySessionAllProfilesTaskQuiescence",
    "LegacySessionAllProfilesTaskQuiescer",
    "LegacySessionLocalTaskObservation",
    "LegacySessionLocalTaskQuiescence",
    "LegacySessionLocalTaskQuiescer",
    "LegacySessionProfileTaskObservation",
    "ProfileSessionTaskQuiescer",
    "SessionTaskQuiescenceOwner",
]
