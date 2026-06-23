"""Plugin-level cron scheduling manager.

Wraps APScheduler (optional dependency) to provide a lightweight cron
interface for plugins.  The scheduler is created lazily on the first
registered job.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class PluginCronManager:
    """Manages cron-scheduled jobs registered by plugins.

    Jobs are tracked per plugin ID so they can be bulk-removed on
    plugin unload.  The underlying APScheduler instance is created
    lazily on first use.

    Args:
        timezone: Default timezone for jobs that don't specify one.
    """

    def __init__(self, timezone: str | None = None) -> None:
        self._timezone = timezone
        self._scheduler: Any | None = None
        self._jobs: dict[str, list[str]] = {}  # plugin_id -> [job_ids]

    def _ensure_scheduler(self) -> None:
        """Lazily create and start the APScheduler instance."""
        if self._scheduler is not None:
            return
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler
        except ImportError as exc:
            raise RuntimeError(
                "APScheduler is required for plugin cron scheduling. "
                "Install it with: pip install apscheduler"
            ) from exc
        self._scheduler = AsyncIOScheduler(timezone=self._timezone)
        self._scheduler.start()
        logger.info("PluginCronManager: scheduler started")

    def add_cron_job(
        self,
        plugin_id: str,
        func: Callable[..., Any],
        cron_expr: str,
        *,
        timezone: str | None = None,
        job_id: str | None = None,
        description: str = "",
    ) -> str:
        """Register a cron job for a plugin.

        Args:
            plugin_id:   Owning plugin identifier.
            func:        Async or sync callable to invoke on schedule.
            cron_expr:   Standard 5-field cron expression
                         (minute hour day month day_of_week).
            timezone:    Optional timezone override for this job.
            job_id:      Explicit job identifier.  Auto-generated if omitted.
            description: Human-readable description for logging.

        Returns:
            The registered job ID.

        Raises:
            ValueError: If *cron_expr* is not a valid 5-field expression.
        """
        self._ensure_scheduler()

        parts = cron_expr.strip().split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression {cron_expr!r}: expected 5 fields "
                "(minute hour day month day_of_week), got {len(parts)}"
            )

        from apscheduler.triggers.cron import CronTrigger

        minute, hour, day, month, day_of_week = parts
        trigger = CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            timezone=timezone,
        )

        if job_id is None:
            seq = len(self._jobs.get(plugin_id, []))
            job_id = f"{plugin_id}_cron_{seq}"

        self._scheduler.add_job(func, trigger, id=job_id, replace_existing=True)
        self._jobs.setdefault(plugin_id, []).append(job_id)
        logger.info(
            "PluginCronManager: registered job %r for plugin %r (expr=%r%s)",
            job_id,
            plugin_id,
            cron_expr,
            f", desc={description!r}" if description else "",
        )
        return job_id

    def remove_jobs(self, plugin_id: str) -> int:
        """Remove all cron jobs owned by *plugin_id*.

        Returns:
            The number of jobs removed.
        """
        removed = 0
        for job_id in self._jobs.pop(plugin_id, []):
            try:
                if self._scheduler is not None:
                    self._scheduler.remove_job(job_id)
                removed += 1
            except Exception:
                logger.debug(
                    "PluginCronManager: failed to remove job %r (may already be gone)",
                    job_id,
                )
        if removed:
            logger.info(
                "PluginCronManager: removed %d job(s) for plugin %r",
                removed,
                plugin_id,
            )
        return removed

    def shutdown(self, wait: bool = False) -> None:
        """Shut down the scheduler.

        Args:
            wait: If ``True``, wait for currently executing jobs to finish.
        """
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=wait)
            self._scheduler = None
            self._jobs.clear()
            logger.info("PluginCronManager: scheduler shut down")
