"""The scheduler: one thread, a tick every TICK_SECONDS

A tick, in order:

1. schedule   for every enabled job, a run per interval that ended since its
              cursor: all of them with catchup (CATCHUP_BATCH at most per
              tick), only the latest without (the others counted as skipped);
2. reconcile  sweep this stack's leftover services, then bring every running
              attempt up to date: succeeded, failed, lost, timed out;
3. launch     start pending attempts whose time has come, oldest interval
              first, within the job's max_active_runs and MAX_RUNNING.

Reconcile comes before launch, so a slot freed this tick is used this tick.
Each step starts from the database, so a failed tick (Docker unreachable) is
just retried by the next. Whatever happens the gauges are refreshed; the
heartbeat only moves when a tick completes -- the "scheduler down" alert
watches it, and docker_up tells whether Docker was the reason.
"""

import contextlib
import datetime
import fcntl
import logging
import threading
from typing import Dict, Iterator, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload, sessionmaker

from docker_jobs_backend.db import Attempt, AttemptState, Job, Run, RunState, RunType, utcnow
from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.runner import RunnerUnavailable
from docker_jobs_backend.schedule import due_intervals, latest_complete_interval
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics

__all__ = ["Scheduler", "SchedulerThread", "exclusive_lock"]

LOGGER = logging.getLogger(__name__)

# what launch and reconcile read from an attempt: its run, the run's job and attempts
_WITH_RUN = (
    selectinload(Attempt.run).selectinload(Run.job),
    selectinload(Attempt.run).selectinload(Run.attempts),
)


class Scheduler:
    """the three steps of a tick"""

    def __init__(
        self,
        session_factory: sessionmaker,
        lifecycle: Lifecycle,
        settings: Settings,
        metrics: Metrics,
    ):
        self.session_factory = session_factory
        self.lifecycle = lifecycle
        self.settings = settings
        self.metrics = metrics

    def tick(self, now: Optional[datetime.datetime] = None) -> None:
        """schedule, reconcile, launch; RunnerUnavailable propagates"""
        now = now or utcnow()
        reachable = True
        try:
            self.schedule(now)
            self.reconcile(now)
            self.launch(now)
        except RunnerUnavailable:
            reachable = False
            raise
        finally:
            self.metrics.docker_reachable(reachable)
            with self.session_factory() as session:
                self.metrics.refresh(session, now)
        self.metrics.tick(now)

    # --- 1. schedule ------------------------------------------------------------

    def schedule(self, now: datetime.datetime) -> None:
        """create the runs of the intervals that ended"""
        with self.session_factory() as session:
            jobs = session.scalars(
                select(Job).where(Job.enabled.is_(True)).order_by(Job.id)
            ).all()
            for job in jobs:
                name = job.name
                with _isolated(session, f"scheduling job {name}"):
                    self._schedule_job(session, job, now)
                    session.commit()

    def _schedule_job(self, session: Session, job: Job, now: datetime.datetime) -> None:
        # backpressure: at most CATCHUP_BATCH runs of a job wait at once, so a
        # long backlog never becomes a huge queue that every tick must read
        queued = session.scalar(
            select(func.count())
            .select_from(Run)
            .where(Run.job_id == job.id, Run.state == RunState.QUEUED)
        )
        room = self.settings.catchup_batch - queued
        if room <= 0:
            return

        due = due_intervals(
            job.schedule,
            job.start_date,
            job.end_date,
            job.schedule_cursor,
            now,
            room,
        )
        if not due:
            return

        if not job.catchup:
            upper = now if job.end_date is None else min(now, job.end_date)
            latest = latest_complete_interval(job.schedule, upper)
            # exact below a batch, a lower bound above
            skipped = len(due) - 1 if due[-1] == latest else len(due)
            if skipped:
                self.metrics.intervals_skipped(job.name, skipped)
            due = [latest]

        existing = {
            (start, end)
            for start, end in session.execute(
                select(Run.logical_date, Run.data_interval_end).where(
                    Run.job_id == job.id,
                    Run.run_type == RunType.SCHEDULED,
                    Run.logical_date >= due[0].start,
                    Run.logical_date <= due[-1].start,
                )
            )
        }
        for interval in due:
            if (interval.start, interval.end) not in existing:
                self.lifecycle.create_run(session, job, interval, RunType.SCHEDULED, now)

        job.schedule_cursor = due[-1].end

    # --- 2. reconcile -----------------------------------------------------------

    def reconcile(self, now: datetime.datetime) -> None:
        """sweep, then update every running attempt from its service"""
        with self.session_factory() as session:
            self.lifecycle.sweep(session)
            running = session.scalars(
                select(Attempt)
                .where(Attempt.state == AttemptState.RUNNING)
                .order_by(Attempt.id)
                .options(*_WITH_RUN)
            ).all()
            session.commit()

            for attempt in running:
                attempt_id = attempt.id
                with _isolated(session, f"reconciling attempt {attempt_id}"):
                    self.lifecycle.reconcile(session, attempt, now)

    # --- 3. launch --------------------------------------------------------------

    def launch(self, now: datetime.datetime) -> None:
        """start pending attempts, oldest interval first, within the limits"""
        with self.session_factory() as session:
            pending = session.scalars(
                select(Attempt)
                .join(Attempt.run)
                .join(Run.job)
                .where(
                    Attempt.state == AttemptState.PENDING,
                    Job.enabled.is_(True),
                    or_(Attempt.not_before.is_(None), Attempt.not_before <= now),
                )
                .order_by(Run.logical_date, Attempt.id)
                .options(*_WITH_RUN)
            ).all()
            running = session.scalar(
                select(func.count()).select_from(Attempt).where(Attempt.state == AttemptState.RUNNING)
            )
            active: Dict[int, int] = dict(
                session.execute(
                    select(Run.job_id, func.count())
                    .where(Run.state.in_([RunState.RUNNING, RunState.RETRYING]))
                    .group_by(Run.job_id)
                ).all()
            )
            session.commit()

            limit = self.settings.max_running
            for attempt in pending:
                if limit and running >= limit:
                    break
                run = attempt.run
                job = run.job
                # a retrying run holds its slot already; a queued one needs one
                holds_slot = run.state == RunState.RETRYING
                if not holds_slot and active.get(job.id, 0) >= job.max_active_runs:
                    continue

                attempt_id = attempt.id
                with _isolated(session, f"launching attempt {attempt_id}"):
                    self.lifecycle.launch(session, attempt, now)

                if attempt.state == AttemptState.RUNNING:
                    running += 1
                if not holds_slot and run.state in (RunState.RUNNING, RunState.RETRYING):
                    active[job.id] = active.get(job.id, 0) + 1


@contextlib.contextmanager
def _isolated(session: Session, what: str) -> Iterator[None]:
    """one job's or attempt's failure is logged and rolled back; the others go on

    RunnerUnavailable still ends the tick: Docker is down for everyone.
    """
    try:
        yield
    except RunnerUnavailable:
        raise
    except Exception:  # pylint: disable=broad-except
        session.rollback()
        LOGGER.exception("Scheduler: %s failed", what)


class SchedulerThread(threading.Thread):
    """ticks until stopped; a failed tick is logged, never fatal"""

    def __init__(self, scheduler: Scheduler, interval_seconds: float):
        super().__init__(name="scheduler", daemon=True)
        self.scheduler = scheduler
        self.interval_seconds = interval_seconds
        # not _stop: threading.Thread has a method of that name
        self._stopping = threading.Event()

    def run(self) -> None:
        while not self._stopping.is_set():
            try:
                self.scheduler.tick()
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Scheduler tick failed")
            self._stopping.wait(self.interval_seconds)

    def stop(self, timeout: float = 30.0) -> None:
        self._stopping.set()
        self.join(timeout)


@contextlib.contextmanager
def exclusive_lock(path: str) -> Iterator[None]:
    """hold an exclusive flock on path, or fail at once: one scheduler per database"""
    with open(path, "a", encoding="UTF-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"another scheduler holds {path}: run one backend process per database"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
