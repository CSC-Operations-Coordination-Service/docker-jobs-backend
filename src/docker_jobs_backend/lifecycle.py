"""What happens to a run: created, launched, finished, retried, rerun, cancelled

The scheduler thread and the API both go through here, so a run follows one
set of rules whoever moves it:

    Run      queued --launch--> running --success--> success
                                   |------failure--> retrying --launch--> running
                                   +------failure--> failed      (no try left)
             queued/running/retrying --cancel--> cancelled
             success/failed/cancelled --rerun--> queued          (fresh tries)

    Attempt  pending -> running -> success | failed | timeout | lost
             pending/running -> cancelled

Concurrency. The API and the scheduler share the database, one transaction
at a time (BEGIN IMMEDIATE, db.py). None is held while Docker answers:
launch() and reconcile() commit first, then re-read the attempt (_still)
before writing what Docker said -- the API may have cancelled the run, or
deleted its job, meanwhile.

Order of effects. An outcome is committed before its service is removed: if
the process dies in between, the sweep removes the service later, whereas the
other order would lose the outcome. A service is created only once its name
and token are committed: if the process dies in between, the next launch of
the same attempt adopts it (SwarmRunner.launch).
"""

import datetime
import logging
from typing import Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

from docker_jobs_backend.db import (
    TERMINAL_RUN_STATES,
    Attempt,
    AttemptState,
    Job,
    Reason,
    Run,
    RunState,
    RunType,
)
from docker_jobs_backend.runner import (
    ATTEMPT_LABEL,
    COMPLETE,
    FAILED,
    GONE,
    JOB_LABEL,
    PENDING,
    RUN_LABEL,
    RUNNING,
    LaunchError,
    LaunchSpec,
    Runner,
    RunnerUnavailable,
    TaskStatus,
)
from docker_jobs_backend.schedule import Interval
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics
from docker_jobs_backend.templating import TemplateProblem, build_context, render

__all__ = ["Conflict", "Lifecycle", "loki_selector", "service_name"]

LOGGER = logging.getLogger(__name__)


class Conflict(Exception):
    """the request contradicts the run's state: the API answers 409"""


def service_name(stack: str, job_name: str, run_id: int, try_number: int) -> str:
    """<stack>_<job>_<run>_<try>: at most 10+1+32+1+10+1+2 = 57 characters, Swarm allows 63"""
    return f"{stack}_{job_name}_{run_id}_{try_number}"


def loki_selector(stack: str, name: str) -> str:
    """the LogQL selector of one attempt's logs, from the labels the loki driver sets"""
    return f'{{swarm_stack="{stack}", swarm_service="{name}"}}'


class Lifecycle:
    """the rules, and the runner and metrics they drive"""

    def __init__(self, runner: Runner, metrics: Metrics, settings: Settings):
        self.runner = runner
        self.metrics = metrics
        self.settings = settings

    # --- new work: the caller commits -----------------------------------------

    def create_run(
        self,
        session: Session,
        job: Job,
        interval: Interval,
        run_type: str,
        now: datetime.datetime,
        requested_by: str = "",
        note: str = "",
    ) -> Run:
        """a queued run of job over interval, its first attempt pending"""
        run = Run(
            job=job,
            logical_date=interval.start,
            data_interval_end=interval.end,
            run_type=run_type,
            state=RunState.QUEUED,
            max_tries=job.retries + 1,
            created_at=now,
            requested_by=requested_by,
            note=note,
        )
        run.attempts.append(
            Attempt(
                try_number=1,
                reason=Reason.SCHEDULED if run_type == RunType.SCHEDULED else Reason.MANUAL,
                state=AttemptState.PENDING,
                requested_by=requested_by,
                note=note,
                created_at=now,
            )
        )
        session.add(run)
        self.metrics.run_created(job.name, run_type)
        return run

    def rerun(
        self, run: Run, now: datetime.datetime, requested_by: str = "", note: str = ""
    ) -> Attempt:
        """queue an ended run again, with the job's retries afresh"""
        if run.state not in TERMINAL_RUN_STATES:
            raise Conflict(f"run {run.id} is {run.state}: only an ended run can be rerun")
        if not run.job.enabled:
            raise Conflict(f"job {run.job.name} is disabled: enable it to rerun its runs")

        tried = run.try_number
        attempt = Attempt(
            try_number=tried + 1,
            reason=Reason.RERUN,
            state=AttemptState.PENDING,
            requested_by=requested_by,
            note=note,
            created_at=now,
        )
        run.attempts.append(attempt)
        run.max_tries = tried + 1 + run.job.retries
        run.state = RunState.QUEUED
        run.finished_at = None
        return attempt

    # --- Docker: these commit -------------------------------------------------

    def launch(self, session: Session, attempt: Attempt, now: datetime.datetime) -> None:
        """start a pending attempt's service; RunnerUnavailable leaves it pending"""
        if not self._still(session, attempt, AttemptState.PENDING):
            session.commit()
            return

        run = attempt.run
        job = run.job
        context = build_context(
            Interval(run.logical_date, run.data_interval_end),
            run.id,
            attempt.try_number,
            job.name,
            job.params,
        )
        try:
            rendered = render(job.entrypoint, job.command, job.env, context)
        except TemplateProblem as exc:
            self._finish(attempt, AttemptState.FAILED, now, error=f"template {exc}")
            session.commit()
            return

        attempt.service_name = service_name(self.settings.run_stack, job.name, run.id, attempt.try_number)
        attempt.rendered_entrypoint = rendered.entrypoint
        attempt.rendered_command = rendered.command
        session.commit()  # the name and the token, before the service exists

        spec = LaunchSpec(
            service_name=attempt.service_name,
            image=job.image,
            entrypoint=rendered.entrypoint,
            command=rendered.command,
            env=rendered.env,
            labels={JOB_LABEL: job.name, RUN_LABEL: str(run.id), ATTEMPT_LABEL: attempt.token},
            networks=job.networks,
            constraints=job.constraints,
            secrets=job.secrets,
            configs=job.configs,
            mounts=job.mounts,
            cpu_limit=job.cpu_limit,
            memory_limit_mb=job.memory_limit_mb,
        )
        try:
            service_id = self.runner.launch(spec)
        except LaunchError as exc:
            if self._still(session, attempt, AttemptState.PENDING):
                self._finish(attempt, AttemptState.FAILED, now, error=str(exc))
            session.commit()
            return

        if not self._still(session, attempt, AttemptState.PENDING):
            # cancelled, or its job deleted, while Docker was creating it
            session.commit()
            self._remove(service_id)
            return

        attempt.service_id = service_id
        attempt.state = AttemptState.RUNNING
        attempt.launched_at = now
        run.state = RunState.RUNNING
        if run.started_at is None:
            run.started_at = now
        session.commit()
        self.metrics.attempt_started(job.name, attempt.reason)

    def reconcile(self, session: Session, attempt: Attempt, now: datetime.datetime) -> None:
        """bring a running attempt up to date with its service"""
        status = self.runner.status(attempt.service_id)

        if not self._still(session, attempt, AttemptState.RUNNING):
            session.commit()
            return

        if status.node_id:
            attempt.node_id = status.node_id

        outcome = self._outcome(attempt, status, now)
        if outcome is None:
            if status.state == RUNNING and attempt.started_at is None:
                attempt.started_at = now
            if status.state == PENDING and status.error:
                attempt.error = status.error  # e.g. "no suitable node", while it waits
            session.commit()
            return

        state, exit_code, error = outcome
        self._finish(attempt, state, now, exit_code=exit_code, error=error)
        session.commit()
        if status.state != GONE:
            self._remove(attempt.service_id)

    def cancel(
        self,
        session: Session,
        run: Run,
        now: datetime.datetime,
        requested_by: str = "",
        note: str = "",
    ) -> None:
        """end a run that has not ended; its service, if any, is removed"""
        if run.state in TERMINAL_RUN_STATES:
            raise Conflict(f"run {run.id} is {run.state}: there is nothing to cancel")

        attempt = run.attempts[-1]
        service_id = attempt.service_id if attempt.state == AttemptState.RUNNING else None
        error = f"cancelled by {requested_by}" + (f": {note}" if note else "")
        self._finish(attempt, AttemptState.CANCELLED, now, error=error)
        session.commit()

        if service_id:
            self._remove(service_id)

    def sweep(self, session: Session) -> int:
        """remove this stack's services whose attempt is over or unknown

        Whatever left one behind -- a removal that failed, a job deleted while
        its attempt launched, a database restored from a backup. The service of
        a pending attempt stays: its next launch adopts it.
        """
        services = self.runner.managed_services()
        if not services:
            return 0

        live = set(
            session.scalars(
                select(Attempt.token).where(
                    Attempt.state.in_([AttemptState.PENDING, AttemptState.RUNNING])
                )
            )
        )
        session.commit()

        removed = 0
        for service_id, token in services.items():
            if token not in live:
                LOGGER.warning("Removing service %s: its attempt is over or unknown", service_id)
                self._remove(service_id)
                removed += 1
        return removed

    # --- internals ------------------------------------------------------------

    @staticmethod
    def _still(session: Session, attempt: Attempt, state: str) -> bool:
        """re-read the attempt's state: is it still state? (False once deleted)"""
        try:
            session.refresh(attempt, ["state"])
        except InvalidRequestError:
            return False
        return attempt.state == state

    def _outcome(
        self, attempt: Attempt, status: TaskStatus, now: datetime.datetime
    ) -> Optional[Tuple[str, Optional[int], Optional[str]]]:
        """(state, exit code, error) once the attempt is over, else None"""
        if status.state == COMPLETE:
            return AttemptState.SUCCESS, status.exit_code, None
        if status.state == FAILED:
            return AttemptState.FAILED, status.exit_code, status.error
        if status.state == GONE:
            return AttemptState.LOST, None, status.error

        waited = (now - attempt.launched_at).total_seconds()
        start_timeout = self.settings.start_timeout_seconds
        if status.state == PENDING and attempt.started_at is None and waited >= start_timeout:
            return (
                AttemptState.TIMEOUT,
                None,
                f"not started after {start_timeout}s: {status.error or 'still pending'}",
            )

        timeout = attempt.run.job.timeout_seconds
        if waited >= timeout:
            return AttemptState.TIMEOUT, None, f"timed out after {timeout}s"
        return None

    def _finish(
        self,
        attempt: Attempt,
        state: str,
        now: datetime.datetime,
        exit_code: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """record an attempt's outcome, then the run's: ended, or a retry"""
        run = attempt.run
        job = run.job
        attempt.state = state
        attempt.exit_code = exit_code
        attempt.error = error
        attempt.finished_at = now
        started = attempt.launched_at or now
        self.metrics.attempt_finished(job.name, state, (now - started).total_seconds())

        if state == AttemptState.SUCCESS:
            self._end_run(run, RunState.SUCCESS, now)
        elif state == AttemptState.CANCELLED:
            self._end_run(run, RunState.CANCELLED, now)
        elif attempt.try_number < run.max_tries:
            run.state = RunState.RETRYING
            run.attempts.append(
                Attempt(
                    try_number=attempt.try_number + 1,
                    reason=Reason.RETRY,
                    state=AttemptState.PENDING,
                    not_before=now + datetime.timedelta(seconds=job.retry_delay_seconds),
                    requested_by="",
                    note="",
                    created_at=now,
                )
            )
        else:
            self._end_run(run, RunState.FAILED, now)

    def _end_run(self, run: Run, state: str, now: datetime.datetime) -> None:
        run.state = state
        run.finished_at = now
        self.metrics.run_finished(run.job.name, state)

    def _remove(self, service_id: str) -> None:
        """best effort: what stays behind, the sweep removes"""
        try:
            self.runner.remove(service_id)
        except RunnerUnavailable as exc:
            LOGGER.warning("Service %s not removed (%s): the sweep will", service_id, exc)
