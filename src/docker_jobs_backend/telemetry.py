"""Metrics, pushed over OTLP/HTTP to Alloy, on to Mimir

Instruments carry OpenTelemetry's dotted names. Mimir's OTLP endpoint turns
them into Prometheus names -- dots to underscores and, when Mimir's
otel_metric_suffixes_enabled is on, _seconds for a unit of seconds and _total
for a counter. The alert rules (deploy/grafana/docker-jobs-alerts.yaml) match
both forms; with the suffixes:

    docker_jobs.runs.created         docker_jobs_runs_created_total{docker_job, run_type}
    docker_jobs.runs.finished        docker_jobs_runs_finished_total{docker_job, state}
    docker_jobs.attempts.started     docker_jobs_attempts_started_total{docker_job, reason}
    docker_jobs.attempts.finished    docker_jobs_attempts_finished_total{docker_job, outcome}
    docker_jobs.intervals.skipped    docker_jobs_intervals_skipped_total{docker_job}
    docker_jobs.attempt.duration     docker_jobs_attempt_duration_seconds_*{docker_job, outcome}
    docker_jobs.job.gaps             docker_jobs_job_gaps{docker_job}
    docker_jobs.job.covered_until    docker_jobs_job_covered_until_seconds{docker_job}
    docker_jobs.runs.active          docker_jobs_runs_active{docker_job, state}
    docker_jobs.docker.up            docker_jobs_docker_up
    docker_jobs.scheduler.heartbeat  docker_jobs_scheduler_heartbeat_seconds

The job is the docker_job attribute, never job: Mimir fills job from the
resource's service.name.

Outcome counters start at 0 for every job (register_job): PromQL's increase()
needs a sample before the first failure to see it.

Gauges come from a snapshot the scheduler thread computes at the end of every
tick (refresh); the export thread only reads it. A callback querying the
database itself would have its errors swallowed by the SDK, and the gauge
would silently vanish -- the gaps alert with it.
"""

import dataclasses
import datetime
import logging
from typing import Dict, Iterable, Optional, Tuple

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation
from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from docker_jobs_backend.db import AttemptState, Job, Run, RunState
from docker_jobs_backend.schedule import due_intervals
from docker_jobs_backend.settings import Settings

__all__ = ["DURATION_BUCKETS", "JOB", "Metrics", "count_gaps", "create_meter_provider"]

LOGGER = logging.getLogger(__name__)

JOB = "docker_job"
# the gaps count stops here: past it the alert fires anyway, and counting
# further would cost every tick a cron walk under the database lock
GAP_LIMIT = 1_000
ACTIVE_STATES = (RunState.QUEUED, RunState.RUNNING, RunState.RETRYING)
NOT_DONE_STATES = frozenset({RunState.FAILED, RunState.CANCELLED, RunState.QUEUED, RunState.RETRYING})
RUN_OUTCOMES = (RunState.SUCCESS, RunState.FAILED, RunState.CANCELLED)
ATTEMPT_OUTCOMES = (
    AttemptState.SUCCESS,
    AttemptState.FAILED,
    AttemptState.TIMEOUT,
    AttemptState.LOST,
    AttemptState.CANCELLED,
)
# seconds: from a quick check to a day-long recollect
DURATION_BUCKETS = (1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200, 14400, 28800, 86400)


def create_meter_provider(settings: Settings) -> SDKMeterProvider:
    """OTLP over HTTP, as the standard variables say

    OTEL_EXPORTER_OTLP_ENDPOINT (http://alloy:4318), OTEL_METRIC_EXPORT_INTERVAL
    (milliseconds, 60000 by default), OTEL_SDK_DISABLED=true to turn it off.
    service.instance.id is the run stack: stable across restarts, so a restart
    continues the same series, and two backends never mix theirs.
    """
    resource = Resource.create(
        {"service.name": settings.otel_service_name, "service.instance.id": settings.run_stack}
    )
    reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    return SDKMeterProvider(resource=resource, metric_readers=[reader])


def count_gaps(
    session: Session,
    job: Job,
    now: datetime.datetime,
    grace: datetime.timedelta,
    lookback: datetime.timedelta,
) -> int:
    """intervals of job that ended in (now - lookback, now - grace] with no data

    Two kinds, each interval counted once:

    * not scheduled: ended after the job's cursor -- the scheduler is down, or
      behind. Computed with the current schedule from the cursor on only, so a
      schedule change never invents a past gap;
    * not done: it has runs, none succeeded, and one failed, was cancelled, or
      still waits to start or to retry. A run in progress is no gap: the
      timeouts deal with runs that never end.
    """
    until = now - grace
    since = now - lookback
    lower = since if job.schedule_cursor is None else max(job.schedule_cursor, since)
    unscheduled = {
        interval.start
        for interval in due_intervals(job.schedule, job.start_date, job.end_date, lower, until, GAP_LIMIT)
    }

    rows = session.execute(
        select(Run.logical_date, Run.state).where(
            Run.job_id == job.id,
            Run.data_interval_end > since,
            Run.data_interval_end <= until,
        )
    ).all()
    done = {logical for logical, state in rows if state == RunState.SUCCESS}
    undone = {logical for logical, state in rows if state in NOT_DONE_STATES}

    return len((unscheduled | undone) - done)


@dataclasses.dataclass(frozen=True)
class Snapshot:
    """the gauges, as of the last refresh"""

    gaps: Dict[str, int] = dataclasses.field(default_factory=dict)
    covered_until: Dict[str, float] = dataclasses.field(default_factory=dict)
    active: Dict[Tuple[str, str], int] = dataclasses.field(default_factory=dict)


class Metrics:
    """the instruments the lifecycle and the scheduler record into"""

    def __init__(self, meter_provider: MeterProvider, settings: Settings):
        self._settings = settings
        self._snapshot = Snapshot()
        self._docker_up: Optional[bool] = None
        self._last_tick: Optional[datetime.datetime] = None

        meter = meter_provider.get_meter("docker_jobs_backend")
        self._runs_created = meter.create_counter(
            "docker_jobs.runs.created", description="Runs created, by run_type: scheduled or manual"
        )
        self._runs_finished = meter.create_counter(
            "docker_jobs.runs.finished",
            description="Runs ended, by state: success, failed (no try left), cancelled",
        )
        self._attempts_started = meter.create_counter(
            "docker_jobs.attempts.started",
            description="Attempts started, by reason: scheduled, manual, retry, rerun",
        )
        self._attempts_finished = meter.create_counter(
            "docker_jobs.attempts.finished",
            description="Attempts ended, by outcome: success, failed, timeout, lost, cancelled",
        )
        self._intervals_skipped = meter.create_counter(
            "docker_jobs.intervals.skipped",
            description="Intervals never run because the job does not catch up",
        )
        self._duration = meter.create_histogram(
            "docker_jobs.attempt.duration",
            unit="s",
            description="Attempt duration, launch to outcome",
            explicit_bucket_boundaries_advisory=DURATION_BUCKETS,
        )
        meter.create_observable_gauge(
            "docker_jobs.job.gaps",
            callbacks=[self._observe_gaps],
            description="Intervals that ended in the lookback window with no data",
        )
        meter.create_observable_gauge(
            "docker_jobs.job.covered_until",
            unit="s",
            callbacks=[self._observe_covered_until],
            description="End of the latest interval with a successful run, Unix time",
        )
        meter.create_observable_gauge(
            "docker_jobs.runs.active",
            callbacks=[self._observe_active],
            description="Runs not ended, by state: queued, running, retrying",
        )
        meter.create_observable_gauge(
            "docker_jobs.docker.up",
            callbacks=[self._observe_docker_up],
            description="1 when the last tick reached Docker, 0 when it could not",
        )
        meter.create_observable_gauge(
            "docker_jobs.scheduler.heartbeat",
            unit="s",
            callbacks=[self._observe_heartbeat],
            description="Unix time of the last completed scheduler tick",
        )

    # --- events -----------------------------------------------------------------

    def register_job(self, job: str) -> None:
        """start the job's outcome counters at 0, so increase() sees the first"""
        for state in RUN_OUTCOMES:
            self._runs_finished.add(0, {JOB: job, "state": str(state)})
        for outcome in ATTEMPT_OUTCOMES:
            self._attempts_finished.add(0, {JOB: job, "outcome": str(outcome)})

    def register_jobs(self, jobs: Iterable[str]) -> None:
        for job in jobs:
            self.register_job(job)

    def run_created(self, job: str, run_type: str) -> None:
        self._runs_created.add(1, {JOB: job, "run_type": str(run_type)})

    def attempt_started(self, job: str, reason: str) -> None:
        self._attempts_started.add(1, {JOB: job, "reason": str(reason)})

    def attempt_finished(self, job: str, outcome: str, duration_seconds: float) -> None:
        attributes = {JOB: job, "outcome": str(outcome)}
        self._attempts_finished.add(1, attributes)
        self._duration.record(max(duration_seconds, 0.0), attributes)

    def run_finished(self, job: str, state: str) -> None:
        self._runs_finished.add(1, {JOB: job, "state": str(state)})

    def intervals_skipped(self, job: str, count: int) -> None:
        self._intervals_skipped.add(count, {JOB: job})

    # --- the scheduler's ----------------------------------------------------

    def refresh(self, session: Session, now: datetime.datetime) -> None:
        """recompute the gauges from the database"""
        grace = datetime.timedelta(seconds=self._settings.gap_grace_seconds)
        lookback = datetime.timedelta(hours=self._settings.gap_lookback_hours)
        jobs = session.scalars(select(Job).order_by(Job.name)).all()

        covered = session.execute(
            select(Job.name, func.max(Run.data_interval_end))
            .join(Run, Run.job_id == Job.id)
            .where(Run.state == RunState.SUCCESS)
            .group_by(Job.name)
        ).all()
        active = {(job.name, str(state)): 0 for job in jobs for state in ACTIVE_STATES}
        for name, state, count in session.execute(
            select(Job.name, Run.state, func.count())
            .join(Run, Run.job_id == Job.id)
            .where(Run.state.in_(ACTIVE_STATES))
            .group_by(Job.name, Run.state)
        ):
            active[(name, state)] = count

        gaps = {}
        for job in jobs:
            if not job.enabled:
                continue
            try:
                gaps[job.name] = count_gaps(session, job, now, grace, lookback)
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Gaps of job %s not computed", job.name)

        self._snapshot = Snapshot(
            gaps=gaps,
            covered_until={name: end.timestamp() for name, end in covered},
            active=active,
        )

    def docker_reachable(self, reachable: bool) -> None:
        self._docker_up = reachable

    def tick(self, now: datetime.datetime) -> None:
        """a tick completed"""
        self._last_tick = now

    @property
    def last_tick(self) -> Optional[datetime.datetime]:
        return self._last_tick

    # --- gauge callbacks: memory only ---------------------------------------

    def _observe_gaps(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(count, {JOB: job}) for job, count in self._snapshot.gaps.items()]

    def _observe_covered_until(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(end, {JOB: job}) for job, end in self._snapshot.covered_until.items()]

    def _observe_active(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [
            Observation(count, {JOB: job, "state": state})
            for (job, state), count in self._snapshot.active.items()
        ]

    def _observe_docker_up(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [] if self._docker_up is None else [Observation(1 if self._docker_up else 0)]

    def _observe_heartbeat(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [] if self._last_tick is None else [Observation(self._last_tick.timestamp())]
