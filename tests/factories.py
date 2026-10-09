"""Test data with fixed times: jobs, runs, their attempts"""

import datetime

from docker_jobs_backend.db import Attempt, AttemptState, Job, Reason, Run, RunState, RunType
from docker_jobs_backend.runner import (
    ATTEMPT_LABEL,
    COMPLETE,
    FAILED,
    GONE,
    PENDING,
    RUNNING,
    RunnerUnavailable,
    TaskStatus,
)

UTC = datetime.UTC
T0 = datetime.datetime(2026, 10, 9, tzinfo=UTC)
SECOND = datetime.timedelta(seconds=1)
MINUTE = datetime.timedelta(minutes=1)
HOUR = datetime.timedelta(hours=1)
DAY = datetime.timedelta(days=1)


def make_job(session, **overrides) -> Job:
    """an hourly job from T0, committed; any column can be overridden"""
    values = dict(
        name="collect",
        description="",
        image="127.0.0.1:8085/maas-collector:3.10.1",
        entrypoint=None,
        command=["--start", "{{ data_interval_start | iso_ms }}"],
        env={},
        params={},
        schedule="0 * * * *",
        start_date=T0,
        end_date=None,
        enabled=True,
        catchup=True,
        max_active_runs=1,
        retries=0,
        retry_delay_seconds=300,
        timeout_seconds=3600,
        networks=[],
        constraints=[],
        secrets=[],
        configs=[],
        mounts=[],
        cpu_limit=None,
        memory_limit_mb=None,
        schedule_cursor=None,
        created_at=T0,
        updated_at=T0,
        updated_by="",
    )
    values.update(overrides)
    job = Job(**values)
    session.add(job)
    session.commit()
    return job


def make_run(
    job,
    start=T0,
    run_type=RunType.SCHEDULED,
    state=RunState.QUEUED,
    attempts=1,
    attempt_state=AttemptState.PENDING,
) -> Run:
    """a run of job over [start, start + 1h), NOT added to a session

    Attempts are numbered from 1; the last is in attempt_state, earlier ones
    failed.
    """
    run = Run(
        job=job,
        logical_date=start,
        data_interval_end=start + HOUR,
        run_type=run_type,
        state=state,
        max_tries=job.retries + 1,
        created_at=start + HOUR,
        requested_by="",
        note="",
    )
    for number in range(1, attempts + 1):
        run.attempts.append(
            Attempt(
                try_number=number,
                reason=Reason.SCHEDULED if number == 1 else Reason.RETRY,
                state=attempt_state if number == attempts else AttemptState.FAILED,
                requested_by="",
                note="",
                created_at=start + HOUR,
            )
        )
    return run


def attrs(**attributes) -> frozenset:
    """a data point's attributes, as points() keys them"""
    return frozenset(attributes.items())


def points(reader, name) -> dict:
    """{attributes: value} of one metric at a fresh collection; a histogram gives its count"""
    found = {}
    data = reader.get_metrics_data()
    if data is None:
        return found
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != name:
                    continue
                for point in metric.data.data_points:
                    value = point.count if hasattr(point, "bucket_counts") else point.value
                    found[frozenset(point.attributes.items())] = value
    return found


class FakeRunner:
    """the Runner, in memory: services, their task states, what was removed"""

    def __init__(self):
        self.launched = []  # LaunchSpec, in launch order
        self.services = {}  # service id -> attempt token, as managed_services()
        self.statuses = {}  # service id -> TaskStatus
        self.removed = []
        self.launch_error = None  # an exception launch() raises
        self.unavailable = False
        self.existing_objects = set()  # secret, config and network names

    def _reachable(self):
        if self.unavailable:
            raise RunnerUnavailable("docker unreachable: connection refused")

    def launch(self, spec):
        self._reachable()
        if self.launch_error is not None:
            raise self.launch_error
        self.launched.append(spec)
        service_id = f"svc-{len(self.launched)}"
        self.services[service_id] = spec.labels[ATTEMPT_LABEL]
        self.statuses[service_id] = TaskStatus(PENDING)
        return service_id

    def start(self, service_id):
        """the service's task runs"""
        self.statuses[service_id] = TaskStatus(RUNNING, node_id="node-1")

    def finish(self, service_id, exit_code=0):
        """the service's task exits"""
        if exit_code == 0:
            self.statuses[service_id] = TaskStatus(COMPLETE, exit_code=0, node_id="node-1")
        else:
            self.statuses[service_id] = TaskStatus(
                FAILED, exit_code=exit_code, error=f"task: non-zero exit ({exit_code})", node_id="node-1"
            )

    def status(self, service_id):
        self._reachable()
        if service_id not in self.services:
            return TaskStatus(GONE, error="service no longer exists")
        return self.statuses.get(service_id, TaskStatus(PENDING))

    def remove(self, service_id):
        self._reachable()
        self.removed.append(service_id)
        self.services.pop(service_id, None)
        self.statuses.pop(service_id, None)

    def managed_services(self):
        self._reachable()
        return dict(self.services)

    def missing_objects(self, secrets, configs, networks):
        self._reachable()
        wanted = (("secret", secrets), ("config", configs), ("network", networks))
        return [
            f"{kind} {name!r} does not exist"
            for kind, names in wanted
            for name in names
            if name not in self.existing_objects
        ]
