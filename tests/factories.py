"""Test data with fixed times: jobs, runs, their attempts"""

import datetime

from docker_jobs_backend.db import Attempt, AttemptState, Job, Reason, Run, RunState, RunType

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
