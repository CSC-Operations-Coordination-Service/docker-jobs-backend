"""Jobs: define, change, delete, run now, preview

    GET    /jobs                    every job, by name
    POST   /jobs                    create; 409 if the name is taken
    GET    /jobs/{name}
    PUT    /jobs/{name}             replace the definition; the name stays
    DELETE /jobs/{name}             with its runs; 409 while an attempt runs
    POST   /jobs/{name}/trigger     a manual run: of {logical_date}'s interval,
                                    or of the latest that ended
    POST   /preview                 render a definition, check its swarm objects
"""

import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from docker_jobs_backend.auth import User, get_current_user
from docker_jobs_backend.db import Attempt, AttemptState, Job, Run, RunType, utcnow
from docker_jobs_backend.deps import (
    get_app_settings,
    get_lifecycle,
    get_metrics,
    get_runner,
    get_session,
)
from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.policy import refusals
from docker_jobs_backend.runner import Runner, RunnerUnavailable
from docker_jobs_backend.schedule import (
    Interval,
    interval_containing,
    latest_complete_interval,
    next_fire_times,
)
from docker_jobs_backend.schemas import (
    JobCreate,
    JobOut,
    JobSpec,
    PreviewOut,
    PreviewRequest,
    RunOut,
    TriggerRequest,
    job_columns,
    job_out,
    run_out,
)
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics
from docker_jobs_backend.templating import TemplateProblem, build_context, render

__all__ = ["router"]

router = APIRouter(tags=["jobs"])


def find_job(session: Session, name: str) -> Job:
    job = session.scalar(select(Job).where(Job.name == name))
    if job is None:
        raise HTTPException(status_code=404, detail=f"no job {name!r}")
    return job


def check_policy(spec: JobSpec, settings: Settings) -> None:
    """422 for what no job may have (policy.py): bind mounts, the backend's own
    objects, what the allow lists leave out"""
    problems = refusals(
        spec.networks,
        [mount.model_dump() for mount in spec.mounts],
        settings,
        secrets=[secret.model_dump() for secret in spec.secrets],
        configs=[config.model_dump() for config in spec.configs],
    )
    if problems:
        raise HTTPException(status_code=422, detail="; ".join(problems))


def interval_for(
    schedule: str, logical_date: Optional[datetime.datetime], now: datetime.datetime
) -> Interval:
    """the interval logical_date falls in, or the latest that ended"""
    if logical_date is None:
        return latest_complete_interval(schedule, now)
    return interval_containing(schedule, logical_date)


@router.get("/jobs", response_model=List[JobOut])
def list_jobs(session: Session = Depends(get_session)) -> List[JobOut]:
    now = utcnow()
    return [job_out(job, now) for job in session.scalars(select(Job).order_by(Job.name))]


@router.post("/jobs", status_code=201, response_model=JobOut)
def create_job(
    body: JobCreate,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    metrics: Metrics = Depends(get_metrics),
    settings: Settings = Depends(get_app_settings),
) -> JobOut:
    check_policy(body, settings)
    now = utcnow()
    job = Job(
        name=body.name,
        # by default the first interval is the current one: it runs at its end
        start_date=body.start_date or interval_containing(body.schedule, now).start,
        schedule_cursor=None,
        created_at=now,
        updated_at=now,
        updated_by=user.login,
        **job_columns(body),
    )
    session.add(job)
    try:
        session.commit()
    except IntegrityError as exc:
        raise HTTPException(status_code=409, detail=f"job {body.name!r} exists") from exc
    metrics.register_job(job.name)
    return job_out(job, now)


@router.get("/jobs/{name}", response_model=JobOut)
def get_job(name: str, session: Session = Depends(get_session)) -> JobOut:
    return job_out(find_job(session, name), utcnow())


@router.put("/jobs/{name}", response_model=JobOut)
def update_job(
    name: str,
    body: JobSpec,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    settings: Settings = Depends(get_app_settings),
) -> JobOut:
    check_policy(body, settings)
    job = find_job(session, name)
    start_date = body.start_date or job.start_date
    if body.end_date is not None and body.end_date <= start_date:
        raise HTTPException(
            status_code=422,
            detail=f"end_date must come after start_date ({start_date.isoformat()})",
        )
    now = utcnow()
    for key, value in job_columns(body).items():
        setattr(job, key, value)
    if body.start_date is not None:
        job.start_date = body.start_date
    job.updated_at = now
    job.updated_by = user.login
    session.commit()
    return job_out(job, now)


@router.delete("/jobs/{name}", status_code=204)
def delete_job(name: str, session: Session = Depends(get_session)) -> Response:
    job = find_job(session, name)
    running = session.scalar(
        select(func.count())
        .select_from(Attempt)
        .join(Run)
        .where(Run.job_id == job.id, Attempt.state == AttemptState.RUNNING)
    )
    if running:
        raise HTTPException(
            status_code=409,
            detail=f"job {name!r} has {running} attempt(s) running: cancel them, or wait",
        )
    session.delete(job)
    session.commit()
    return Response(status_code=204)


@router.post("/jobs/{name}/trigger", status_code=201, response_model=RunOut)
def trigger_job(
    name: str,
    body: Optional[TriggerRequest] = None,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    lifecycle: Lifecycle = Depends(get_lifecycle),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    body = body or TriggerRequest()
    job = find_job(session, name)
    if not job.enabled:
        raise HTTPException(status_code=409, detail=f"job {name!r} is disabled: enable it to run it")

    now = utcnow()
    interval = interval_for(job.schedule, body.logical_date, now)
    if interval.end > now:
        raise HTTPException(
            status_code=422,
            detail=f"the interval {interval.start.isoformat()} - {interval.end.isoformat()} has not ended yet",
        )

    run = lifecycle.create_run(
        session, job, interval, RunType.MANUAL, now, requested_by=user.login, note=body.note
    )
    session.commit()
    return run_out(run, settings.run_stack, with_attempts=True)


@router.post("/preview", response_model=PreviewOut)
def preview(
    body: PreviewRequest,
    runner: Runner = Depends(get_runner),
    settings: Settings = Depends(get_app_settings),
) -> PreviewOut:
    job = body.job
    check_policy(job, settings)
    now = utcnow()
    interval = interval_for(job.schedule, body.logical_date, now)
    try:
        rendered = render(
            job.entrypoint, job.command, job.env, build_context(interval, 0, 1, job.name, job.params)
        )
    except TemplateProblem as exc:
        raise HTTPException(status_code=422, detail=f"template {exc}") from exc

    try:
        problems = runner.missing_objects(
            [secret.source for secret in job.secrets],
            [config.source for config in job.configs],
            job.networks,
        )
    except RunnerUnavailable as exc:
        problems = [f"swarm objects not checked: {exc}"]

    first = job.start_date if job.start_date and job.start_date > now else now
    return PreviewOut(
        data_interval_start=interval.start,
        data_interval_end=interval.end,
        entrypoint=rendered.entrypoint,
        command=rendered.command,
        env=rendered.env,
        next_fire_times=next_fire_times(job.schedule, first, 5),
        problems=problems,
    )
