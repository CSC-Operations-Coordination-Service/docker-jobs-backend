"""Runs: browse, inspect, rerun, cancel

    GET  /runs?job=&state=&offset=&size=   newest interval first
    GET  /runs/{id}                         with every attempt
    POST /runs/{id}/rerun                   an ended run, again; 409 otherwise
    POST /runs/{id}/cancel                  a run not ended yet; 409 otherwise
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from docker_jobs_backend.auth import User, get_current_user
from docker_jobs_backend.db import Job, Run, RunState, utcnow
from docker_jobs_backend.deps import get_app_settings, get_lifecycle, get_session
from docker_jobs_backend.lifecycle import Conflict, Lifecycle
from docker_jobs_backend.schemas import NoteRequest, RunOut, run_out
from docker_jobs_backend.settings import Settings

__all__ = ["router"]

router = APIRouter(tags=["runs"])

_WITH_ATTEMPTS = (selectinload(Run.job), selectinload(Run.attempts))


def find_run(session: Session, run_id: int) -> Run:
    run = session.get(Run, run_id, options=list(_WITH_ATTEMPTS))
    if run is None:
        raise HTTPException(status_code=404, detail=f"no run {run_id}")
    return run


@router.get("/runs", response_model=List[RunOut])
def list_runs(
    job: Optional[str] = None,
    state: Optional[RunState] = None,
    offset: int = Query(default=0, ge=0),
    size: int = Query(default=50, ge=1, le=500),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_app_settings),
) -> List[RunOut]:
    query = (
        select(Run)
        .join(Run.job)
        .options(*_WITH_ATTEMPTS)
        .order_by(Run.logical_date.desc(), Run.id.desc())
    )
    if job is not None:
        query = query.where(Job.name == job)
    if state is not None:
        query = query.where(Run.state == state)
    return [run_out(run, settings.run_stack) for run in session.scalars(query.offset(offset).limit(size))]


@router.get("/runs/{run_id}", response_model=RunOut)
def get_run(
    run_id: int,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    return run_out(find_run(session, run_id), settings.run_stack, with_attempts=True)


@router.post("/runs/{run_id}/rerun", response_model=RunOut)
def rerun_run(
    run_id: int,
    body: Optional[NoteRequest] = None,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    lifecycle: Lifecycle = Depends(get_lifecycle),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    body = body or NoteRequest()
    run = find_run(session, run_id)
    try:
        lifecycle.rerun(run, utcnow(), requested_by=user.login, note=body.note)
    except Conflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    session.commit()
    return run_out(run, settings.run_stack, with_attempts=True)


@router.post("/runs/{run_id}/cancel", response_model=RunOut)
def cancel_run(
    run_id: int,
    body: Optional[NoteRequest] = None,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    lifecycle: Lifecycle = Depends(get_lifecycle),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    body = body or NoteRequest()
    run = find_run(session, run_id)
    try:
        lifecycle.cancel(session, run, utcnow(), requested_by=user.login, note=body.note)
    except Conflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return run_out(run, settings.run_stack, with_attempts=True)
