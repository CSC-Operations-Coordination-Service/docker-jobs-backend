"""Request and response bodies of the API

What the Grafana app sends and reads. Datetimes are ISO 8601 with a timezone
(2026-10-09T00:00:00Z) both ways: one without is refused, never guessed.
"""

import datetime
import re
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from docker_jobs_backend.db import Attempt, Job, Run
from docker_jobs_backend.lifecycle import loki_selector
from docker_jobs_backend.schedule import next_fire_times, validate_cron
from docker_jobs_backend.templating import TemplateProblem, validate_templates

__all__ = [
    "AttemptOut",
    "ConfigRef",
    "JobCreate",
    "JobOut",
    "JobSpec",
    "MountSpec",
    "NAME_PATTERN",
    "NoteRequest",
    "PreviewOut",
    "PreviewRequest",
    "RunOut",
    "SecretRef",
    "TriggerRequest",
    "aware_utc",
    "job_columns",
    "job_out",
    "run_out",
]

# lowercase, digits, inner dashes, 32 at most: it goes in service names
NAME_PATTERN = r"^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$"
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def aware_utc(value: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
    """the datetime in UTC; ValueError when it has no timezone"""
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("give a timezone, e.g. 2026-10-09T00:00:00Z")
    return value.astimezone(datetime.UTC)


class SecretRef(BaseModel):
    """a swarm secret, at /run/secrets/<target, or source when empty>"""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    target: str = ""
    uid: str = "10001"
    gid: str = "10001"
    mode: int = 0o400


class ConfigRef(BaseModel):
    """a swarm config, at target"""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    target: str = Field(min_length=1)
    uid: str = "10001"
    gid: str = "10001"
    mode: int = 0o444


class MountSpec(BaseModel):
    """a volume, tmpfs, or -- with ALLOW_BIND_MOUNTS -- a host path"""

    model_config = ConfigDict(extra="forbid")

    type: Literal["volume", "bind", "tmpfs"] = "volume"
    source: str = ""
    target: str = Field(min_length=1)
    read_only: bool = False


class JobSpec(BaseModel):
    """a job's definition, as PUT /jobs/{name} takes it"""

    model_config = ConfigDict(extra="forbid")

    description: str = ""
    image: str = Field(min_length=1)
    entrypoint: Optional[List[str]] = None
    command: List[str] = Field(default_factory=list)
    env: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, str] = Field(default_factory=dict)
    schedule: str
    start_date: Optional[datetime.datetime] = None
    end_date: Optional[datetime.datetime] = None
    enabled: bool = True
    catchup: bool = True
    max_active_runs: int = Field(default=1, ge=1, le=16)
    retries: int = Field(default=0, ge=0, le=10)
    retry_delay_seconds: int = Field(default=300, ge=0, le=86_400)
    timeout_seconds: int = Field(default=3600, ge=60, le=7 * 86_400)
    networks: List[str] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)
    secrets: List[SecretRef] = Field(default_factory=list)
    configs: List[ConfigRef] = Field(default_factory=list)
    mounts: List[MountSpec] = Field(default_factory=list)
    cpu_limit: Optional[float] = Field(default=None, gt=0)
    memory_limit_mb: Optional[int] = Field(default=None, gt=0)

    @field_validator("schedule")
    @classmethod
    def _schedule(cls, value: str) -> str:
        return validate_cron(value)

    @field_validator("start_date", "end_date")
    @classmethod
    def _dates(cls, value):
        return aware_utc(value)

    @field_validator("env")
    @classmethod
    def _env_names(cls, value: Dict[str, str]) -> Dict[str, str]:
        bad = sorted(name for name in value if not _ENV_NAME.match(name))
        if bad:
            raise ValueError(f"not environment variable names: {bad}")
        return value

    @model_validator(mode="after")
    def _consistent(self):
        if self.start_date and self.end_date and self.end_date <= self.start_date:
            raise ValueError("end_date must come after start_date")
        try:
            validate_templates(self.entrypoint, self.command, self.env, self.params, "job")
        except TemplateProblem as exc:
            raise ValueError(f"template {exc}") from exc
        return self


class JobCreate(JobSpec):
    """a job, as POST /jobs takes it"""

    name: str = Field(pattern=NAME_PATTERN)


class JobOut(JobCreate):
    """a job, as the API returns it"""

    model_config = ConfigDict(extra="ignore")

    start_date: datetime.datetime
    schedule_cursor: Optional[datetime.datetime] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    updated_by: str
    # when its next run will be created: the end of its next interval
    next_fire_at: Optional[datetime.datetime] = None


class AttemptOut(BaseModel):
    """one try of a run"""

    try_number: int
    reason: str
    state: str
    requested_by: str
    note: str
    not_before: Optional[datetime.datetime]
    created_at: datetime.datetime
    launched_at: Optional[datetime.datetime]
    started_at: Optional[datetime.datetime]
    finished_at: Optional[datetime.datetime]
    service_name: Optional[str]
    node_id: Optional[str]
    rendered_entrypoint: Optional[List[str]]
    rendered_command: Optional[List[str]]
    exit_code: Optional[int]
    error: Optional[str]
    # LogQL selector of this try's logs; with launched_at/finished_at, a Loki query
    loki_selector: Optional[str]


class RunOut(BaseModel):
    """one interval of a job, or a manual run of one"""

    id: int
    job: str
    run_type: str
    state: str
    logical_date: datetime.datetime
    data_interval_start: datetime.datetime
    data_interval_end: datetime.datetime
    try_number: int
    max_tries: int
    created_at: datetime.datetime
    started_at: Optional[datetime.datetime]
    finished_at: Optional[datetime.datetime]
    requested_by: str
    note: str
    last_attempt: Optional[AttemptOut]
    # only GET /runs/{id} and the actions fill it
    attempts: Optional[List[AttemptOut]] = None


class TriggerRequest(BaseModel):
    """POST /jobs/{name}/trigger: the interval logical_date falls in, or the latest that ended"""

    model_config = ConfigDict(extra="forbid")

    logical_date: Optional[datetime.datetime] = None
    note: str = ""

    @field_validator("logical_date")
    @classmethod
    def _date(cls, value):
        return aware_utc(value)


class NoteRequest(BaseModel):
    """POST /runs/{id}/rerun and /cancel: why"""

    model_config = ConfigDict(extra="forbid")

    note: str = ""


class PreviewRequest(BaseModel):
    """POST /preview: a definition to render, for an interval"""

    model_config = ConfigDict(extra="forbid")

    job: JobCreate
    logical_date: Optional[datetime.datetime] = None

    @field_validator("logical_date")
    @classmethod
    def _date(cls, value):
        return aware_utc(value)


class PreviewOut(BaseModel):
    """what an attempt of that definition would run"""

    data_interval_start: datetime.datetime
    data_interval_end: datetime.datetime
    entrypoint: Optional[List[str]]
    command: List[str]
    env: Dict[str, str]
    next_fire_times: List[datetime.datetime]
    # missing secrets, configs, networks: what would fail the launch
    problems: List[str]


def job_columns(spec: JobSpec) -> dict:
    """the Job columns a spec sets; the caller decides about start_date"""
    return spec.model_dump(exclude={"name", "start_date"})


def _next_fire(job: Job, now: datetime.datetime) -> Optional[datetime.datetime]:
    if not job.enabled:
        return None
    [fire] = next_fire_times(job.schedule, max(now, job.start_date), 1)
    if job.end_date is not None and fire > job.end_date:
        return None
    return fire


def job_out(job: Job, now: datetime.datetime) -> JobOut:
    values = {column.key: getattr(job, column.key) for column in Job.__table__.columns}
    return JobOut(**values, next_fire_at=_next_fire(job, now))


_ATTEMPT_COLUMNS = [name for name in AttemptOut.model_fields if name != "loki_selector"]


def _attempt_out(attempt: Attempt, stack: str) -> AttemptOut:
    return AttemptOut(
        **{name: getattr(attempt, name) for name in _ATTEMPT_COLUMNS},
        loki_selector=loki_selector(stack, attempt.service_name) if attempt.service_name else None,
    )


def run_out(run: Run, stack: str, with_attempts: bool = False) -> RunOut:
    attempts = [_attempt_out(attempt, stack) for attempt in run.attempts]
    return RunOut(
        id=run.id,
        job=run.job.name,
        run_type=run.run_type,
        state=run.state,
        logical_date=run.logical_date,
        data_interval_start=run.logical_date,
        data_interval_end=run.data_interval_end,
        try_number=run.try_number,
        max_tries=run.max_tries,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        requested_by=run.requested_by,
        note=run.note,
        last_attempt=attempts[-1] if attempts else None,
        attempts=attempts if with_attempts else None,
    )
