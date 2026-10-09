"""Jinja templates in a job's entrypoint, command and environment

Each element of the entrypoint and command lists, and each environment value,
is a template rendered on its own when an attempt is launched. There is no
shell: a rendered value with spaces stays one argument. The sandbox keeps
Python internals out; StrictUndefined turns a typo into an error instead of an
empty string.

Context:
    data_interval_start, data_interval_end    aware UTC datetimes
    logical_date                               = data_interval_start
    ds                                         logical date, YYYY-MM-DD
    ts                                         logical date, ISO 8601
    run_id, try_number, job_name
    params                                     the job's own dict
    timedelta                                  for window arithmetic
Filters:
    iso_ms   2026-10-09T00:00:00.000Z
    iso      2026-10-09T00:00:00Z

    --replay-start-date {{ (data_interval_end - timedelta(days=7)) | iso_ms }}
"""

import dataclasses
import datetime
from typing import Dict, List, Mapping, Optional, Sequence

from jinja2 import StrictUndefined
from jinja2.sandbox import ImmutableSandboxedEnvironment

from docker_jobs_backend.schedule import Interval

__all__ = [
    "Rendered",
    "TemplateProblem",
    "build_context",
    "iso",
    "iso_ms",
    "render",
    "validate_templates",
]

UTC = datetime.UTC
SAMPLE_INTERVAL = Interval(
    datetime.datetime(2026, 1, 1, tzinfo=UTC), datetime.datetime(2026, 1, 1, 1, tzinfo=UTC)
)


class TemplateProblem(ValueError):
    """a template that does not render; field says which one"""

    def __init__(self, field: str, message: str):
        super().__init__(f"{field}: {message}")
        self.field = field
        self.message = message


def iso_ms(moment: datetime.datetime) -> str:
    """2026-10-09T00:00:00.000Z"""
    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def iso(moment: datetime.datetime) -> str:
    """2026-10-09T00:00:00Z"""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


_ENVIRONMENT = ImmutableSandboxedEnvironment(
    undefined=StrictUndefined, autoescape=False, keep_trailing_newline=True
)
_ENVIRONMENT.filters["iso_ms"] = iso_ms
_ENVIRONMENT.filters["iso"] = iso


@dataclasses.dataclass(frozen=True)
class Rendered:
    """an attempt's entrypoint, command and environment, rendered"""

    entrypoint: Optional[List[str]]
    command: List[str]
    env: Dict[str, str]


def build_context(
    interval: Interval,
    run_id: int,
    try_number: int,
    job_name: str,
    params: Mapping[str, str],
) -> dict:
    """what the templates of one attempt see"""
    return {
        "data_interval_start": interval.start,
        "data_interval_end": interval.end,
        "logical_date": interval.start,
        "ds": interval.start.strftime("%Y-%m-%d"),
        "ts": interval.start.isoformat(),
        "run_id": run_id,
        "try_number": try_number,
        "job_name": job_name,
        "params": dict(params),
        "timedelta": datetime.timedelta,
    }


def _render_one(field: str, template: str, context: dict) -> str:
    try:
        return _ENVIRONMENT.from_string(template).render(context)
    except Exception as exc:  # syntax, undefined, sandbox, or the expression itself
        raise TemplateProblem(field, str(exc)) from exc


def render(
    entrypoint: Optional[Sequence[str]],
    command: Sequence[str],
    env: Mapping[str, str],
    context: dict,
) -> Rendered:
    """every template rendered; TemplateProblem on the first that fails"""
    return Rendered(
        entrypoint=None
        if entrypoint is None
        else [_render_one(f"entrypoint[{i}]", item, context) for i, item in enumerate(entrypoint)],
        command=[_render_one(f"command[{i}]", item, context) for i, item in enumerate(command)],
        env={name: _render_one(f"env.{name}", value, context) for name, value in env.items()},
    )


def validate_templates(
    entrypoint: Optional[Sequence[str]],
    command: Sequence[str],
    env: Mapping[str, str],
    params: Mapping[str, str],
    job_name: str,
) -> Rendered:
    """render with a sample interval: TemplateProblem if any template is broken"""
    return render(
        entrypoint, command, env, build_context(SAMPLE_INTERVAL, 0, 1, job_name, params)
    )
