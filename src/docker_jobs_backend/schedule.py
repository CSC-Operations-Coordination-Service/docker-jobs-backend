"""Cron schedules as data intervals, Airflow style

A job's schedule is a 5-field cron expression, in UTC. Consecutive fire times
delimit its data intervals: the run for [start, end) fires at end, and its
logical date is start. An hourly job firing at 01:00 covers [00:00, 01:00).

croniter's get_next/get_prev are strict (never the base time itself); cron is
minute-grained, so "the latest fire at or before t" is get_prev from the start
of the minute after t.
"""

import dataclasses
import datetime
from typing import List, Optional

from croniter import CroniterBadDateError, croniter

__all__ = [
    "Interval",
    "due_intervals",
    "interval_containing",
    "latest_complete_interval",
    "next_fire_times",
    "validate_cron",
]

UTC = datetime.UTC
_MINUTE = datetime.timedelta(minutes=1)


@dataclasses.dataclass(frozen=True)
class Interval:
    """[start, end): the data a run covers"""

    start: datetime.datetime
    end: datetime.datetime


def validate_cron(expression: str) -> str:
    """the expression with single spaces; ValueError unless 5-field cron"""
    normalised = " ".join(expression.split())

    if len(normalised.split(" ")) != 5 or not croniter.is_valid(normalised):
        raise ValueError(f"not a 5-field cron expression: {expression!r}")

    # valid syntax, but a date that never comes (0 0 30 2 *) would make every
    # interval computation raise -- in the scheduler, for every tick
    try:
        _next(normalised, datetime.datetime(2026, 1, 1, tzinfo=UTC))
    except CroniterBadDateError as exc:
        raise ValueError(f"this schedule never fires: {expression!r}") from exc

    return normalised


def _utc(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None:
        raise ValueError(f"naive datetime {moment!r}: give it a timezone")
    return moment.astimezone(UTC)


def _next(expression: str, after: datetime.datetime) -> datetime.datetime:
    return croniter(expression, after).get_next(datetime.datetime)


def _prev(expression: str, before: datetime.datetime) -> datetime.datetime:
    return croniter(expression, before).get_prev(datetime.datetime)


def _latest_fire_at_or_before(
    expression: str, moment: datetime.datetime
) -> datetime.datetime:
    floor = moment.replace(second=0, microsecond=0)
    return _prev(expression, floor + _MINUTE)


def _first_fire_at_or_after(
    expression: str, moment: datetime.datetime
) -> datetime.datetime:
    latest = _latest_fire_at_or_before(expression, moment)
    return latest if latest == moment else _next(expression, moment)


def interval_containing(expression: str, moment: datetime.datetime) -> Interval:
    """the interval moment falls in; a fire time starts its own"""
    start = _latest_fire_at_or_before(expression, _utc(moment))
    return Interval(start, _next(expression, start))


def latest_complete_interval(expression: str, now: datetime.datetime) -> Interval:
    """the interval that ended last, at or before now"""
    end = _latest_fire_at_or_before(expression, _utc(now))
    return Interval(_prev(expression, end), end)


def due_intervals(
    expression: str,
    start_date: datetime.datetime,
    end_date: Optional[datetime.datetime],
    cursor: Optional[datetime.datetime],
    now: datetime.datetime,
    limit: int,
) -> List[Interval]:
    """the intervals that ended in (lower, now], oldest first, at most limit

    lower is the cursor -- the end of the last interval already handled -- but
    never before the first fire time at or after start_date: no interval
    starts before start_date. With end_date, no interval ends after it.
    """
    first = _first_fire_at_or_after(expression, _utc(start_date))
    lower = first if cursor is None else max(_utc(cursor), first)
    upper = _utc(now) if end_date is None else min(_utc(now), _utc(end_date))

    intervals: List[Interval] = []
    end = _next(expression, lower)

    while end <= upper and len(intervals) < limit:
        intervals.append(Interval(_prev(expression, end), end))
        end = _next(expression, end)

    return intervals


def next_fire_times(
    expression: str, after: datetime.datetime, count: int
) -> List[datetime.datetime]:
    """the next count fire times strictly after after"""
    iterator = croniter(expression, _utc(after))
    return [iterator.get_next(datetime.datetime) for _ in range(count)]
