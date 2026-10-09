"""Cron expressions become Airflow-style data intervals"""

import datetime

import pytest

from docker_jobs_backend.schedule import (
    Interval,
    due_intervals,
    interval_containing,
    latest_complete_interval,
    next_fire_times,
    validate_cron,
)

UTC = datetime.UTC
HOURLY = "0 * * * *"


def at(hour, minute=0, day=9):
    return datetime.datetime(2026, 10, day, hour, minute, tzinfo=UTC)


def test_a_valid_expression_comes_back_normalised():
    assert validate_cron("  0   *  * * * ") == "0 * * * *"


@pytest.mark.parametrize(
    "expression", ["", "* * * *", "0 0 * * * *", "61 * * * *", "@hourly", "every hour"]
)
def test_anything_but_five_valid_fields_is_refused(expression):
    with pytest.raises(ValueError):
        validate_cron(expression)


def test_an_interval_ends_at_the_next_fire_time():
    assert interval_containing(HOURLY, at(10, 20)) == Interval(at(10), at(11))


def test_a_fire_time_starts_its_own_interval():
    assert interval_containing(HOURLY, at(10)) == Interval(at(10), at(11))


def test_irregular_schedules_have_irregular_intervals():
    twice_a_day = "0 1,13 * * *"

    assert interval_containing(twice_a_day, at(0, day=10)) == Interval(
        at(13, day=9), at(1, day=10)
    )
    assert interval_containing(twice_a_day, at(5, day=10)) == Interval(
        at(1, day=10), at(13, day=10)
    )


def test_the_latest_complete_interval_is_the_one_that_ended_last():
    assert latest_complete_interval(HOURLY, at(10, 20)) == Interval(at(9), at(10))
    assert latest_complete_interval(HOURLY, at(10)) == Interval(at(9), at(10))


def test_other_timezones_are_converted_to_utc():
    paris = datetime.timezone(datetime.timedelta(hours=2))
    moment = datetime.datetime(2026, 10, 9, 12, 20, tzinfo=paris)

    assert interval_containing(HOURLY, moment) == Interval(at(10), at(11))


def test_naive_datetimes_are_refused():
    with pytest.raises(ValueError, match="timezone"):
        interval_containing(HOURLY, datetime.datetime(2026, 10, 9))


def test_due_intervals_begin_at_start_date():
    assert due_intervals(HOURLY, at(0), None, None, at(3, 30), limit=100) == [
        Interval(at(0), at(1)),
        Interval(at(1), at(2)),
        Interval(at(2), at(3)),
    ]


def test_an_interval_ending_exactly_now_is_due():
    assert due_intervals(HOURLY, at(0), None, None, at(1), limit=100) == [
        Interval(at(0), at(1))
    ]


def test_no_interval_starts_before_start_date():
    assert due_intervals(HOURLY, at(0, 10), None, None, at(3, 30), limit=100) == [
        Interval(at(1), at(2)),
        Interval(at(2), at(3)),
    ]


def test_the_cursor_skips_what_was_handled():
    assert due_intervals(HOURLY, at(0), None, at(2), at(3, 30), limit=100) == [
        Interval(at(2), at(3))
    ]


def test_a_start_date_after_the_cursor_wins():
    assert due_intervals(HOURLY, at(2), None, at(0), at(3, 30), limit=100) == [
        Interval(at(2), at(3))
    ]


def test_end_date_is_the_last_end():
    assert due_intervals(HOURLY, at(0), at(2), None, at(5), limit=100) == [
        Interval(at(0), at(1)),
        Interval(at(1), at(2)),
    ]


def test_catching_up_a_year_is_bounded_by_the_limit():
    start = datetime.datetime(2025, 10, 9, tzinfo=UTC)

    due = due_intervals(HOURLY, start, None, None, at(0), limit=100)

    assert len(due) == 100
    assert due[0] == Interval(start, start + datetime.timedelta(hours=1))
    assert due[-1].end == start + datetime.timedelta(hours=100)


def test_next_fire_times_are_strictly_after():
    assert next_fire_times(HOURLY, at(10, 20), 3) == [at(11), at(12), at(13)]
    assert next_fire_times(HOURLY, at(11), 1) == [at(12)]


def test_a_date_that_never_comes_is_refused():
    with pytest.raises(ValueError, match="never"):
        validate_cron("0 0 30 2 *")
