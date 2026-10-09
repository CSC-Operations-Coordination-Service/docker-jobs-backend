"""Jinja templates in the entrypoint, command and environment of a job"""

import datetime

import pytest

from docker_jobs_backend.schedule import Interval
from docker_jobs_backend.templating import (
    TemplateProblem,
    build_context,
    iso_ms,
    render,
    validate_templates,
)

UTC = datetime.UTC
HOUR = Interval(
    datetime.datetime(2026, 10, 9, 0, tzinfo=UTC),
    datetime.datetime(2026, 10, 9, 1, tzinfo=UTC),
)


def context(**params):
    return build_context(HOUR, run_id=42, try_number=1, job_name="collect", params=params)


def test_iso_ms_is_zulu_with_milliseconds():
    assert iso_ms(datetime.datetime(2026, 10, 9, tzinfo=UTC)) == "2026-10-09T00:00:00.000Z"
    paris = datetime.timezone(datetime.timedelta(hours=2))
    moment = datetime.datetime(2026, 10, 9, 2, 3, 4, 567891, tzinfo=paris)
    assert iso_ms(moment) == "2026-10-09T00:03:04.567Z"


def test_the_hourly_collect_example():
    command = ["python", "-m", "module-collect", "--start", "{{ data_interval_start | iso_ms }}"]

    rendered = render(None, command, {}, context())

    assert rendered.command == [
        "python",
        "-m",
        "module-collect",
        "--start",
        "2026-10-09T00:00:00.000Z",
    ]
    assert rendered.entrypoint is None


def test_a_window_relative_to_the_fire_time():
    command = ["--replay-start-date", "{{ (data_interval_end - timedelta(days=7)) | iso_ms }}"]

    assert render(None, command, {}, context()).command[1] == "2026-10-02T01:00:00.000Z"


def test_everything_in_the_context():
    rendered = render(
        ["{{ job_name }}"],
        [
            "{{ ds }}",
            "{{ ts }}",
            "{{ run_id }}",
            "{{ try_number }}",
            "{{ logical_date | iso }}",
            "{{ data_interval_end | iso }}",
        ],
        {"WHO": "{{ params.who }}"},
        context(who="me"),
    )

    assert rendered.entrypoint == ["collect"]
    assert rendered.command == [
        "2026-10-09",
        "2026-10-09T00:00:00+00:00",
        "42",
        "1",
        "2026-10-09T00:00:00Z",
        "2026-10-09T01:00:00Z",
    ]
    assert rendered.env == {"WHO": "me"}


def test_a_value_with_spaces_stays_one_argument():
    rendered = render(None, ["--name", "{{ params.label }}"], {}, context(label="two words"))

    assert rendered.command == ["--name", "two words"]


def test_an_unknown_variable_is_an_error_not_an_empty_string():
    with pytest.raises(TemplateProblem) as caught:
        render(None, ["ok", "{{ data_interval_begin }}"], {}, context())

    assert caught.value.field == "command[1]"
    assert "data_interval_begin" in caught.value.message


def test_a_missing_param_names_the_variable():
    with pytest.raises(TemplateProblem) as caught:
        render(None, [], {"IFACE": "{{ params.interface }}"}, context())

    assert caught.value.field == "env.IFACE"


def test_a_syntax_error_is_reported_with_its_field():
    with pytest.raises(TemplateProblem, match=r"entrypoint\[0\]"):
        render(["{{ oops"], [], {}, context())


def test_the_sandbox_keeps_python_internals_out():
    with pytest.raises(TemplateProblem):
        render(None, ["{{ ''.__class__.__mro__ }}"], {}, context())


def test_validation_renders_a_sample_interval():
    rendered = validate_templates(None, ["{{ data_interval_start | iso_ms }}"], {}, {}, "collect")

    assert rendered.command == ["2026-01-01T00:00:00.000Z"]
