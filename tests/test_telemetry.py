"""Metrics: counters by job, gauges from the database snapshot"""

from factories import DAY, HOUR, MINUTE, T0, attrs, make_job, make_run, points

from docker_jobs_backend.db import AttemptState, Reason, RunState, RunType
from docker_jobs_backend.telemetry import DURATION_BUCKETS, GAP_LIMIT

OUTCOMES = ("success", "failed", "timeout", "lost", "cancelled")


def test_counters_of_a_job_start_at_zero(metrics, reader):
    metrics.register_job("collect")

    assert points(reader, "docker_jobs.runs.finished") == {
        attrs(docker_job="collect", state=state): 0 for state in ("success", "failed", "cancelled")
    }
    assert points(reader, "docker_jobs.attempts.finished") == {
        attrs(docker_job="collect", outcome=outcome): 0 for outcome in OUTCOMES
    }


def test_events_are_counted_by_job_and_kind(metrics, reader):
    metrics.run_created("collect", RunType.MANUAL)
    metrics.attempt_started("collect", Reason.RETRY)
    metrics.attempt_finished("collect", AttemptState.TIMEOUT, 600.0)
    metrics.run_finished("collect", RunState.FAILED)
    metrics.intervals_skipped("collect", 3)

    assert points(reader, "docker_jobs.runs.created") == {attrs(docker_job="collect", run_type="manual"): 1}
    assert points(reader, "docker_jobs.attempts.started") == {attrs(docker_job="collect", reason="retry"): 1}
    assert points(reader, "docker_jobs.attempts.finished") == {attrs(docker_job="collect", outcome="timeout"): 1}
    assert points(reader, "docker_jobs.attempt.duration") == {attrs(docker_job="collect", outcome="timeout"): 1}
    assert points(reader, "docker_jobs.runs.finished") == {attrs(docker_job="collect", state="failed"): 1}
    assert points(reader, "docker_jobs.intervals.skipped") == {attrs(docker_job="collect"): 3}


def test_durations_are_bucketed_up_to_a_day(metrics, reader):
    metrics.attempt_finished("collect", AttemptState.SUCCESS, 7200.0)

    [point] = [
        point
        for resource_metrics in reader.get_metrics_data().resource_metrics
        for scope_metrics in resource_metrics.scope_metrics
        for metric in scope_metrics.metrics
        if metric.name == "docker_jobs.attempt.duration"
        for point in metric.data.data_points
    ]
    assert tuple(point.explicit_bounds) == DURATION_BUCKETS


def test_gaps_are_unscheduled_or_undone_intervals(metrics, reader, session):
    job = make_job(session, schedule_cursor=T0 + 4 * HOUR)
    session.add_all(
        [
            make_run(job, T0, state=RunState.SUCCESS, attempt_state=AttemptState.SUCCESS),
            make_run(job, T0 + HOUR, state=RunState.FAILED, attempt_state=AttemptState.FAILED),
            make_run(job, T0 + 2 * HOUR, state=RunState.SUCCESS, attempt_state=AttemptState.SUCCESS),
            make_run(job, T0 + 3 * HOUR, state=RunState.RUNNING, attempt_state=AttemptState.RUNNING),
        ]
    )
    session.commit()

    # at 05:10, ended by 05:05: [01,02) failed; [04,05) never scheduled
    metrics.refresh(session, T0 + 5 * HOUR + 10 * MINUTE)

    assert points(reader, "docker_jobs.job.gaps") == {attrs(docker_job="collect"): 2}


def test_a_waiting_run_is_a_gap_after_the_grace_period_a_running_one_never(metrics, reader, session):
    waiting = make_job(session, name="waiting", schedule_cursor=T0 + HOUR)
    running = make_job(session, name="running", schedule_cursor=T0 + HOUR)
    session.add_all(
        [
            make_run(waiting, T0),
            make_run(running, T0, state=RunState.RUNNING, attempt_state=AttemptState.RUNNING),
        ]
    )
    session.commit()

    metrics.refresh(session, T0 + HOUR + 2 * MINUTE)
    assert points(reader, "docker_jobs.job.gaps") == {
        attrs(docker_job="waiting"): 0,
        attrs(docker_job="running"): 0,
    }

    metrics.refresh(session, T0 + HOUR + 6 * MINUTE)
    assert points(reader, "docker_jobs.job.gaps") == {
        attrs(docker_job="waiting"): 1,
        attrs(docker_job="running"): 0,
    }


def test_a_success_by_any_run_covers_the_interval(metrics, reader, session):
    job = make_job(session, schedule_cursor=T0 + HOUR)
    session.add_all(
        [
            make_run(job, T0, state=RunState.FAILED, attempt_state=AttemptState.FAILED),
            make_run(job, T0, run_type=RunType.MANUAL, state=RunState.SUCCESS, attempt_state=AttemptState.SUCCESS),
        ]
    )
    session.commit()

    metrics.refresh(session, T0 + HOUR + 30 * MINUTE)

    assert points(reader, "docker_jobs.job.gaps") == {attrs(docker_job="collect"): 0}


def test_old_failures_leave_the_lookback_window(metrics, reader, session, settings):
    settings.gap_lookback_hours = 2
    job = make_job(session, schedule_cursor=T0 + 5 * HOUR)
    session.add(make_run(job, T0, state=RunState.FAILED, attempt_state=AttemptState.FAILED))
    session.commit()

    metrics.refresh(session, T0 + 5 * HOUR)

    assert points(reader, "docker_jobs.job.gaps") == {attrs(docker_job="collect"): 0}


def test_a_disabled_job_reports_no_gaps(metrics, reader, session):
    job = make_job(session, enabled=False)
    session.add(make_run(job, T0, state=RunState.FAILED, attempt_state=AttemptState.FAILED))
    session.commit()

    metrics.refresh(session, T0 + 5 * HOUR)

    assert points(reader, "docker_jobs.job.gaps") == {}


def test_covered_until_is_the_end_of_the_latest_success(metrics, reader, session):
    job = make_job(session)
    session.add_all(
        [
            make_run(job, T0, state=RunState.SUCCESS, attempt_state=AttemptState.SUCCESS),
            make_run(job, T0 + 2 * HOUR, state=RunState.SUCCESS, attempt_state=AttemptState.SUCCESS),
            make_run(job, T0 + 3 * HOUR, state=RunState.FAILED, attempt_state=AttemptState.FAILED),
        ]
    )
    session.commit()

    metrics.refresh(session, T0 + 5 * HOUR)

    assert points(reader, "docker_jobs.job.covered_until") == {
        attrs(docker_job="collect"): (T0 + 3 * HOUR).timestamp()
    }


def test_active_runs_by_state_zeros_included(metrics, reader, session):
    job = make_job(session)
    session.add_all(
        [
            make_run(job, T0),
            make_run(job, T0 + HOUR, state=RunState.RUNNING, attempt_state=AttemptState.RUNNING),
            make_run(job, T0 + 2 * HOUR, state=RunState.SUCCESS, attempt_state=AttemptState.SUCCESS),
        ]
    )
    session.commit()

    metrics.refresh(session, T0 + 5 * HOUR)

    assert points(reader, "docker_jobs.runs.active") == {
        attrs(docker_job="collect", state="queued"): 1,
        attrs(docker_job="collect", state="running"): 1,
        attrs(docker_job="collect", state="retrying"): 0,
    }


def test_heartbeat_and_docker_up_appear_with_the_scheduler(metrics, reader):
    assert points(reader, "docker_jobs.scheduler.heartbeat") == {}
    assert points(reader, "docker_jobs.docker.up") == {}

    metrics.docker_reachable(False)
    metrics.tick(T0 + HOUR)

    assert points(reader, "docker_jobs.docker.up") == {attrs(): 0}
    assert points(reader, "docker_jobs.scheduler.heartbeat") == {attrs(): (T0 + HOUR).timestamp()}
    assert metrics.last_tick == T0 + HOUR


def test_gaps_are_capped_so_a_huge_backlog_stays_cheap(metrics, reader, session):
    # a minutely job never scheduled for the whole 7-day lookback: 10080 intervals
    make_job(session, schedule="* * * * *", start_date=T0 - 30 * DAY)

    metrics.refresh(session, T0)

    assert points(reader, "docker_jobs.job.gaps") == {attrs(docker_job="collect"): GAP_LIMIT}
    assert GAP_LIMIT == 1000
