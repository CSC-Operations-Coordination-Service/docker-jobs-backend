"""Ticks: intervals become runs, runs start within limits, Docker outages cost nothing"""

import time

import pytest
from factories import DAY, HOUR, MINUTE, SECOND, T0, attrs, make_job, make_run, points
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from docker_jobs_backend.db import Job, Run
from docker_jobs_backend.runner import RunnerUnavailable
from docker_jobs_backend.scheduler import SchedulerThread, exclusive_lock


def runs_of(session_factory, job_name="collect"):
    with session_factory() as session:
        return session.scalars(
            select(Run)
            .join(Run.job)
            .where(Job.name == job_name)
            .order_by(Run.logical_date, Run.id)
            .options(selectinload(Run.attempts))
        ).all()


def starts(runs):
    return [run.logical_date for run in runs]


def states(runs):
    return [run.state for run in runs]


def test_catch_up_creates_a_run_per_ended_interval_oldest_first(scheduler, session, session_factory):
    make_job(session)

    scheduler.schedule(T0 + 3 * HOUR + 30 * MINUTE)

    assert starts(runs_of(session_factory)) == [T0, T0 + HOUR, T0 + 2 * HOUR]


def test_scheduling_again_creates_nothing_new(scheduler, session, session_factory):
    make_job(session)

    scheduler.schedule(T0 + 3 * HOUR)
    scheduler.schedule(T0 + 3 * HOUR + 30 * MINUTE)

    assert len(runs_of(session_factory)) == 3


def test_without_catchup_only_the_latest_interval_runs(scheduler, session, session_factory, reader):
    make_job(session, catchup=False)

    scheduler.schedule(T0 + 3 * HOUR + 30 * MINUTE)

    assert starts(runs_of(session_factory)) == [T0 + 2 * HOUR]
    assert points(reader, "docker_jobs.intervals.skipped") == {attrs(docker_job="collect"): 2}


def test_without_catchup_a_long_pause_jumps_to_now(scheduler, session, session_factory):
    make_job(session, catchup=False, start_date=T0 - 30 * DAY)

    scheduler.schedule(T0)

    assert starts(runs_of(session_factory)) == [T0 - HOUR]


def test_catch_up_is_batched_and_resumes(scheduler, session, session_factory):
    make_job(session, start_date=T0 - 10 * DAY)  # 240 hourly intervals

    scheduler.schedule(T0)
    assert len(runs_of(session_factory)) == 100

    scheduler.schedule(T0)
    scheduler.schedule(T0)
    runs = runs_of(session_factory)
    assert len(runs) == 240
    assert len(set(starts(runs))) == 240
    assert runs[-1].data_interval_end == T0


def test_only_the_oldest_run_starts_when_one_may_run_at_a_time(scheduler, session, session_factory, runner):
    make_job(session)

    scheduler.tick(T0 + 3 * HOUR)

    [spec] = runner.launched
    assert spec.command == ["--start", "2026-10-09T00:00:00.000Z"]
    assert states(runs_of(session_factory)) == ["running", "queued", "queued"]


def test_a_finished_attempt_frees_its_slot_in_the_same_tick(scheduler, session, session_factory, runner):
    make_job(session)
    scheduler.tick(T0 + 3 * HOUR)
    runner.finish("svc-1", exit_code=0)

    scheduler.tick(T0 + 3 * HOUR + 10 * SECOND)

    assert states(runs_of(session_factory)) == ["success", "running", "queued"]
    assert runner.removed == ["svc-1"]
    assert runner.launched[1].command == ["--start", "2026-10-09T01:00:00.000Z"]


def test_max_active_runs_lets_several_run(scheduler, session, session_factory):
    make_job(session, max_active_runs=2)

    scheduler.tick(T0 + 3 * HOUR)

    assert states(runs_of(session_factory)) == ["running", "running", "queued"]


def test_max_running_caps_all_jobs_together(scheduler, session, runner, settings):
    settings.max_running = 2
    for name in ("one", "two", "three"):
        make_job(session, name=name)

    scheduler.tick(T0 + HOUR)

    assert len(runner.launched) == 2


def test_a_retrying_run_keeps_its_slot_and_retries_after_the_delay(
    scheduler, session, session_factory, runner, reader
):
    make_job(session, retries=1, retry_delay_seconds=600)
    scheduler.tick(T0 + 2 * HOUR)
    runner.finish("svc-1", exit_code=3)

    scheduler.tick(T0 + 2 * HOUR + MINUTE)

    assert states(runs_of(session_factory)) == ["retrying", "queued"]
    assert len(runner.launched) == 1

    scheduler.tick(T0 + 2 * HOUR + 11 * MINUTE)

    assert [spec.service_name for spec in runner.launched] == ["djobs_collect_1_1", "djobs_collect_1_2"]
    assert points(reader, "docker_jobs.attempts.started") == {
        attrs(docker_job="collect", reason="scheduled"): 1,
        attrs(docker_job="collect", reason="retry"): 1,
    }


def test_a_disabled_job_neither_schedules_nor_launches(scheduler, session, session_factory, runner):
    job = make_job(session)
    session.add(make_run(job))
    job.enabled = False
    session.commit()

    scheduler.tick(T0 + 3 * HOUR)

    assert runner.launched == []
    assert len(runs_of(session_factory)) == 1


def test_docker_down_burns_no_retry_and_stops_the_heartbeat(
    scheduler, session, session_factory, runner, reader
):
    make_job(session, retries=2)
    runner.unavailable = True

    with pytest.raises(RunnerUnavailable):
        scheduler.tick(T0 + HOUR)

    assert points(reader, "docker_jobs.scheduler.heartbeat") == {}
    assert points(reader, "docker_jobs.docker.up") == {attrs(): 0}
    [run] = runs_of(session_factory)
    assert [(attempt.try_number, attempt.state) for attempt in run.attempts] == [(1, "pending")]

    runner.unavailable = False
    scheduler.tick(T0 + HOUR + 10 * SECOND)

    [run] = runs_of(session_factory)
    assert [(attempt.try_number, attempt.state) for attempt in run.attempts] == [(1, "running")]
    assert points(reader, "docker_jobs.docker.up") == {attrs(): 1}
    assert points(reader, "docker_jobs.scheduler.heartbeat") == {
        attrs(): (T0 + HOUR + 10 * SECOND).timestamp()
    }


def test_the_gauges_are_refreshed_every_tick(scheduler, session, reader):
    make_job(session)

    scheduler.tick(T0 + HOUR)

    assert points(reader, "docker_jobs.runs.active")[attrs(docker_job="collect", state="running")] == 1


def test_the_sweep_keeps_the_service_of_a_pending_attempt(scheduler, session, runner):
    job = make_job(session)
    run = make_run(job)
    session.add(run)
    session.commit()
    # the process died after creating the service, before recording it
    runner.services["svc-early"] = run.attempts[0].token
    runner.services["svc-orphan"] = "not-a-token"

    scheduler.reconcile(T0 + HOUR)

    assert runner.removed == ["svc-orphan"]


def test_the_thread_survives_a_failing_tick():
    ticks = []

    class Flaky:
        def tick(self):
            ticks.append(1)
            if len(ticks) == 1:
                raise RuntimeError("boom")

    thread = SchedulerThread(Flaky(), interval_seconds=0.01)
    thread.start()
    deadline = time.monotonic() + 5
    while len(ticks) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    thread.stop()

    assert len(ticks) >= 3
    assert not thread.is_alive()


def test_one_scheduler_per_database(tmp_path):
    path = str(tmp_path / "scheduler.lock")

    with exclusive_lock(path):
        with pytest.raises(RuntimeError, match="another scheduler"):
            with exclusive_lock(path):
                pass

    with exclusive_lock(path):
        pass


def test_one_broken_job_does_not_stop_the_others(scheduler, session, runner, reader):
    # stored before validation refused such schedules: every interval computation raises
    make_job(session, name="broken", schedule="0 0 30 2 *")
    make_job(session, name="good")

    scheduler.tick(T0 + HOUR)

    assert [spec.service_name for spec in runner.launched] == ["djobs_good_1_1"]
    assert points(reader, "docker_jobs.scheduler.heartbeat") == {attrs(): (T0 + HOUR).timestamp()}


def test_an_attempt_that_cannot_launch_does_not_stop_the_others(scheduler, session, session_factory, runner):
    make_job(session, name="aaa", image="broken")
    make_job(session, name="bbb")
    create = runner.launch

    def launch(spec):
        if spec.image == "broken":
            raise RuntimeError("unexpected")
        return create(spec)

    runner.launch = launch

    scheduler.tick(T0 + HOUR)

    assert [spec.service_name for spec in runner.launched] == ["djobs_bbb_2_1"]
    [run] = runs_of(session_factory, "aaa")
    assert run.attempts[0].state == "pending"
