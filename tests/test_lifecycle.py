"""A run's life: created, launched, finished, retried, rerun, cancelled"""

import pytest
from factories import HOUR, MINUTE, SECOND, T0, attrs, make_job, points

from docker_jobs_backend.db import Attempt, Run, RunType
from docker_jobs_backend.lifecycle import Conflict, Lifecycle, loki_selector, service_name
from docker_jobs_backend.runner import LaunchError, RunnerUnavailable
from docker_jobs_backend.schedule import Interval

SECRET = {"source": "cdse", "target": "", "uid": "10001", "gid": "10001", "mode": 0o400}


def new_run(session, lifecycle, **job_overrides):
    """the committed, queued run of a new job's first hour"""
    job = make_job(session, **job_overrides)
    run = lifecycle.create_run(session, job, Interval(T0, T0 + HOUR), RunType.SCHEDULED, T0 + HOUR)
    session.commit()
    return run


def launched(session, lifecycle, **job_overrides):
    run = new_run(session, lifecycle, **job_overrides)
    lifecycle.launch(session, run.attempts[0], T0 + HOUR)
    return run, run.attempts[0]


def test_a_new_run_waits_with_its_first_attempt(session, lifecycle, reader):
    run = new_run(session, lifecycle)

    assert (run.state, run.max_tries) == ("queued", 1)
    [attempt] = run.attempts
    assert (attempt.try_number, attempt.reason, attempt.state) == (1, "scheduled", "pending")
    assert points(reader, "docker_jobs.runs.created") == {
        attrs(docker_job="collect", run_type="scheduled"): 1
    }


def test_a_manual_run_says_who_asked_and_why(session, lifecycle):
    job = make_job(session)

    run = lifecycle.create_run(
        session, job, Interval(T0, T0 + HOUR), RunType.MANUAL, T0 + HOUR,
        requested_by="aagazzi", note="after the outage",
    )
    session.commit()

    [attempt] = run.attempts
    assert (attempt.reason, attempt.requested_by, attempt.note) == ("manual", "aagazzi", "after the outage")


def test_launch_renders_the_runs_interval_into_a_service(session, lifecycle, runner, reader):
    run = new_run(
        session, lifecycle,
        env={"IFACE": "{{ params.interface }}"}, params={"interface": "LTA_Acri"}, secrets=[SECRET],
    )
    attempt = run.attempts[0]

    lifecycle.launch(session, attempt, T0 + HOUR)

    [spec] = runner.launched
    assert spec.service_name == f"djobs_collect_{run.id}_1"
    assert spec.command == ["--start", "2026-10-09T00:00:00.000Z"]
    assert spec.env == {"IFACE": "LTA_Acri"}
    assert spec.secrets == [SECRET]
    assert spec.labels == {
        "docker-jobs.job": "collect",
        "docker-jobs.run": str(run.id),
        "docker-jobs.attempt": attempt.token,
    }
    assert (attempt.state, attempt.service_id, attempt.launched_at) == ("running", "svc-1", T0 + HOUR)
    assert attempt.rendered_command == ["--start", "2026-10-09T00:00:00.000Z"]
    assert (run.state, run.started_at) == ("running", T0 + HOUR)
    assert points(reader, "docker_jobs.attempts.started") == {
        attrs(docker_job="collect", reason="scheduled"): 1
    }


def test_a_spec_docker_refuses_fails_the_attempt(session, lifecycle, runner):
    runner.launch_error = LaunchError("swarm secret 'cdse-v3' does not exist")
    run = new_run(session, lifecycle)

    lifecycle.launch(session, run.attempts[0], T0 + HOUR)

    assert (run.attempts[0].state, run.attempts[0].error) == (
        "failed",
        "swarm secret 'cdse-v3' does not exist",
    )
    assert run.state == "failed"


def test_a_broken_template_fails_the_attempt_without_docker(session, lifecycle, runner):
    run = new_run(session, lifecycle, command=["{{ nope }}"])

    lifecycle.launch(session, run.attempts[0], T0 + HOUR)

    assert run.attempts[0].state == "failed"
    assert "nope" in run.attempts[0].error
    assert runner.launched == []


def test_docker_down_leaves_the_attempt_pending(session, session_factory, lifecycle, runner):
    runner.unavailable = True
    run = new_run(session, lifecycle)

    with pytest.raises(RunnerUnavailable):
        lifecycle.launch(session, run.attempts[0], T0 + HOUR)

    with session_factory() as other:
        assert other.get(Attempt, run.attempts[0].id).state == "pending"


def test_a_completed_service_succeeds_the_run_and_is_removed(session, lifecycle, runner, reader):
    run, attempt = launched(session, lifecycle)
    runner.finish("svc-1", exit_code=0)

    lifecycle.reconcile(session, attempt, T0 + HOUR + 5 * MINUTE)

    assert (attempt.state, attempt.exit_code, attempt.node_id) == ("success", 0, "node-1")
    assert (run.state, run.finished_at) == ("success", T0 + HOUR + 5 * MINUTE)
    assert runner.removed == ["svc-1"]
    assert points(reader, "docker_jobs.runs.finished") == {attrs(docker_job="collect", state="success"): 1}


def test_a_failure_with_a_try_left_queues_a_retry_after_the_delay(session, lifecycle, runner):
    run, attempt = launched(session, lifecycle, retries=2, retry_delay_seconds=600)
    runner.finish("svc-1", exit_code=2)

    lifecycle.reconcile(session, attempt, T0 + 2 * HOUR)

    assert (attempt.state, attempt.exit_code, attempt.error) == ("failed", 2, "task: non-zero exit (2)")
    assert (run.state, run.finished_at) == ("retrying", None)
    retry = run.attempts[-1]
    assert (retry.try_number, retry.reason, retry.state, retry.not_before) == (
        2,
        "retry",
        "pending",
        T0 + 2 * HOUR + 10 * MINUTE,
    )


def test_the_last_failure_fails_the_run(session, lifecycle, runner, reader):
    run, attempt = launched(session, lifecycle)
    runner.finish("svc-1", exit_code=1)

    lifecycle.reconcile(session, attempt, T0 + 2 * HOUR)

    assert run.state == "failed"
    assert len(run.attempts) == 1
    assert points(reader, "docker_jobs.runs.finished") == {attrs(docker_job="collect", state="failed"): 1}


def test_a_running_attempt_records_when_it_started(session, lifecycle, runner):
    _, attempt = launched(session, lifecycle)
    runner.start("svc-1")

    lifecycle.reconcile(session, attempt, T0 + HOUR + 30 * SECOND)

    assert (attempt.state, attempt.started_at, attempt.node_id) == ("running", T0 + HOUR + 30 * SECOND, "node-1")


def test_an_attempt_past_its_timeout_is_stopped(session, lifecycle, runner):
    run, attempt = launched(session, lifecycle, timeout_seconds=600)
    runner.start("svc-1")
    lifecycle.reconcile(session, attempt, T0 + HOUR + SECOND)

    lifecycle.reconcile(session, attempt, T0 + HOUR + 10 * MINUTE)

    assert (attempt.state, attempt.error) == ("timeout", "timed out after 600s")
    assert runner.removed == ["svc-1"]
    assert run.state == "failed"


def test_a_task_that_never_starts_times_out_early(session, lifecycle, runner, settings):
    settings.start_timeout_seconds = 300
    _, attempt = launched(session, lifecycle)

    lifecycle.reconcile(session, attempt, T0 + HOUR + 5 * MINUTE)

    assert attempt.state == "timeout"
    assert attempt.error.startswith("not started after 300s")


def test_a_vanished_service_loses_the_attempt(session, lifecycle, runner):
    _, attempt = launched(session, lifecycle)
    runner.services.clear()

    lifecycle.reconcile(session, attempt, T0 + HOUR + MINUTE)

    assert (attempt.state, attempt.error) == ("lost", "service no longer exists")


def test_a_finished_run_is_rerun_with_fresh_tries(session, lifecycle, runner):
    run, attempt = launched(session, lifecycle, retries=1)
    runner.finish("svc-1", exit_code=0)
    lifecycle.reconcile(session, attempt, T0 + 2 * HOUR)

    rerun = lifecycle.rerun(run, T0 + 3 * HOUR, requested_by="aagazzi", note="upstream fixed")
    session.commit()

    assert (rerun.try_number, rerun.reason, rerun.state, rerun.requested_by) == (2, "rerun", "pending", "aagazzi")
    assert (run.state, run.max_tries, run.finished_at) == ("queued", 3, None)


def test_only_a_finished_run_is_rerun(session, lifecycle):
    run = new_run(session, lifecycle)

    with pytest.raises(Conflict, match="queued"):
        lifecycle.rerun(run, T0 + 2 * HOUR)


def test_a_disabled_jobs_runs_are_not_rerun(session, lifecycle, runner):
    run, attempt = launched(session, lifecycle)
    runner.finish("svc-1", exit_code=0)
    lifecycle.reconcile(session, attempt, T0 + 2 * HOUR)
    run.job.enabled = False

    with pytest.raises(Conflict, match="disabled"):
        lifecycle.rerun(run, T0 + 3 * HOUR)


def test_cancelling_a_running_attempt_removes_its_service(session, lifecycle, runner, reader):
    run, attempt = launched(session, lifecycle, retries=3)

    lifecycle.cancel(session, run, T0 + HOUR + MINUTE, requested_by="operator", note="wrong window")

    assert (attempt.state, attempt.error) == ("cancelled", "cancelled by operator: wrong window")
    assert run.state == "cancelled"
    assert runner.removed == ["svc-1"]
    assert points(reader, "docker_jobs.runs.finished") == {attrs(docker_job="collect", state="cancelled"): 1}


def test_cancelling_a_queued_run_needs_no_docker(session, lifecycle, runner):
    run = new_run(session, lifecycle)
    runner.unavailable = True

    lifecycle.cancel(session, run, T0 + HOUR, requested_by="operator")

    assert run.state == "cancelled"


def test_an_ended_run_cannot_be_cancelled(session, lifecycle, runner):
    run, attempt = launched(session, lifecycle)
    runner.finish("svc-1", exit_code=0)
    lifecycle.reconcile(session, attempt, T0 + 2 * HOUR)

    with pytest.raises(Conflict, match="success"):
        lifecycle.cancel(session, run, T0 + 3 * HOUR)


def test_a_cancel_while_docker_creates_the_service_wins(session, session_factory, lifecycle, runner):
    run = new_run(session, lifecycle)
    attempt = run.attempts[0]
    create = runner.launch

    def create_while_the_operator_cancels(spec):
        service_id = create(spec)
        with session_factory() as other:
            other_lifecycle = Lifecycle(runner, lifecycle.metrics, lifecycle.settings)
            other_lifecycle.cancel(other, other.get(Run, run.id), T0 + HOUR, requested_by="operator")
        return service_id

    runner.launch = create_while_the_operator_cancels

    lifecycle.launch(session, attempt, T0 + HOUR)

    assert attempt.state == "cancelled"
    assert runner.removed == ["svc-1"]


def test_a_cancelled_attempt_is_not_reconciled_back(session, session_factory, lifecycle, runner):
    run, attempt = launched(session, lifecycle)
    with session_factory() as other:
        Lifecycle(runner, lifecycle.metrics, lifecycle.settings).cancel(
            other, other.get(Run, run.id), T0 + HOUR + MINUTE, requested_by="operator"
        )

    lifecycle.reconcile(session, attempt, T0 + HOUR + 2 * MINUTE)

    assert attempt.state == "cancelled"


def test_the_sweep_removes_services_whose_attempt_is_over_or_unknown(session, lifecycle, runner):
    _, attempt = launched(session, lifecycle)
    runner.services["svc-orphan"] = "not-a-token"

    assert lifecycle.sweep(session) == 1

    assert runner.removed == ["svc-orphan"]
    assert runner.services == {"svc-1": attempt.token}


def test_service_names_and_log_selectors():
    assert service_name("djobs", "collect", 42, 3) == "djobs_collect_42_3"
    assert loki_selector("djobs", "djobs_collect_42_3") == (
        '{swarm_stack="djobs", swarm_service="djobs_collect_42_3"}'
    )
