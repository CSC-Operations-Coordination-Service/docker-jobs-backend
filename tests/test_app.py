"""The application shell: health, the caller, the API token"""

import time

import pytest
from factories import HOUR, attrs, make_job, points
from fastapi.testclient import TestClient

from docker_jobs_backend.db import utcnow
from docker_jobs_backend.main import create_app
from docker_jobs_backend.schedule import interval_containing
from docker_jobs_backend.scheduler import exclusive_lock


def test_health_answers(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_the_caller_is_the_grafana_user(client):
    response = client.get("/api/v1/whoami", headers={"X-Grafana-User": "aagazzi"})

    assert response.json() == {"login": "aagazzi"}


def test_without_the_header_the_caller_is_anonymous(client):
    assert client.get("/api/v1/whoami").json() == {"login": "anonymous"}


def test_with_a_token_set_the_api_requires_it(settings, make_app):
    settings.api_token = "s3cret"

    with TestClient(make_app()) as client:
        assert client.get("/api/v1/whoami").status_code == 401
        wrong = {"Authorization": "Bearer nope"}
        assert client.get("/api/v1/whoami", headers=wrong).status_code == 401
        right = {"Authorization": "Bearer s3cret"}
        assert client.get("/api/v1/whoami", headers=right).status_code == 200
        assert client.get("/health").status_code == 200


def test_startup_registers_existing_jobs(session_factory, make_app, reader):
    with session_factory() as session:
        make_job(session)

    with TestClient(make_app()):
        assert points(reader, "docker_jobs.runs.finished") == {
            attrs(docker_job="collect", state=state): 0 for state in ("success", "failed", "cancelled")
        }


def test_the_scheduler_thread_runs_with_the_app(settings, session_factory, runner, metrics):
    settings.tick_seconds = 0.05
    with session_factory() as session:
        make_job(session, start_date=interval_containing("0 * * * *", utcnow() - 2 * HOUR).start)
    app = create_app(
        settings, session_factory=session_factory, runner=runner, metrics=metrics, start_scheduler=True
    )

    with TestClient(app) as client:
        deadline = time.monotonic() + 5
        while client.get("/health").json()["last_tick"] is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert client.get("/health").json()["last_tick"] is not None

    assert runner.launched


def test_a_second_scheduler_on_the_same_database_refuses_to_start(settings, session_factory, runner, metrics):
    app = create_app(
        settings, session_factory=session_factory, runner=runner, metrics=metrics, start_scheduler=True
    )

    with exclusive_lock(settings.lock_file):
        with pytest.raises(RuntimeError, match="another scheduler"):
            with TestClient(app):
                pass
