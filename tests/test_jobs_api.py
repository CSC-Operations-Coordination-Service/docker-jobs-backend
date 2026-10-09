"""The jobs API, through HTTP, on a fake runner"""

import datetime

import pytest
from factories import attrs, points
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from docker_jobs_backend.db import Run, utcnow
from docker_jobs_backend.schedule import latest_complete_interval

USER = {"X-Grafana-User": "aagazzi"}
COMMAND = ["maas_collector.rawdata.cli.odata", "--replay-start-date", "{{ data_interval_start | iso_ms }}"]


def job_body(**overrides):
    body = {
        "name": "collect",
        "image": "127.0.0.1:8085/maas-collector:3.10.1",
        "command": COMMAND,
        "schedule": "0 * * * *",
    }
    body.update(overrides)
    return body


def spec_body(**overrides):
    """a PUT body: a job without its name"""
    body = job_body(**overrides)
    del body["name"]
    return body


def create(client, **overrides):
    response = client.post("/api/v1/jobs", json=job_body(**overrides), headers=USER)
    assert response.status_code == 201, response.text
    return response.json()


def when(text):
    return datetime.datetime.fromisoformat(text)


def test_a_job_is_created_with_the_house_defaults(client):
    job = create(client)

    assert job["name"] == "collect"
    assert (job["enabled"], job["catchup"], job["max_active_runs"], job["retries"], job["timeout_seconds"]) == (
        True,
        True,
        1,
        0,
        3600,
    )
    assert job["updated_by"] == "aagazzi"
    start = when(job["start_date"])
    assert (start.minute, start.second) == (0, 0) and start <= utcnow()
    assert when(job["next_fire_at"]) > utcnow()


def test_secrets_default_to_the_app_user(client):
    job = create(client, secrets=[{"source": "cdse"}])

    assert job["secrets"] == [{"source": "cdse", "target": "", "uid": "10001", "gid": "10001", "mode": 0o400}]


def test_a_taken_name_is_a_conflict(client):
    create(client)

    assert client.post("/api/v1/jobs", json=job_body()).status_code == 409


@pytest.mark.parametrize("name", ["Collect", "collect_hourly", "-collect", "collect-", "x" * 33, ""])
def test_a_name_must_fit_in_a_service_name(client, name):
    assert client.post("/api/v1/jobs", json=job_body(name=name)).status_code == 422


@pytest.mark.parametrize("schedule", ["@hourly", "0 * * *", "0 0 * * * *", "61 * * * *"])
def test_only_five_field_cron(client, schedule):
    assert client.post("/api/v1/jobs", json=job_body(schedule=schedule)).status_code == 422


def test_a_template_error_is_a_422_naming_the_field(client):
    response = client.post("/api/v1/jobs", json=job_body(command=["--start", "{{ data_interval_begin }}"]))

    assert response.status_code == 422
    assert "command[1]" in response.text


def test_a_naive_start_date_is_a_422(client):
    response = client.post("/api/v1/jobs", json=job_body(start_date="2026-10-09T00:00:00"))

    assert response.status_code == 422
    assert "timezone" in response.text


def test_environment_names_are_checked(client):
    assert client.post("/api/v1/jobs", json=job_body(env={"BAD-NAME": "x"})).status_code == 422


def test_end_date_comes_after_start_date(client):
    body = job_body(start_date="2026-10-09T00:00:00Z", end_date="2026-10-08T00:00:00Z")

    assert client.post("/api/v1/jobs", json=body).status_code == 422


def test_bind_mounts_must_be_allowed(settings, make_app):
    bind = [{"type": "bind", "source": "/etc", "target": "/host-etc"}]

    with TestClient(make_app()) as client:
        assert client.post("/api/v1/jobs", json=job_body(mounts=bind)).status_code == 422

    settings.allow_bind_mounts = True
    with TestClient(make_app()) as client:
        assert client.post("/api/v1/jobs", json=job_body(mounts=bind)).status_code == 201


def test_list_and_get(client):
    create(client, name="b-job")
    create(client, name="a-job")

    assert [job["name"] for job in client.get("/api/v1/jobs").json()] == ["a-job", "b-job"]
    assert client.get("/api/v1/jobs/a-job").json()["name"] == "a-job"
    assert client.get("/api/v1/jobs/nope").status_code == 404


def test_update_replaces_the_definition_and_keeps_the_start(client):
    job = create(client)

    response = client.put(
        "/api/v1/jobs/collect", json=spec_body(command=["--once"]), headers={"X-Grafana-User": "someone-else"}
    )

    assert response.status_code == 200
    updated = response.json()
    assert updated["command"] == ["--once"]
    assert updated["start_date"] == job["start_date"]
    assert updated["updated_by"] == "someone-else"


def test_the_name_cannot_change(client):
    create(client)

    assert client.put("/api/v1/jobs/collect", json=job_body(name="renamed")).status_code == 422


def test_delete_removes_the_job_and_its_runs(client, session_factory):
    create(client)
    client.post("/api/v1/jobs/collect/trigger", json={"logical_date": "2026-10-01T00:00:00Z"})

    assert client.delete("/api/v1/jobs/collect").status_code == 204

    assert client.get("/api/v1/jobs/collect").status_code == 404
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Run)) == 0


def test_a_job_with_a_running_attempt_is_not_deleted(client):
    create(client)
    client.post("/api/v1/jobs/collect/trigger", json={"logical_date": "2026-10-01T00:00:00Z"})
    client.app.state.scheduler.launch(utcnow())

    assert client.delete("/api/v1/jobs/collect").status_code == 409


def test_a_new_job_counts_from_zero(client, reader):
    create(client)

    assert points(reader, "docker_jobs.runs.finished")[attrs(docker_job="collect", state="failed")] == 0


def test_trigger_runs_the_latest_ended_interval_by_default(client):
    create(client)

    before = latest_complete_interval("0 * * * *", utcnow()).start
    response = client.post("/api/v1/jobs/collect/trigger", json={"note": "check"}, headers=USER)
    after = latest_complete_interval("0 * * * *", utcnow()).start

    assert response.status_code == 201
    run = response.json()
    assert when(run["logical_date"]) in {before, after}
    assert (run["run_type"], run["state"], run["requested_by"], run["note"]) == ("manual", "queued", "aagazzi", "check")
    [attempt] = run["attempts"]
    assert (attempt["reason"], attempt["state"]) == ("manual", "pending")


def test_trigger_snaps_a_date_to_its_interval_and_may_repeat_it(client):
    create(client)

    first = client.post("/api/v1/jobs/collect/trigger", json={"logical_date": "2026-10-01T00:20:00Z"})
    second = client.post("/api/v1/jobs/collect/trigger", json={"logical_date": "2026-10-01T00:20:00Z"})

    assert (first.status_code, second.status_code) == (201, 201)
    for run in (first.json(), second.json()):
        assert (run["logical_date"], run["data_interval_end"]) == ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z")
    assert first.json()["id"] != second.json()["id"]


def test_an_interval_that_has_not_ended_is_not_triggered(client):
    create(client)

    response = client.post("/api/v1/jobs/collect/trigger", json={"logical_date": utcnow().isoformat()})

    assert response.status_code == 422


def test_a_disabled_job_is_not_triggered(client):
    create(client, enabled=False)

    assert client.post("/api/v1/jobs/collect/trigger").status_code == 409


def test_a_naive_logical_date_is_a_422(client):
    create(client)

    response = client.post("/api/v1/jobs/collect/trigger", json={"logical_date": "2026-10-09T00:00:00"})

    assert response.status_code == 422


def test_preview_renders_and_checks_swarm_objects(client, runner):
    runner.existing_objects = {"ocs_rabbitmq"}

    response = client.post(
        "/api/v1/preview",
        json={
            "job": job_body(secrets=[{"source": "cdse"}], networks=["ocs_rabbitmq"]),
            "logical_date": "2026-10-01T00:30:00Z",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["command"] == ["maas_collector.rawdata.cli.odata", "--replay-start-date", "2026-10-01T00:00:00.000Z"]
    assert (body["data_interval_start"], body["data_interval_end"]) == ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z")
    assert body["problems"] == ["secret 'cdse' does not exist"]
    assert len(body["next_fire_times"]) == 5


def test_preview_without_docker_says_so(client, runner):
    runner.unavailable = True

    body = client.post("/api/v1/preview", json={"job": job_body()}).json()

    assert body["problems"] == ["swarm objects not checked: docker unreachable: connection refused"]


def test_the_backends_own_network_and_volume_are_refused(client):
    socket = client.post("/api/v1/jobs", json=job_body(networks=["docker-jobs-socket"]))
    data = client.post("/api/v1/jobs", json=job_body(mounts=[{"source": "docker-jobs-data", "target": "/x"}]))

    assert (socket.status_code, data.status_code) == (422, 422)
    assert "backend's own" in socket.text and "backend's own" in data.text
