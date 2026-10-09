"""The runs API, through HTTP, driven by the scheduler on a fake runner"""

from factories import attrs, points

from docker_jobs_backend.db import utcnow

USER = {"X-Grafana-User": "aagazzi"}
OPERATOR = {"X-Grafana-User": "operator"}


def create_job(client, **overrides):
    body = {
        "name": "collect",
        "image": "127.0.0.1:8085/maas-collector:3.10.1",
        "command": ["--start", "{{ data_interval_start | iso_ms }}"],
        "schedule": "0 * * * *",
    }
    body.update(overrides)
    assert client.post("/api/v1/jobs", json=body, headers=USER).status_code == 201


def trigger(client, logical_date, job="collect"):
    response = client.post(f"/api/v1/jobs/{job}/trigger", json={"logical_date": logical_date}, headers=USER)
    assert response.status_code == 201, response.text
    return response.json()


def tick(client):
    """reconcile and launch, now (scheduling is the scheduler tests' business)"""
    scheduler = client.app.state.scheduler
    now = utcnow()
    scheduler.reconcile(now)
    scheduler.launch(now)


def test_runs_are_listed_newest_interval_first(client):
    create_job(client)
    for start in ("2026-10-01T00:00:00Z", "2026-10-01T02:00:00Z", "2026-10-01T01:00:00Z"):
        trigger(client, start)

    runs = client.get("/api/v1/runs").json()

    assert [run["logical_date"] for run in runs] == [
        "2026-10-01T02:00:00Z",
        "2026-10-01T01:00:00Z",
        "2026-10-01T00:00:00Z",
    ]
    assert runs[0]["last_attempt"]["state"] == "pending"
    assert runs[0]["attempts"] is None


def test_runs_filter_by_job_and_state_and_page(client):
    create_job(client)
    create_job(client, name="other")
    for start in ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z", "2026-10-01T02:00:00Z"):
        trigger(client, start)
    trigger(client, "2026-10-01T00:00:00Z", job="other")

    assert len(client.get("/api/v1/runs").json()) == 4
    assert len(client.get("/api/v1/runs", params={"job": "collect", "size": 2}).json()) == 2
    page = client.get("/api/v1/runs", params={"job": "collect", "offset": 2}).json()
    assert [run["logical_date"] for run in page] == ["2026-10-01T00:00:00Z"]
    assert client.get("/api/v1/runs", params={"state": "success"}).json() == []
    assert client.get("/api/v1/runs", params={"state": "bogus"}).status_code == 422


def test_a_run_shows_its_attempts_and_where_their_logs_are(client):
    create_job(client)
    run = trigger(client, "2026-10-01T00:00:00Z")
    tick(client)

    detail = client.get(f"/api/v1/runs/{run['id']}").json()

    [attempt] = detail["attempts"]
    assert attempt["state"] == "running"
    assert attempt["rendered_command"] == ["--start", "2026-10-01T00:00:00.000Z"]
    assert attempt["service_name"] == f"djobs_collect_{run['id']}_1"
    assert attempt["loki_selector"] == (
        f'{{swarm_stack="djobs", swarm_service="djobs_collect_{run["id"]}_1"}}'
    )


def test_a_missing_run_is_a_404(client):
    response = client.get("/api/v1/runs/999")

    assert response.status_code == 404
    assert response.json()["detail"] == "no run 999"


def test_a_failed_run_is_rerun_by_whoever_asks(client, runner):
    create_job(client)
    run = trigger(client, "2026-10-01T00:00:00Z")
    tick(client)
    runner.finish("svc-1", exit_code=1)
    tick(client)
    assert client.get(f"/api/v1/runs/{run['id']}").json()["state"] == "failed"

    response = client.post(f"/api/v1/runs/{run['id']}/rerun", json={"note": "upstream fixed"}, headers=OPERATOR)

    assert response.status_code == 200
    rerun = response.json()
    assert rerun["state"] == "queued"
    last = rerun["attempts"][-1]
    assert (last["try_number"], last["reason"], last["requested_by"], last["note"], last["state"]) == (
        2,
        "rerun",
        "operator",
        "upstream fixed",
        "pending",
    )
    tick(client)
    assert client.get(f"/api/v1/runs/{run['id']}").json()["state"] == "running"


def test_a_successful_run_can_be_rerun_too(client, runner):
    create_job(client)
    run = trigger(client, "2026-10-01T00:00:00Z")
    tick(client)
    runner.finish("svc-1", exit_code=0)
    tick(client)

    assert client.post(f"/api/v1/runs/{run['id']}/rerun").status_code == 200


def test_a_run_in_progress_is_not_rerun(client):
    create_job(client)
    run = trigger(client, "2026-10-01T00:00:00Z")

    response = client.post(f"/api/v1/runs/{run['id']}/rerun")

    assert response.status_code == 409
    assert "queued" in response.json()["detail"]


def test_a_running_run_is_cancelled(client, runner):
    create_job(client)
    run = trigger(client, "2026-10-01T00:00:00Z")
    tick(client)

    response = client.post(f"/api/v1/runs/{run['id']}/cancel", json={"note": "wrong window"}, headers=OPERATOR)

    assert response.status_code == 200
    body = response.json()
    assert body["state"] == "cancelled"
    assert body["attempts"][-1]["error"] == "cancelled by operator: wrong window"
    assert runner.removed == ["svc-1"]
    assert client.post(f"/api/v1/runs/{run['id']}/cancel").status_code == 409


def test_from_trigger_to_success(client, runner, reader):
    create_job(client)
    run = trigger(client, "2026-10-01T00:00:00Z")
    tick(client)
    runner.finish("svc-1", exit_code=0)
    tick(client)

    detail = client.get(f"/api/v1/runs/{run['id']}").json()

    assert detail["state"] == "success"
    assert detail["attempts"][0]["exit_code"] == 0
    assert runner.removed == ["svc-1"]
    assert points(reader, "docker_jobs.runs.finished")[attrs(docker_job="collect", state="success")] == 1
