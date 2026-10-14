"""SwarmRunner against a fake docker-py client: the services it creates, the states it reads"""

import types

import docker
import pytest
import requests

from docker_jobs_backend.runner import (
    ATTEMPT_LABEL,
    COMPLETE,
    FAILED,
    GONE,
    MANAGED_LABEL,
    PENDING,
    RUNNING,
    STACK_LABEL,
    LaunchError,
    LaunchSpec,
    RunnerUnavailable,
    SwarmRunner,
    TaskStatus,
)
from docker_jobs_backend.settings import Settings


def api_error(status, explanation=""):
    response = types.SimpleNamespace(
        status_code=status, reason="Reason", url="http://socket-proxy:2375/services/create"
    )
    return docker.errors.APIError(str(status), response=response, explanation=explanation)


class FakeObjects:
    """client.secrets, client.configs, client.networks"""

    def __init__(self, **ids_by_name):
        self.items = [types.SimpleNamespace(id=i, name=n) for n, i in ids_by_name.items()]

    def list(self, filters=None):
        if filters and "name" in filters:
            # like the daemon: a prefix match; exactness is the caller's job
            return [item for item in self.items if item.name.startswith(filters["name"])]
        return list(self.items)

    def get(self, reference):
        # like the daemon: a full id, an exact name, or a unique id prefix
        for match in (
            [item for item in self.items if item.id == reference],
            [item for item in self.items if item.name == reference],
            [item for item in self.items if item.id.startswith(reference)],
        ):
            if len(match) == 1:
                return match[0]
        raise docker.errors.NotFound(f"network {reference} not found")


class FakeServices:
    def __init__(self):
        self.created = []
        self.existing = {}  # name -> (service id, labels)
        self.error = None

    def create(self, image, command=None, **kwargs):
        if self.error is not None:
            raise self.error
        kwargs.update(image=image, command=command)
        self.created.append(kwargs)
        return types.SimpleNamespace(id=f"svc-{len(self.created)}")

    def get(self, name):
        service_id, labels = self.existing[name]
        return types.SimpleNamespace(id=service_id, attrs={"Spec": {"Labels": labels}})


class FakeAPI:
    def __init__(self):
        self.tasks_by_service = {}
        self.existing = set()
        self.listed = []
        self.removed = []
        self.filters = None
        self.error = None

    def tasks(self, filters=None):
        if self.error is not None:
            raise self.error
        return self.tasks_by_service.get(filters["service"], [])

    def inspect_service(self, service_id):
        if service_id not in self.existing:
            raise docker.errors.NotFound("no such service")
        return {"ID": service_id}

    def remove_service(self, service_id):
        if service_id not in self.existing:
            raise docker.errors.NotFound("no such service")
        self.existing.discard(service_id)
        self.removed.append(service_id)

    def services(self, filters=None):
        self.filters = filters
        return self.listed


class FakeClient:
    def __init__(self):
        self.services = FakeServices()
        self.secrets = FakeObjects(**{"cdse": "sec-1", "cdse-v2": "sec-2"})
        self.configs = FakeObjects(**{"odata-conf": "cfg-1"})
        self.networks = FakeObjects(**{"ocs_rabbitmq": "net-1", "docker-jobs-socket": "sock-9", "host": "host-0"})
        self.api = FakeAPI()


@pytest.fixture(name="docker_client")
def docker_client_fixture():
    return FakeClient()


@pytest.fixture(name="swarm")
def swarm_fixture(docker_client):
    return SwarmRunner(Settings(allowed_networks=["ocs_rabbitmq"]), client_factory=lambda: docker_client)


def spec(**overrides):
    values = dict(
        service_name="djobs_collect_42_1",
        image="127.0.0.1:8085/maas-collector:3.10.1",
        entrypoint=None,
        command=["maas_collector.rawdata.cli.odata", "--replay-start-date", "2026-10-08T00:00:00.000Z"],
        env={"ES_URL": "http://es:9200"},
        labels={ATTEMPT_LABEL: "token-7"},
    )
    values.update(overrides)
    return LaunchSpec(**values)


SECRET = {"source": "cdse", "target": "", "uid": "10001", "gid": "10001", "mode": 0o400}


def test_an_attempt_is_a_one_shot_replicated_job(swarm, docker_client):
    assert swarm.launch(spec()) == "svc-1"

    [created] = docker_client.services.created
    assert created["name"] == "djobs_collect_42_1"
    assert created["mode"] == {"ReplicatedJob": {"MaxConcurrent": 1, "TotalCompletions": 1}}
    assert created["restart_policy"]["Condition"] == "none"


def test_the_image_healthcheck_is_off_a_job_is_judged_by_its_exit_code(swarm, docker_client):
    # the maas images probe localhost:8080, which a one-shot command never
    # serves: inherited, the probe has swarm kill the task as unhealthy
    swarm.launch(spec())

    [created] = docker_client.services.created
    assert created["healthcheck"]["Test"] == ["NONE"]


def test_every_job_logs_through_the_house_loki_block(swarm, docker_client):
    swarm.launch(spec())

    [created] = docker_client.services.created
    assert created["log_driver"] == "loki"
    assert created["log_driver_options"] == {
        "loki-url": "http://127.0.0.1:3100/loki/api/v1/push",
        "loki-retries": "5",
        "loki-batch-size": "400",
        "mode": "non-blocking",
    }


def test_the_command_is_the_cmd_and_the_entrypoint_is_kept_unless_given(swarm, docker_client):
    swarm.launch(spec())
    swarm.launch(spec(service_name="djobs_collect_42_2", entrypoint=["python", "-m"]))

    first, second = docker_client.services.created
    assert first["command"] is None
    assert first["args"] == [
        "maas_collector.rawdata.cli.odata",
        "--replay-start-date",
        "2026-10-08T00:00:00.000Z",
    ]
    assert second["command"] == ["python", "-m"]
    assert first["env"] == ["ES_URL=http://es:9200"]


def test_labels_put_the_service_in_the_runs_stack(swarm, docker_client):
    swarm.launch(spec())

    [created] = docker_client.services.created
    for labels in (created["labels"], created["container_labels"]):
        assert labels[STACK_LABEL] == "djobs"
        assert labels[MANAGED_LABEL] == "true"
        assert labels[ATTEMPT_LABEL] == "token-7"


def test_secrets_and_configs_are_resolved_by_exact_name(swarm, docker_client):
    config = {"source": "odata-conf", "target": "/conf/collector/odata.json", "uid": "10001", "gid": "10001", "mode": 0o444}

    swarm.launch(spec(secrets=[SECRET], configs=[config]))

    [created] = docker_client.services.created
    assert created["secrets"] == [
        {"SecretName": "cdse", "SecretID": "sec-1", "File": {"Name": "cdse", "UID": "10001", "GID": "10001", "Mode": 0o400}}
    ]
    assert created["configs"] == [
        {"ConfigName": "odata-conf", "ConfigID": "cfg-1", "File": {"Name": "/conf/collector/odata.json", "UID": "10001", "GID": "10001", "Mode": 0o444}}
    ]


def test_a_missing_secret_fails_the_launch_by_name(swarm):
    with pytest.raises(LaunchError, match="'cdse-v3'"):
        swarm.launch(spec(secrets=[{**SECRET, "source": "cdse-v3"}]))


def test_networks_constraints_mounts_and_limits_are_passed_on(swarm, docker_client):
    swarm.launch(
        spec(
            networks=["ocs_rabbitmq"],
            constraints=["node.labels.role == maas"],
            mounts=[{"type": "volume", "source": "dm-work", "target": "/work", "read_only": False}],
            cpu_limit=0.5,
            memory_limit_mb=256,
        )
    )

    [created] = docker_client.services.created
    assert created["networks"] == ["ocs_rabbitmq"]
    assert created["constraints"] == ["node.labels.role == maas"]
    assert created["mounts"] == [{"Target": "/work", "Source": "dm-work", "Type": "volume", "ReadOnly": False}]
    assert created["resources"] == {"Limits": {"NanoCPUs": 500_000_000, "MemoryBytes": 256 * 1024 * 1024}}


def test_a_name_conflict_adopts_our_own_service(swarm, docker_client):
    docker_client.services.existing["djobs_collect_42_1"] = ("svc-old", {ATTEMPT_LABEL: "token-7"})
    docker_client.services.error = api_error(409, "name conflicts with an existing object")

    assert swarm.launch(spec()) == "svc-old"


def test_a_name_conflict_with_a_stranger_is_a_launch_error(swarm, docker_client):
    docker_client.services.existing["djobs_collect_42_1"] = ("svc-x", {ATTEMPT_LABEL: "another"})
    docker_client.services.error = api_error(409, "name conflicts with an existing object")

    with pytest.raises(LaunchError, match="another attempt"):
        swarm.launch(spec())


def test_a_refused_spec_is_a_launch_error(swarm, docker_client):
    docker_client.services.error = api_error(400, "invalid reference format")

    with pytest.raises(LaunchError, match="invalid reference format"):
        swarm.launch(spec())


def test_an_unavailable_daemon_is_not_the_jobs_fault(swarm, docker_client):
    docker_client.services.error = api_error(503)

    with pytest.raises(RunnerUnavailable):
        swarm.launch(spec())


def test_a_daemon_that_never_answered_is_asked_again(docker_client):
    calls = []

    def factory():
        calls.append(1)
        if len(calls) == 1:
            raise docker.errors.DockerException("Error while fetching server API version")
        return docker_client

    swarm = SwarmRunner(Settings(), client_factory=factory)

    with pytest.raises(RunnerUnavailable):
        swarm.launch(spec())
    assert swarm.launch(spec()) == "svc-1"


def test_a_dropped_connection_makes_a_new_client(docker_client):
    made = []

    def factory():
        made.append(1)
        return docker_client

    swarm = SwarmRunner(Settings(), client_factory=factory)
    docker_client.api.error = requests.exceptions.ConnectionError("connection refused")

    with pytest.raises(RunnerUnavailable):
        swarm.status("svc-1")
    docker_client.api.error = None
    swarm.status("svc-1")

    assert len(made) == 2


def task(state, index=1, exit_code=None, err=None, node="node-1"):
    status = {"State": state, "Message": state}
    if exit_code is not None:
        status["ContainerStatus"] = {"ExitCode": exit_code}
    if err:
        status["Err"] = err
    return {"ID": f"task-{index}", "NodeID": node, "Version": {"Index": index}, "Status": status}


@pytest.mark.parametrize(
    "tasks, expected",
    [
        ([task("complete", exit_code=0)], TaskStatus(COMPLETE, exit_code=0, node_id="node-1")),
        (
            [task("failed", exit_code=2, err="task: non-zero exit (2)")],
            TaskStatus(FAILED, exit_code=2, error="task: non-zero exit (2)", node_id="node-1"),
        ),
        ([task("rejected", err="No such image: x")], TaskStatus(FAILED, error="No such image: x", node_id="node-1")),
        ([task("shutdown")], TaskStatus(FAILED, error="task shutdown: shutdown", node_id="node-1")),
        ([task("running")], TaskStatus(RUNNING, node_id="node-1")),
        ([task("preparing")], TaskStatus(PENDING, node_id="node-1")),
        (
            [task("pending", node="", err="no suitable node (scheduling constraints not satisfied)")],
            TaskStatus(PENDING, error="no suitable node (scheduling constraints not satisfied)"),
        ),
        (
            [task("failed", index=1, exit_code=1, err="x"), task("complete", index=5, exit_code=0)],
            TaskStatus(COMPLETE, exit_code=0, node_id="node-1"),
        ),
    ],
)
def test_the_latest_task_decides(swarm, docker_client, tasks, expected):
    docker_client.api.tasks_by_service["svc-1"] = tasks

    assert swarm.status("svc-1") == expected


def test_a_service_without_tasks_is_pending_until_it_is_gone(swarm, docker_client):
    docker_client.api.existing.add("svc-1")
    assert swarm.status("svc-1").state == PENDING

    docker_client.api.existing.clear()
    assert swarm.status("svc-1") == TaskStatus(GONE, error="service no longer exists")


def test_remove_tolerates_a_service_already_gone(swarm, docker_client):
    docker_client.api.existing.add("svc-1")

    swarm.remove("svc-1")
    swarm.remove("svc-1")

    assert docker_client.api.removed == ["svc-1"]


def test_managed_services_are_this_stacks_by_token(swarm, docker_client):
    docker_client.api.listed = [
        {"ID": "svc-1", "Spec": {"Labels": {ATTEMPT_LABEL: "token-7"}}},
        {"ID": "svc-2", "Spec": {"Labels": {}}},
    ]

    assert swarm.managed_services() == {"svc-1": "token-7", "svc-2": ""}
    assert docker_client.api.filters == {"label": [f"{MANAGED_LABEL}=true", f"{STACK_LABEL}=djobs"]}


def test_missing_objects_lists_what_a_job_would_not_find(swarm):
    assert swarm.missing_objects(["cdse", "nope"], ["odata-conf"], ["ocs_rabbitmq", "ghost"]) == [
        "secret 'nope' does not exist",
        "network 'ghost' does not exist",
    ]


def test_an_image_reference_docker_py_refuses_is_the_jobs_fault(swarm, docker_client):
    # docker-py raises this before any HTTP call: not a transport failure
    docker_client.services.error = docker.errors.InvalidRepository(
        "Repository name cannot contain a scheme (https://nexus/img:1)"
    )

    with pytest.raises(LaunchError, match="cannot contain a scheme"):
        swarm.launch(spec(image="https://nexus/img:1"))


def test_the_backends_own_network_is_refused_however_it_is_named(swarm):
    for reference in ("docker-jobs-socket", "sock-9", "so"):
        with pytest.raises(LaunchError, match="the backend's own"):
            swarm.launch(spec(networks=[reference]))


def test_a_network_outside_the_allow_list_is_refused_however_it_is_named(swarm):
    for reference in ("host", "host-0", "ho"):
        with pytest.raises(LaunchError, match="'host' is not allowed"):
            swarm.launch(spec(networks=[reference]))


def test_an_unknown_network_fails_the_launch(swarm):
    with pytest.raises(LaunchError, match="'ghost' does not exist"):
        swarm.launch(spec(networks=["ghost"]))
