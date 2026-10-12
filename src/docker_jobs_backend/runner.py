"""Docker Swarm: one replicated-job service per attempt

An attempt is a service in replicated-job mode: one task, run to completion,
never restarted by Swarm. Retries are the scheduler's -- each one a new
service -- so every try keeps its own exit code, node and logs. A service is
removed once its outcome is recorded; its logs are in Loki by then.

Every service is labelled for three readers:

* the loki driver turns com.docker.stack.namespace into swarm_stack and the
  service name into swarm_service: one try's logs are
  {swarm_stack="djobs", swarm_service="djobs_<job>_<run>_<try>"};
* `docker stack services djobs` lists what runs now;
* the sweep (Lifecycle.sweep) finds them by docker-jobs.attempt -- the
  attempt's token -- and removes those whose attempt is over.

Two kinds of failure. LaunchError: this spec cannot run (unknown secret, bad
image reference); the attempt fails. RunnerUnavailable: Docker cannot be
reached (socket-proxy restarting, manager rebooting); nothing is decided and
the scheduler tries again next tick.
"""

import contextlib
import dataclasses
import logging
from typing import Callable, Dict, Iterator, List, Optional, Protocol, Sequence

import docker
import requests
from docker.types import (
    ConfigReference,
    Healthcheck,
    Mount,
    Resources,
    RestartPolicy,
    SecretReference,
    ServiceMode,
)

from docker_jobs_backend.settings import Settings

__all__ = [
    "ATTEMPT_LABEL",
    "COMPLETE",
    "FAILED",
    "GONE",
    "JOB_LABEL",
    "LOG_DRIVER",
    "LaunchError",
    "LaunchSpec",
    "MANAGED_LABEL",
    "PENDING",
    "RUNNING",
    "RUN_LABEL",
    "Runner",
    "RunnerUnavailable",
    "STACK_LABEL",
    "SwarmRunner",
    "TaskStatus",
]

LOGGER = logging.getLogger(__name__)

LOG_DRIVER = "loki"
STACK_LABEL = "com.docker.stack.namespace"
MANAGED_LABEL = "docker-jobs.managed"
JOB_LABEL = "docker-jobs.job"
RUN_LABEL = "docker-jobs.run"
ATTEMPT_LABEL = "docker-jobs.attempt"

# TaskStatus.state
PENDING = "pending"  # not running yet: scheduling, pulling, starting
RUNNING = "running"
COMPLETE = "complete"  # exited 0
FAILED = "failed"  # exited non-zero, rejected, shut down
GONE = "gone"  # the service no longer exists

_FAILED_TASK_STATES = frozenset({"failed", "rejected", "shutdown", "orphaned", "remove"})
# what socket-proxy (HAProxy) answers while the daemon is away
_UNAVAILABLE_STATUSES = frozenset({502, 503, 504})


class LaunchError(Exception):
    """this spec cannot run: the attempt fails"""


class RunnerUnavailable(Exception):
    """Docker cannot be reached: decide nothing, try again next tick"""


@dataclasses.dataclass(frozen=True)
class LaunchSpec:
    """one attempt, rendered, as the runner needs it

    secrets, configs and mounts are dumped schemas.SecretRef, ConfigRef and
    MountSpec: plain dicts, as the jobs table stores them.
    """

    service_name: str
    image: str
    entrypoint: Optional[List[str]]
    command: List[str]
    env: Dict[str, str]
    labels: Dict[str, str]
    networks: Sequence[str] = ()
    constraints: Sequence[str] = ()
    secrets: Sequence[dict] = ()
    configs: Sequence[dict] = ()
    mounts: Sequence[dict] = ()
    cpu_limit: Optional[float] = None
    memory_limit_mb: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class TaskStatus:
    """where an attempt's service is, and what Swarm said about it"""

    state: str
    exit_code: Optional[int] = None
    error: Optional[str] = None
    node_id: Optional[str] = None


class Runner(Protocol):
    """what the lifecycle needs from an orchestrator"""

    def launch(self, spec: LaunchSpec) -> str:
        """create the attempt's service; its id"""

    def status(self, service_id: str) -> TaskStatus:
        """the state of the service's task"""

    def remove(self, service_id: str) -> None:
        """remove the service, if it still exists"""

    def managed_services(self) -> Dict[str, str]:
        """this stack's job services: service id -> attempt token"""

    def missing_objects(
        self, secrets: Sequence[str], configs: Sequence[str], networks: Sequence[str]
    ) -> List[str]:
        """one line per secret, config or network that does not exist"""


def _docker_from_env() -> docker.DockerClient:
    """DOCKER_HOST's daemon, with a timeout well under a tick's patience"""
    return docker.from_env(timeout=15)


class SwarmRunner:
    """the Runner of production, over docker-py

    The client is made on first use: docker.from_env() asks the daemon its API
    version, and the API must start even while the socket-proxy does not
    answer yet. A transport failure drops the client; the next call makes a
    new one.
    """

    def __init__(
        self,
        settings: Settings,
        client_factory: Callable[[], docker.DockerClient] = _docker_from_env,
    ):
        self._settings = settings
        self._client_factory = client_factory
        self._client: Optional[docker.DockerClient] = None

    @contextlib.contextmanager
    def _docker(self) -> Iterator[docker.DockerClient]:
        """the client; transport errors become RunnerUnavailable"""
        try:
            if self._client is None:
                self._client = self._client_factory()
            yield self._client
        except docker.errors.APIError:
            raise  # the daemon answered: the caller decides what it means
        except (docker.errors.DockerException, requests.exceptions.RequestException) as exc:
            self._client = None
            raise RunnerUnavailable(f"docker unreachable: {exc}") from exc

    # --- Runner ---------------------------------------------------------------

    def launch(self, spec: LaunchSpec) -> str:
        with self._docker() as client:
            try:
                return client.services.create(**self.service_kwargs(spec, client)).id
            except docker.errors.APIError as exc:
                if exc.status_code == 409:
                    return self._adopt(client, spec)
                if exc.status_code in _UNAVAILABLE_STATUSES:
                    raise RunnerUnavailable(f"docker unavailable: {exc}") from exc
                raise LaunchError(exc.explanation or str(exc)) from exc
            except (docker.errors.InvalidRepository, docker.errors.InvalidArgument) as exc:
                # refused by docker-py itself, before any request: the spec's fault
                raise LaunchError(str(exc)) from exc

    def status(self, service_id: str) -> TaskStatus:
        with self._docker() as client:
            try:
                tasks = client.api.tasks(filters={"service": service_id})
                if not tasks:
                    client.api.inspect_service(service_id)
                    return TaskStatus(PENDING)
            except docker.errors.NotFound:
                return TaskStatus(GONE, error="service no longer exists")
            except docker.errors.APIError as exc:
                raise RunnerUnavailable(f"docker unavailable: {exc}") from exc

        latest = max(tasks, key=lambda item: item.get("Version", {}).get("Index", 0))
        return self._task_status(latest)

    def remove(self, service_id: str) -> None:
        with self._docker() as client:
            try:
                client.api.remove_service(service_id)
            except docker.errors.NotFound:
                pass
            except docker.errors.APIError as exc:
                raise RunnerUnavailable(f"docker unavailable: {exc}") from exc

    def managed_services(self) -> Dict[str, str]:
        filters = {
            "label": [f"{MANAGED_LABEL}=true", f"{STACK_LABEL}={self._settings.run_stack}"]
        }
        with self._docker() as client:
            try:
                services = client.api.services(filters=filters)
            except docker.errors.APIError as exc:
                raise RunnerUnavailable(f"docker unavailable: {exc}") from exc

        return {
            service["ID"]: service.get("Spec", {}).get("Labels", {}).get(ATTEMPT_LABEL, "")
            for service in services
        }

    def missing_objects(
        self, secrets: Sequence[str], configs: Sequence[str], networks: Sequence[str]
    ) -> List[str]:
        with self._docker() as client:
            try:
                existing = {
                    "secret": {item.name for item in client.secrets.list()},
                    "config": {item.name for item in client.configs.list()},
                    "network": {item.name for item in client.networks.list()},
                }
            except docker.errors.APIError as exc:
                raise RunnerUnavailable(f"docker unavailable: {exc}") from exc

        wanted = (("secret", secrets), ("config", configs), ("network", networks))
        return [
            f"{kind} {name!r} does not exist"
            for kind, names in wanted
            for name in names
            if name not in existing[kind]
        ]

    # --- the service ----------------------------------------------------------

    def log_options(self) -> Dict[str, str]:
        """the house loki block (csc-ocs-ansible docs/swarm/logging.md)"""
        return {
            "loki-url": self._settings.loki_url,
            "loki-retries": str(self._settings.loki_retries),
            "loki-batch-size": str(self._settings.loki_batch_size),
            "mode": self._settings.loki_mode,
        }

    def service_kwargs(self, spec: LaunchSpec, client: docker.DockerClient) -> dict:
        """the services.create() arguments of an attempt

        docker-py's command is the image ENTRYPOINT and its args the CMD: a
        job's entrypoint maps to the first, its command to the second.
        """
        labels = {
            **spec.labels,
            MANAGED_LABEL: "true",
            STACK_LABEL: self._settings.run_stack,
        }
        kwargs = {
            "image": spec.image,
            "command": spec.entrypoint,
            "args": list(spec.command) or None,
            "name": spec.service_name,
            "env": [f"{name}={value}" for name, value in spec.env.items()],
            "labels": labels,
            "container_labels": labels,
            # one task, never restarted by Swarm. replicas=1 is required:
            # without it docker-py sends an empty ReplicatedJob.
            "mode": ServiceMode("replicated-job", replicas=1, concurrency=1),
            "restart_policy": RestartPolicy(condition="none"),
            # a job is judged by its exit code: the image's HEALTHCHECK is for
            # a long-running service, and a one-shot command that does not
            # serve it would be killed by Swarm as unhealthy
            "healthcheck": Healthcheck(test=["NONE"]),
            "networks": [self._network(client, reference) for reference in spec.networks],
            "constraints": list(spec.constraints),
            "secrets": [self._secret(client, secret) for secret in spec.secrets],
            "configs": [self._config(client, config) for config in spec.configs],
            "mounts": [
                Mount(
                    target=mount["target"],
                    source=mount.get("source") or None,
                    type=mount.get("type", "volume"),
                    read_only=mount.get("read_only", False),
                )
                for mount in spec.mounts
            ],
            "log_driver": LOG_DRIVER,
            "log_driver_options": self.log_options(),
        }
        if spec.cpu_limit or spec.memory_limit_mb:
            kwargs["resources"] = Resources(
                cpu_limit=int(spec.cpu_limit * 1e9) if spec.cpu_limit else None,
                mem_limit=spec.memory_limit_mb * 1024 * 1024 if spec.memory_limit_mb else None,
            )
        return kwargs

    def _secret(self, client: docker.DockerClient, secret: dict) -> SecretReference:
        name = secret["source"]
        return SecretReference(
            self._object_id(client.secrets, "secret", name),
            name,
            filename=secret.get("target") or name,
            uid=secret.get("uid", "10001"),
            gid=secret.get("gid", "10001"),
            mode=secret.get("mode", 0o400),
        )

    def _config(self, client: docker.DockerClient, config: dict) -> ConfigReference:
        name = config["source"]
        return ConfigReference(
            self._object_id(client.configs, "config", name),
            name,
            filename=config["target"],
            uid=config.get("uid", "10001"),
            gid=config.get("gid", "10001"),
            mode=config.get("mode", 0o444),
        )

    def _network(self, client: docker.DockerClient, reference: str) -> str:
        """the reference, once it is known not to be one of the backend's own
        networks -- the daemon resolves ids and id prefixes too, so the name
        alone is not enough to tell"""
        try:
            network = client.networks.get(reference)
        except docker.errors.NotFound as exc:
            raise LaunchError(f"network {reference!r} does not exist") from exc
        if network.name in self._settings.protected_networks:
            raise LaunchError(f"network {reference!r} is the backend's own ({network.name})")
        return reference

    @staticmethod
    def _object_id(collection, kind: str, name: str) -> str:
        """the id of the secret or config named exactly name -- the daemon's
        name filter matches prefixes"""
        found = [item for item in collection.list(filters={"name": name}) if item.name == name]
        if len(found) != 1:
            raise LaunchError(f"swarm {kind} {name!r} does not exist")
        return found[0].id

    @staticmethod
    def _adopt(client: docker.DockerClient, spec: LaunchSpec) -> str:
        """the service a dead tick created for this very attempt -- if it is"""
        existing = client.services.get(spec.service_name)
        labels = existing.attrs.get("Spec", {}).get("Labels", {})

        if labels.get(ATTEMPT_LABEL) != spec.labels.get(ATTEMPT_LABEL):
            raise LaunchError(
                f"a service named {spec.service_name} exists and belongs to another attempt"
            )

        LOGGER.warning("Adopting the existing service %s", spec.service_name)
        return existing.id

    @staticmethod
    def _task_status(task: dict) -> TaskStatus:
        status = task.get("Status", {})
        state = status.get("State", "")
        exit_code = status.get("ContainerStatus", {}).get("ExitCode")
        node_id = task.get("NodeID") or None

        if state == "complete":
            return TaskStatus(COMPLETE, exit_code=0 if exit_code is None else exit_code, node_id=node_id)
        if state in _FAILED_TASK_STATES:
            error = status.get("Err") or f"task {state}: {status.get('Message', '')}"
            return TaskStatus(FAILED, exit_code=exit_code, error=error, node_id=node_id)
        if state == "running":
            return TaskStatus(RUNNING, node_id=node_id)
        return TaskStatus(PENDING, error=status.get("Err") or None, node_id=node_id)
