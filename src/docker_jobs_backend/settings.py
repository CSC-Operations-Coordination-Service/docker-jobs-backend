"""Configuration, from the environment

One variable per field, upper-cased: DATABASE_URL, MAX_RUNNING, LOKI_URL...
API_TOKEN can also be a file, swarm style: API_TOKEN_FILE wins over it.
Two families are read by their libraries instead, under their standard names:
docker-py reads DOCKER_HOST, the OTLP exporter reads OTEL_EXPORTER_OTLP_*.
"""

import functools
import os
from typing import Annotated, List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

__all__ = ["Settings", "get_settings"]


def _read_secret_file(name: str) -> Optional[str]:
    """the content of $<name>_FILE, when that variable is set"""
    path = os.environ.get(f"{name}_FILE")

    if not path:
        return None

    with open(path, encoding="UTF-8") as secret:
        return secret.read().strip()


class Settings(BaseSettings):
    """Every knob of the backend"""

    model_config = SettingsConfigDict(extra="ignore")

    # --- Storage --------------------------------------------------------------
    # SQLite on the service's local volume (WAL does not work over NFS).
    database_url: str = "sqlite:////data/docker-jobs.db"
    # Held while the scheduler runs: a second process on the same volume (a
    # start-first update, uvicorn --workers) refuses to start instead of
    # running every job twice.
    lock_file: str = "/data/scheduler.lock"

    # --- Scheduler ------------------------------------------------------------
    # False serves the API alone, e.g. over a copy of the database.
    scheduler_enabled: bool = True
    tick_seconds: float = Field(default=10.0, gt=0)
    # Attempts running at once across all jobs, 0 for no limit: the 01:00
    # burst of recollects is what this is for.
    max_running: int = Field(default=8, ge=0)
    # Runs of one job that may wait to start at once: catching up a year of
    # hourly intervals queues them a batch at a time, as earlier ones run,
    # never as one huge queue every tick must read.
    catchup_batch: int = Field(default=100, ge=1)
    # An attempt whose task never starts (no node matches its constraints,
    # the image cannot be pulled) times out after this, not after the job's
    # whole timeout.
    start_timeout_seconds: int = Field(default=600, ge=60)
    # Job services are <run_stack>_<job>_<run>_<try>, labelled
    # com.docker.stack.namespace=<run_stack>. Two backends on one swarm need
    # two values: each removes the services of its stack it does not know.
    run_stack: str = Field(default="djobs", pattern=r"^[a-z][a-z0-9-]{0,9}$")
    # A bind mount is root on the node: refused unless explicitly allowed.
    allow_bind_mounts: bool = False
    # The backend's own swarm objects, which no job may use: the network the
    # socket-proxy listens on (from it, a job could create any service) and
    # the volume of the database. The names deploy/docker-compose.yml gives
    # them. Comma-separated.
    protected_networks: Annotated[List[str], NoDecode] = Field(
        default_factory=lambda: ["docker-jobs-socket"]
    )
    protected_volumes: Annotated[List[str], NoDecode] = Field(
        default_factory=lambda: ["docker-jobs-data"]
    )
    # What a job may name at all, beyond the protected objects above: every
    # other swarm network (host included), named volume, secret and config is
    # refused. Empty refuses every one, so a deployment lists what its jobs
    # need. An entry matches that name and its stack versions, <name>_v<n>;
    # one ending in * matches by prefix, and * alone matches everything.
    # Comma-separated.
    allowed_networks: Annotated[List[str], NoDecode] = Field(default_factory=list)
    allowed_volumes: Annotated[List[str], NoDecode] = Field(default_factory=list)
    allowed_secrets: Annotated[List[str], NoDecode] = Field(default_factory=list)
    allowed_configs: Annotated[List[str], NoDecode] = Field(default_factory=list)

    # --- Logs: the loki driver of every job container ----------------------
    loki_url: str = "http://127.0.0.1:3100/loki/api/v1/push"
    loki_retries: int = 5
    loki_batch_size: int = 400
    loki_mode: str = "non-blocking"

    # --- Metrics --------------------------------------------------------------
    otel_service_name: str = "docker-jobs-backend"
    # An interval counts as a gap once it ended this long ago with no data...
    gap_grace_seconds: int = Field(default=300, ge=0)
    # ...and stops counting once it ended this long ago.
    gap_lookback_hours: int = Field(default=168, ge=1)

    # --- HTTP -----------------------------------------------------------------
    # Path prefix a reverse proxy serves the API under, if any.
    root_path: str = ""
    # Header carrying the caller's login: Grafana's plugin proxy sets it.
    user_header: str = "X-Grafana-User"
    # Shared secret of the Grafana app's proxy route; empty disables the check.
    api_token: str = ""

    @field_validator(
        "protected_networks",
        "protected_volumes",
        "allowed_networks",
        "allowed_volumes",
        "allowed_secrets",
        "allowed_configs",
        mode="before",
    )
    @classmethod
    def _split(cls, value):
        """accept a comma-separated string, which is what an env var holds"""
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    def model_post_init(self, __context) -> None:
        """swarm secrets: API_TOKEN_FILE wins over API_TOKEN"""
        secret = _read_secret_file("API_TOKEN")
        if secret is not None:
            self.api_token = secret


@functools.lru_cache
def get_settings() -> Settings:
    """the process-wide settings, read once"""
    return Settings()
