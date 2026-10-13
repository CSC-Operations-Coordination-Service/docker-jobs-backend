# docker-jobs-backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI service, shipped as a container, that does the following:
- runs cron-scheduled jobs as Docker Swarm `replicated-job` services, with Airflow-style data intervals, templated commands, retries, manual runs, reruns and cancels;
- sends every job's logs through the `loki` driver;
- pushes run and attempt metrics over OTLP, so that missed or failed runs fire Grafana alerts.

**Architecture:** One process (one swarm replica, pinned to a manager node) does two jobs:
- It serves a REST API under `/api/v1` for the Grafana app, which gets its own plan next, in `../docker-jobs-app`.
- It runs a scheduler thread that ticks every 10 s. Each tick creates runs for due cron intervals, reconciles running attempts against their Swarm tasks, and launches pending attempts within the limits.

State lives in SQLite on the service's volume. The service reaches Docker through `tecnativa/docker-socket-proxy` and sends metrics through the OTel SDK to Alloy.

**Execution (chosen 2026-10-09):** Native. I implement every task in this session with superpowers:executing-plans, then one fresh reviewer checks the whole branch.

**Tech Stack:**
- Python 3.11: FastAPI, pydantic-settings, SQLAlchemy 2.0 on SQLite, croniter, Jinja2 sandbox, docker-py 7, opentelemetry-sdk with the OTLP/HTTP exporter, pytest run through tox.
- Infrastructure: Docker Swarm, the Loki docker driver, Grafana Alloy → Mimir, Grafana alerting.

**Spec:** There is no spec file. The requirements are the request quoted under *Context*, plus the decisions recorded there. House conventions come from these sources:
- `../download-manager/download-manager-backend/` for code, tests, the Dockerfile and CI. It is the template: read its `README.md`, `settings.py`, `auth.py` and `tests/conftest.py` before starting.
- `../download-manager/deploy/` for the stack layout.
- `../csc-ocs-ansible/docs/swarm/` for logging and stacks.
- `../csc-ocs-ansible/docs/ROADMAP.md` § "Ofelia", which lists the cron jobs this backend replaces.

## Context

Today, scheduled container work runs from host crontabs on two of the swarm nodes, using `docker run --rm` plus a shell script that does the date arithmetic. Those crontabs are not versioned, not visible to anyone, and not monitored. The roadmap planned Ofelia for this, but Ofelia leaves open several needs: no overlap between runs, logs in Loki, failures that are visible, and the 01:00 burst of 18 recollects. This backend replaces Ofelia and covers those needs.

The request (2026-10-09), condensed:
- Orchestrate containers on a schedule, like a k8s CronJob or Job but for Docker Swarm, and like Apache Airflow.
- Every job uses the `loki` logging driver.
- Commands are templated, e.g. every hour `python -m module-collect --start 2026-10-09T00:00:00.000Z`. The app generates those values.
- OTLP metrics count runs, successes, failures and retries, enough to detect a missing run or a failed job.
- Any run, failed or successful, can be rerun by hand.
- The system has two parts. This backend talks to Docker, ships as a container and lives in this folder's repository. The frontend is a Grafana app in `../docker-jobs-app`, planned separately against this API.

Decisions made while planning:
- Jobs are defined through the API and stored in the DB.
- Catch-up is a per-job flag and is on by default.
- Each attempt runs as one Swarm `replicated-job` service.
- Storage is SQLite.

Airflow semantics apply: the run for the interval `[start, end)` fires at `end`, and its `logical_date` is `start`. An hourly job firing at 01:00 therefore renders `{{ data_interval_start | iso_ms }}` as `2026-10-09T00:00:00.000Z`.

## Global Constraints

- **Python and packaging**
  - Python `>=3.11`, image base `python:3.11-slim`, running as non-root `appuser` with uid/gid `10001`.
  - PEP 621 `pyproject.toml` with setuptools and setuptools_scm (`version_scheme = "no-guess-dev"`, `fallback_version = "0.1.0"`).
  - src layout `src/docker_jobs_backend/`, with a `testing` extra.
- **Tests**
  - The default `tox` env runs `pytest --junitxml=pytest-report.xml --cov-report=xml:coverage.xml` with `TZ = utc`, and must write both files.
  - Tests are plain pytest functions. The network boundaries (Docker, OTLP) are faked.
- **Configuration**
  - Environment variables only, read through pydantic-settings.
  - docker-py reads `DOCKER_HOST` itself, and the OTLP exporter reads `OTEL_EXPORTER_OTLP_*` itself.
  - `API_TOKEN_FILE` takes precedence over `API_TOKEN` (swarm secret).
- **Time**
  - Every datetime is timezone-aware UTC. The API answers 422 to a naive datetime.
  - Cron expressions have 5 fields and are evaluated in UTC only.
- **One process:** exactly one backend process per database. That means one replica, `update_config.order: stop-first`, uvicorn without `--workers`, and a `flock` on `LOCK_FILE`.
- **Every job service**
  - Uses `log_driver` `loki` with exactly these options: `loki-url: http://127.0.0.1:3100/loki/api/v1/push`, `loki-retries: "5"`, `loki-batch-size: "400"`, `mode: non-blocking`. It sets no `loki-external-labels`, and a job cannot override any of this.
  - Has mode `{"ReplicatedJob": {"MaxConcurrent": 1, "TotalCompletions": 1}}` and restart condition `none`. Retries belong to the backend, with one service per attempt.
  - Is named `<RUN_STACK>_<job>_<run id>_<try>`.
  - Is labelled `com.docker.stack.namespace=<RUN_STACK>`, `docker-jobs.managed=true` and `docker-jobs.attempt=<attempt token>`, on both the service and the container.
- **Mounts:** secrets default to uid/gid `"10001"` and mode `0o400`; configs to mode `0o444`. Bind mounts are refused unless `ALLOW_BIND_MOUNTS=true`.
- **Metrics:** the job's metric attribute is `docker_job`, never `job`, because Mimir maps `service.name` to `job`.
- **API and auth**
  - The API lives under `/api/v1`.
  - The caller is read from `X-Grafana-User` (setting `USER_HEADER`) and is `anonymous` when the header is missing.
  - When `API_TOKEN` is set, every `/api/v1` route requires `Authorization: Bearer <token>`. `/health` stays open.
- **Commits:** every commit message ends with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.

## Review Focus

These are the inputs and failure modes that would most likely hurt someone running this, most likely first. Each has a test in the task that owns the code.

1. **Docker unreachable mid-tick** (socket-proxy restarting, manager rebooting). Pending attempts stay pending, no retry is used up, the thread keeps ticking, `docker_jobs_docker_up` drops to 0, and the heartbeat stops advancing. *Task 8:* `test_docker_down_burns_no_retry_and_stops_the_heartbeat`.
2. **The process dies between `services.create` and recording it.** The same attempt adopts its own service on the next tick, matched by token. A same-named service that belongs to someone else is never adopted. *Task 5:* `test_a_name_conflict_adopts_our_own_service` and `test_a_name_conflict_with_a_stranger_is_a_launch_error`. *Task 8:* `test_the_sweep_keeps_the_service_of_a_pending_attempt`.
3. **Catch-up after a long outage, or a far-past `start_date` on a frequent schedule.** At most `CATCHUP_BATCH` runs are created per job per tick, the cursor resumes where it stopped, and launches go oldest first within `max_active_runs`. *Task 8:* `test_catch_up_is_batched_and_resumes` and `test_only_the_oldest_run_starts_when_one_may_run_at_a_time`.
4. **Naive datetimes and SQLite's lost timezone.** The API rejects naive datetimes with 422, and values read back from SQLite are aware UTC. *Task 4:* `test_datetimes_come_back_aware_utc` and `test_a_naive_datetime_is_refused`. *Task 10:* `test_a_naive_start_date_is_a_422`.
5. **A job's very first failure must show up to PromQL `increase()`.** Outcome counters start at 0 for every job, both at startup and at creation. *Task 6:* `test_counters_of_a_job_start_at_zero`. *Task 9:* `test_startup_registers_existing_jobs`. *Task 10:* `test_a_new_job_counts_from_zero`.

## File Structure

```
docker-jobs-backend/
├── pyproject.toml, tox.ini, .gitignore, .dockerignore, LICENSE.txt,
│   .pre-commit-config.yaml, sonar-project.properties, README.md
├── Dockerfile                      wheel from dist/ → uvicorn --factory, uid 10001, /data volume
├── .github/workflows/docker-jobs-backend.yml   python-ci → sbom → docker-build
├── docs/superpowers/plans/2026-10-09-docker-jobs-backend.md   this plan
├── src/docker_jobs_backend/
│   ├── settings.py     Settings (env), get_settings
│   ├── deps.py         FastAPI dependencies reading app.state
│   ├── auth.py         User, get_current_user (X-Grafana-User), require_token (API_TOKEN)
│   ├── db.py           SQLite engine (WAL, BEGIN IMMEDIATE), UTCDateTime, Job/Run/Attempt, enums
│   ├── schedule.py     cron → Airflow intervals: due_intervals, interval_containing, …
│   ├── templating.py   Jinja sandbox: build_context, render, validate_templates, iso_ms
│   ├── runner.py       LaunchSpec/TaskStatus, Runner protocol, SwarmRunner (docker-py)
│   ├── telemetry.py    Metrics (counters + snapshot gauges), count_gaps, create_meter_provider
│   ├── lifecycle.py    Lifecycle: create_run, rerun, cancel, launch, reconcile, sweep
│   ├── scheduler.py    Scheduler.tick (schedule → reconcile → launch), SchedulerThread, exclusive_lock
│   ├── schemas.py      API bodies + job_out/run_out
│   ├── jobs.py         /jobs, /jobs/{name}/trigger, /preview
│   ├── runs.py         /runs, /runs/{id}, /runs/{id}/rerun, /runs/{id}/cancel
│   └── main.py         create_app(): wiring, lifespan (lock, metrics registration, thread)
├── tests/
│   ├── conftest.py     fixtures: settings, session_factory, session, reader, metrics, runner,
│   │                   lifecycle, scheduler, make_app, client
│   ├── factories.py    T0/HOUR…, make_job, make_run, FakeRunner, points, attrs
│   └── test_*.py       one per module, plus test_jobs_api, test_runs_api, test_alert_rules
└── deploy/
    ├── docker-compose.yml, stack.yml, .env.example, deploy.sh, README.md
    ├── secrets/.gitkeep                 api-token goes here (gitignored)
    └── grafana/docker-jobs-alerts.yaml   Grafana alert provisioning
```

Dependency order: settings → (schedule, templating, db) → runner → telemetry → lifecycle → scheduler → main/deps → schemas → jobs, runs.

---

### Task 1: Repository scaffold, settings, caller identity, health

**Files:**
- Create: `.gitignore`, `pyproject.toml`, `tox.ini`, `README.md`, `LICENSE.txt` (copied), `.pre-commit-config.yaml` (copied), `docs/superpowers/plans/2026-10-09-docker-jobs-backend.md` (copied)
- Create: `src/docker_jobs_backend/__init__.py`, `settings.py`, `deps.py`, `auth.py`, `main.py`
- Test: `tests/conftest.py`, `tests/test_settings.py`, `tests/test_app.py`

**Interfaces:**
- Produces:
  - `Settings` with these fields: `database_url`, `lock_file`, `scheduler_enabled`, `tick_seconds`, `max_running`, `catchup_batch`, `start_timeout_seconds`, `run_stack`, `allow_bind_mounts`, `loki_url`, `loki_retries`, `loki_batch_size`, `loki_mode`, `otel_service_name`, `gap_grace_seconds`, `gap_lookback_hours`, `root_path`, `user_header`, `api_token`. Also `get_settings() -> Settings`.
  - `deps.get_app_settings(request) -> Settings`, which reads `request.app.state.settings`.
  - `auth.User(login: str)`, `auth.get_current_user(request, settings) -> User` and `auth.require_token(request, settings) -> None`.
  - `main.API_PREFIX = "/api/v1"` and `main.create_app(settings: Optional[Settings] = None) -> FastAPI`. Task 9 extends this signature.
  - Fixtures: `settings`, `make_app` (a zero-argument callable returning the app) and `client`.

- [ ] **Step 1: Create the repository and the packaging files**

```bash
cd /home/aagazzi/work/docker-jobs-backend
git init -b develop
cp ../download-manager/LICENSE.txt LICENSE.txt
cp ../download-manager/.pre-commit-config.yaml .pre-commit-config.yaml
mkdir -p src/docker_jobs_backend tests docs/superpowers/plans
cp /home/aagazzi/.claude/plans/curious-exploring-magpie.md docs/superpowers/plans/2026-10-09-docker-jobs-backend.md
```

`.gitignore`:

```gitignore
.venv/
__pycache__/
*.egg-info/
build/
dist/
.tox/
.pytest_cache/
.coverage
coverage.xml
pytest-report.xml
sbom-python.cdx.json
*.db
*.db-wal
*.db-shm
*.lock
deploy/.env
deploy/secrets/api-token
```

`pyproject.toml`:

```toml
[build-system]
requires = ["setuptools>=61", "setuptools_scm[toml]>=8", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "docker-jobs-backend"
description = "Scheduled jobs on Docker Swarm: cron intervals, templated commands, retries, reruns, Loki logs, OTLP metrics"
readme = "README.md"
license = { text = "Apache-2.0" }
authors = [{ name = "Serco - Space unit", email = "ocsproc@serco.com" }]
requires-python = ">=3.11"
dynamic = ["version"]
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.32",
    "pydantic>=2.9",
    "pydantic-settings>=2.7",
    "sqlalchemy>=2.0.30",
    "croniter>=6.0",
    "jinja2>=3.1",
    "docker>=7.1",
    "opentelemetry-sdk>=1.30",
    "opentelemetry-exporter-otlp-proto-http>=1.30",
]

[project.optional-dependencies]
testing = [
    "pytest",
    "pytest-cov",
    "httpx",
    "pyyaml",
]

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools_scm]
version_scheme = "no-guess-dev"
fallback_version = "0.1.0"

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--cov docker_jobs_backend --cov-report term-missing --verbose"
```

`tox.ini`:

```ini
# The interface the reusable python-ci / sbom workflows drive: `tox` (tests +
# reports), `tox -e build`, `tox -e sbom`. Same as download-manager-backend's.

[tox]
minversion = 3.24
envlist = default
isolated_build = True


[testenv:.pkg]
# setuptools_scm runs in the isolated build env: forward its variables, and
# HOME/PIP_* so the build requirements resolve through the Nexus index.
passenv =
    HOME
    PIP_*
    SETUPTOOLS_*


[testenv]
description = Invoke pytest to run automated tests
setenv =
    TOXINIDIR = {toxinidir}
    # tests compare timestamps with fixed values
    TZ = utc
passenv =
    HOME
    PIP_*
    SETUPTOOLS_*
extras =
    testing
commands =
    pytest \
        --junitxml=pytest-report.xml \
        --cov-report=xml:coverage.xml {posargs}


[testenv:{build,clean}]
description =
    build: Build the package in isolation according to PEP517
    clean: Remove old distribution files and temporary build artifacts (./build and ./dist)
skip_install = True
changedir = {toxinidir}
deps =
    build: build[virtualenv]
passenv =
    SETUPTOOLS_*
commands =
    clean: python -c 'import shutil; [shutil.rmtree(p, True) for p in ("build", "dist")]'
    clean: python -c 'import pathlib, shutil; [shutil.rmtree(p, True) for p in pathlib.Path("src").glob("*.egg-info")]'
    build: python -m build {posargs}


[testenv:sbom]
description = CycloneDX SBOM of an installed environment: `tox -e sbom -- .sbom-venv`
skip_install = True
changedir = {toxinidir}
deps = cyclonedx-bom
commands =
    cyclonedx-py environment {posargs} --output-format JSON --output-file sbom-python.cdx.json
```

`README.md` (Task 13 completes it):

```markdown
# docker-jobs-backend

Scheduled jobs on Docker Swarm: cron intervals, templated commands, retries, reruns,
Loki logs, OTLP metrics. The UI is the Grafana app in `../docker-jobs-app`.
```

`src/docker_jobs_backend/__init__.py`:

```python
"""docker-jobs-backend: scheduled jobs on Docker Swarm"""
```

- [ ] **Step 2: Create the virtualenv**

```bash
python3.11 -m venv .venv
.venv/bin/pip install -e '.[testing]'
```

Expected: the install ends with `Successfully installed ... docker-jobs-backend-0.1.0 ...`. Packages resolve through the Nexus index in `~/.config/pip/pip.conf`, the same way download-manager's do.

- [ ] **Step 3: Write the failing tests**

`tests/conftest.py`:

```python
"""Fixtures: settings on a temporary database, and the application around them"""

import pytest
from fastapi.testclient import TestClient

from docker_jobs_backend.main import create_app
from docker_jobs_backend.settings import Settings


@pytest.fixture(name="settings")
def settings_fixture(tmp_path):
    """a temporary SQLite file and lock, the scheduler thread off"""
    return Settings(
        database_url=f"sqlite:///{tmp_path / 'jobs.db'}",
        lock_file=str(tmp_path / "scheduler.lock"),
        scheduler_enabled=False,
    )


@pytest.fixture(name="make_app")
def make_app_fixture(settings):
    """builds the application when called, so a test can change settings first"""
    return lambda: create_app(settings)


@pytest.fixture(name="client")
def client_fixture(make_app):
    with TestClient(make_app()) as client:
        yield client
```

`tests/test_settings.py`:

```python
"""Settings come from the environment"""

import pytest
from pydantic import ValidationError

from docker_jobs_backend.settings import Settings


def test_the_loki_defaults_are_the_house_block():
    settings = Settings()

    assert settings.loki_url == "http://127.0.0.1:3100/loki/api/v1/push"
    assert (settings.loki_retries, settings.loki_batch_size, settings.loki_mode) == (
        5,
        400,
        "non-blocking",
    )


def test_each_field_reads_its_upper_case_variable(monkeypatch):
    monkeypatch.setenv("MAX_RUNNING", "3")
    monkeypatch.setenv("RUN_STACK", "djobs-dev")
    monkeypatch.setenv("ALLOW_BIND_MOUNTS", "true")

    settings = Settings()

    assert (settings.max_running, settings.run_stack, settings.allow_bind_mounts) == (
        3,
        "djobs-dev",
        True,
    )


@pytest.mark.parametrize("stack", ["Djobs", "djobs_x", "-djobs", "a-stack-name-too-long"])
def test_a_run_stack_that_cannot_prefix_a_service_name_is_refused(stack):
    with pytest.raises(ValidationError):
        Settings(run_stack=stack)


def test_the_api_token_can_be_a_swarm_secret(tmp_path, monkeypatch):
    secret = tmp_path / "token"
    secret.write_text("from-file\n")
    monkeypatch.setenv("API_TOKEN", "from-env")
    monkeypatch.setenv("API_TOKEN_FILE", str(secret))

    assert Settings().api_token == "from-file"
```

`tests/test_app.py`:

```python
"""The application shell: health, the caller, the API token"""

from fastapi.testclient import TestClient


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
```

- [ ] **Step 4: Run them to see them fail**

Run: `.venv/bin/pytest -q`
Expected: collection errors such as `ModuleNotFoundError: No module named 'docker_jobs_backend.main'`.

- [ ] **Step 5: Write the implementation**

`src/docker_jobs_backend/settings.py`:

```python
"""Configuration, from the environment

One variable per field, upper-cased: DATABASE_URL, MAX_RUNNING, LOKI_URL...
API_TOKEN can also be a file, swarm style: API_TOKEN_FILE wins over it.
Two families are read by their libraries instead, under their standard names:
docker-py reads DOCKER_HOST, the OTLP exporter reads OTEL_EXPORTER_OTLP_*.
"""

import functools
import os
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    # Runs one job may create per tick while catching up: a year of hourly
    # intervals becomes queued work over minutes, not one transaction.
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

    def model_post_init(self, __context) -> None:
        """swarm secrets: API_TOKEN_FILE wins over API_TOKEN"""
        secret = _read_secret_file("API_TOKEN")
        if secret is not None:
            self.api_token = secret


@functools.lru_cache
def get_settings() -> Settings:
    """the process-wide settings, read once"""
    return Settings()
```

`src/docker_jobs_backend/deps.py` (Task 9 adds the rest):

```python
"""FastAPI dependencies: what create_app() keeps on app.state"""

from fastapi import Request

from docker_jobs_backend.settings import Settings

__all__ = ["get_app_settings"]


def get_app_settings(request: Request) -> Settings:
    """the settings the application was built with"""
    return request.app.state.settings
```

`src/docker_jobs_backend/auth.py`:

```python
"""Who is calling

The backend trusts a header naming the user, which Grafana's plugin proxy sets
(X-Grafana-User) when the Grafana app calls it, overwriting any the browser
sent. With API_TOKEN set, a call must also carry the token the proxy route adds
from the app's secureJsonData: anything else on the overlay network that
reaches the backend gets a 401, so it cannot forge the header either.

Keycloak replaces these two functions and nothing else: every route that
needs a user depends on get_current_user, every /api/v1 route on
require_token.
"""

import hmac

from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

from docker_jobs_backend.deps import get_app_settings
from docker_jobs_backend.settings import Settings

__all__ = ["ANONYMOUS", "User", "get_current_user", "require_token"]

ANONYMOUS = "anonymous"


class User(BaseModel):
    """the caller"""

    login: str


def get_current_user(
    request: Request, settings: Settings = Depends(get_app_settings)
) -> User:
    """the caller, from the configured header"""
    login = request.headers.get(settings.user_header, "").strip()

    return User(login=login or ANONYMOUS)


def require_token(
    request: Request, settings: Settings = Depends(get_app_settings)
) -> None:
    """401 unless the request carries the API token, when one is set"""
    if not settings.api_token:
        return

    supplied = request.headers.get("Authorization", "").encode()
    expected = f"Bearer {settings.api_token}".encode()

    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(
            status_code=401,
            detail="missing or wrong API token",
            headers={"WWW-Authenticate": "Bearer"},
        )
```

`src/docker_jobs_backend/main.py` (Task 9 replaces it):

```python
"""The FastAPI application

    /health            liveness
    /api/v1/whoami     the caller, as the backend sees them
    /docs              OpenAPI UI
"""

import logging
from typing import Optional

from fastapi import Depends, FastAPI

from docker_jobs_backend.auth import User, get_current_user, require_token
from docker_jobs_backend.settings import Settings, get_settings

__all__ = ["API_PREFIX", "create_app"]

API_PREFIX = "/api/v1"


def create_app(settings: Optional[Settings] = None) -> FastAPI:
    """build the application"""
    settings = settings or get_settings()

    application = FastAPI(
        title="docker-jobs-backend",
        description="Scheduled jobs on Docker Swarm",
        root_path=settings.root_path,
    )
    application.state.settings = settings

    @application.get("/health", tags=["health"])
    def health() -> dict:
        return {"status": "ok"}

    @application.get(
        f"{API_PREFIX}/whoami", tags=["auth"], dependencies=[Depends(require_token)]
    )
    def whoami(user: User = Depends(get_current_user)) -> User:
        return user

    return application


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
```

- [ ] **Step 6: Run the tests to see them pass**

Run: `.venv/bin/pytest -q`
Expected: `11 passed`.

- [ ] **Step 7: Commit**

```bash
git add .gitignore pyproject.toml tox.ini README.md LICENSE.txt .pre-commit-config.yaml docs src tests
git commit -m "feat: scaffold docker-jobs-backend with settings, caller identity and health" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Cron intervals (`schedule.py`)

**Files:**
- Create: `src/docker_jobs_backend/schedule.py`
- Test: `tests/test_schedule.py`

**Interfaces:**
- Produces:
  - `Interval(start: datetime, end: datetime)` (frozen dataclass)
  - `validate_cron(expression: str) -> str`, which returns the normalised expression or raises `ValueError`
  - `interval_containing(expression, moment) -> Interval`
  - `latest_complete_interval(expression, now) -> Interval`
  - `due_intervals(expression, start_date, end_date, cursor, now, limit) -> List[Interval]`
  - `next_fire_times(expression, after, count) -> List[datetime]`
- All of these raise `ValueError` on a naive datetime, and return aware UTC datetimes.

- [ ] **Step 1: Write the failing tests**

`tests/test_schedule.py`:

```python
"""Cron expressions become Airflow-style data intervals"""

import datetime

import pytest

from docker_jobs_backend.schedule import (
    Interval,
    due_intervals,
    interval_containing,
    latest_complete_interval,
    next_fire_times,
    validate_cron,
)

UTC = datetime.UTC
HOURLY = "0 * * * *"


def at(hour, minute=0, day=9):
    return datetime.datetime(2026, 10, day, hour, minute, tzinfo=UTC)


def test_a_valid_expression_comes_back_normalised():
    assert validate_cron("  0   *  * * * ") == "0 * * * *"


@pytest.mark.parametrize(
    "expression", ["", "* * * *", "0 0 * * * *", "61 * * * *", "@hourly", "every hour"]
)
def test_anything_but_five_valid_fields_is_refused(expression):
    with pytest.raises(ValueError):
        validate_cron(expression)


def test_an_interval_ends_at_the_next_fire_time():
    assert interval_containing(HOURLY, at(10, 20)) == Interval(at(10), at(11))


def test_a_fire_time_starts_its_own_interval():
    assert interval_containing(HOURLY, at(10)) == Interval(at(10), at(11))


def test_irregular_schedules_have_irregular_intervals():
    twice_a_day = "0 1,13 * * *"

    assert interval_containing(twice_a_day, at(0, day=10)) == Interval(
        at(13, day=9), at(1, day=10)
    )
    assert interval_containing(twice_a_day, at(5, day=10)) == Interval(
        at(1, day=10), at(13, day=10)
    )


def test_the_latest_complete_interval_is_the_one_that_ended_last():
    assert latest_complete_interval(HOURLY, at(10, 20)) == Interval(at(9), at(10))
    assert latest_complete_interval(HOURLY, at(10)) == Interval(at(9), at(10))


def test_other_timezones_are_converted_to_utc():
    paris = datetime.timezone(datetime.timedelta(hours=2))
    moment = datetime.datetime(2026, 10, 9, 12, 20, tzinfo=paris)

    assert interval_containing(HOURLY, moment) == Interval(at(10), at(11))


def test_naive_datetimes_are_refused():
    with pytest.raises(ValueError, match="timezone"):
        interval_containing(HOURLY, datetime.datetime(2026, 10, 9))


def test_due_intervals_begin_at_start_date():
    assert due_intervals(HOURLY, at(0), None, None, at(3, 30), limit=100) == [
        Interval(at(0), at(1)),
        Interval(at(1), at(2)),
        Interval(at(2), at(3)),
    ]


def test_an_interval_ending_exactly_now_is_due():
    assert due_intervals(HOURLY, at(0), None, None, at(1), limit=100) == [
        Interval(at(0), at(1))
    ]


def test_no_interval_starts_before_start_date():
    assert due_intervals(HOURLY, at(0, 10), None, None, at(3, 30), limit=100) == [
        Interval(at(1), at(2)),
        Interval(at(2), at(3)),
    ]


def test_the_cursor_skips_what_was_handled():
    assert due_intervals(HOURLY, at(0), None, at(2), at(3, 30), limit=100) == [
        Interval(at(2), at(3))
    ]


def test_a_start_date_after_the_cursor_wins():
    assert due_intervals(HOURLY, at(2), None, at(0), at(3, 30), limit=100) == [
        Interval(at(2), at(3))
    ]


def test_end_date_is_the_last_end():
    assert due_intervals(HOURLY, at(0), at(2), None, at(5), limit=100) == [
        Interval(at(0), at(1)),
        Interval(at(1), at(2)),
    ]


def test_catching_up_a_year_is_bounded_by_the_limit():
    start = datetime.datetime(2025, 10, 9, tzinfo=UTC)

    due = due_intervals(HOURLY, start, None, None, at(0), limit=100)

    assert len(due) == 100
    assert due[0] == Interval(start, start + datetime.timedelta(hours=1))
    assert due[-1].end == start + datetime.timedelta(hours=100)


def test_next_fire_times_are_strictly_after():
    assert next_fire_times(HOURLY, at(10, 20), 3) == [at(11), at(12), at(13)]
    assert next_fire_times(HOURLY, at(11), 1) == [at(12)]
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_schedule.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.schedule'`.

- [ ] **Step 3: Write the implementation**

`src/docker_jobs_backend/schedule.py`:

```python
"""Cron schedules as data intervals, Airflow style

A job's schedule is a 5-field cron expression, in UTC. Consecutive fire times
delimit its data intervals: the run for [start, end) fires at end, and its
logical date is start. An hourly job firing at 01:00 covers [00:00, 01:00).

croniter's get_next/get_prev are strict (never the base time itself); cron is
minute-grained, so "the latest fire at or before t" is get_prev from the start
of the minute after t.
"""

import dataclasses
import datetime
from typing import List, Optional

from croniter import croniter

__all__ = [
    "Interval",
    "due_intervals",
    "interval_containing",
    "latest_complete_interval",
    "next_fire_times",
    "validate_cron",
]

UTC = datetime.UTC
_MINUTE = datetime.timedelta(minutes=1)


@dataclasses.dataclass(frozen=True)
class Interval:
    """[start, end): the data a run covers"""

    start: datetime.datetime
    end: datetime.datetime


def validate_cron(expression: str) -> str:
    """the expression with single spaces; ValueError unless 5-field cron"""
    normalised = " ".join(expression.split())

    if len(normalised.split(" ")) != 5 or not croniter.is_valid(normalised):
        raise ValueError(f"not a 5-field cron expression: {expression!r}")

    return normalised


def _utc(moment: datetime.datetime) -> datetime.datetime:
    if moment.tzinfo is None:
        raise ValueError(f"naive datetime {moment!r}: give it a timezone")
    return moment.astimezone(UTC)


def _next(expression: str, after: datetime.datetime) -> datetime.datetime:
    return croniter(expression, after).get_next(datetime.datetime)


def _prev(expression: str, before: datetime.datetime) -> datetime.datetime:
    return croniter(expression, before).get_prev(datetime.datetime)


def _latest_fire_at_or_before(
    expression: str, moment: datetime.datetime
) -> datetime.datetime:
    floor = moment.replace(second=0, microsecond=0)
    return _prev(expression, floor + _MINUTE)


def _first_fire_at_or_after(
    expression: str, moment: datetime.datetime
) -> datetime.datetime:
    latest = _latest_fire_at_or_before(expression, moment)
    return latest if latest == moment else _next(expression, moment)


def interval_containing(expression: str, moment: datetime.datetime) -> Interval:
    """the interval moment falls in; a fire time starts its own"""
    start = _latest_fire_at_or_before(expression, _utc(moment))
    return Interval(start, _next(expression, start))


def latest_complete_interval(expression: str, now: datetime.datetime) -> Interval:
    """the interval that ended last, at or before now"""
    end = _latest_fire_at_or_before(expression, _utc(now))
    return Interval(_prev(expression, end), end)


def due_intervals(
    expression: str,
    start_date: datetime.datetime,
    end_date: Optional[datetime.datetime],
    cursor: Optional[datetime.datetime],
    now: datetime.datetime,
    limit: int,
) -> List[Interval]:
    """the intervals that ended in (lower, now], oldest first, at most limit

    lower is the cursor -- the end of the last interval already handled -- but
    never before the first fire time at or after start_date: no interval
    starts before start_date. With end_date, no interval ends after it.
    """
    first = _first_fire_at_or_after(expression, _utc(start_date))
    lower = first if cursor is None else max(_utc(cursor), first)
    upper = _utc(now) if end_date is None else min(_utc(now), _utc(end_date))

    intervals: List[Interval] = []
    end = _next(expression, lower)

    while end <= upper and len(intervals) < limit:
        intervals.append(Interval(_prev(expression, end), end))
        end = _next(expression, end)

    return intervals


def next_fire_times(
    expression: str, after: datetime.datetime, count: int
) -> List[datetime.datetime]:
    """the next count fire times strictly after after"""
    iterator = croniter(expression, _utc(after))
    return [iterator.get_next(datetime.datetime) for _ in range(count)]
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_schedule.py -q`
Expected: all pass (`22 passed`).

- [ ] **Step 5: Commit**

```bash
git add src/docker_jobs_backend/schedule.py tests/test_schedule.py
git commit -m "feat: cron schedules as Airflow-style data intervals" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Command templates (`templating.py`)

**Files:**
- Create: `src/docker_jobs_backend/templating.py`
- Test: `tests/test_templating.py`

**Interfaces:**
- Consumes: `schedule.Interval`
- Produces:
  - `TemplateProblem(ValueError)` with attributes `.field` (e.g. `"command[1]"`, `"env.IFACE"`) and `.message`
  - `iso_ms(dt) -> str` and `iso(dt) -> str`
  - `build_context(interval, run_id, try_number, job_name, params) -> dict`
  - `Rendered(entrypoint: Optional[List[str]], command: List[str], env: Dict[str, str])`
  - `render(entrypoint, command, env, context) -> Rendered`
  - `validate_templates(entrypoint, command, env, params, job_name) -> Rendered`

- [ ] **Step 1: Write the failing tests**

`tests/test_templating.py`:

```python
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
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_templating.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.templating'`.

- [ ] **Step 3: Write the implementation**

`src/docker_jobs_backend/templating.py`:

```python
"""Jinja templates in a job's entrypoint, command and environment

Each element of the entrypoint and command lists, and each environment value,
is a template rendered on its own when an attempt is launched. There is no
shell: a rendered value with spaces stays one argument. The sandbox keeps
Python internals out; StrictUndefined turns a typo into an error instead of an
empty string.

Context:
    data_interval_start, data_interval_end    aware UTC datetimes
    logical_date                               = data_interval_start
    ds                                         logical date, YYYY-MM-DD
    ts                                         logical date, ISO 8601
    run_id, try_number, job_name
    params                                     the job's own dict
    timedelta                                  for window arithmetic
Filters:
    iso_ms   2026-10-09T00:00:00.000Z
    iso      2026-10-09T00:00:00Z

    --replay-start-date {{ (data_interval_end - timedelta(days=7)) | iso_ms }}
"""

import dataclasses
import datetime
from typing import Dict, List, Mapping, Optional, Sequence

from jinja2 import StrictUndefined
from jinja2.sandbox import ImmutableSandboxedEnvironment

from docker_jobs_backend.schedule import Interval

__all__ = [
    "Rendered",
    "TemplateProblem",
    "build_context",
    "iso",
    "iso_ms",
    "render",
    "validate_templates",
]

UTC = datetime.UTC
SAMPLE_INTERVAL = Interval(
    datetime.datetime(2026, 1, 1, tzinfo=UTC), datetime.datetime(2026, 1, 1, 1, tzinfo=UTC)
)


class TemplateProblem(ValueError):
    """a template that does not render; field says which one"""

    def __init__(self, field: str, message: str):
        super().__init__(f"{field}: {message}")
        self.field = field
        self.message = message


def iso_ms(moment: datetime.datetime) -> str:
    """2026-10-09T00:00:00.000Z"""
    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def iso(moment: datetime.datetime) -> str:
    """2026-10-09T00:00:00Z"""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


_ENVIRONMENT = ImmutableSandboxedEnvironment(
    undefined=StrictUndefined, autoescape=False, keep_trailing_newline=True
)
_ENVIRONMENT.filters["iso_ms"] = iso_ms
_ENVIRONMENT.filters["iso"] = iso


@dataclasses.dataclass(frozen=True)
class Rendered:
    """an attempt's entrypoint, command and environment, rendered"""

    entrypoint: Optional[List[str]]
    command: List[str]
    env: Dict[str, str]


def build_context(
    interval: Interval,
    run_id: int,
    try_number: int,
    job_name: str,
    params: Mapping[str, str],
) -> dict:
    """what the templates of one attempt see"""
    return {
        "data_interval_start": interval.start,
        "data_interval_end": interval.end,
        "logical_date": interval.start,
        "ds": interval.start.strftime("%Y-%m-%d"),
        "ts": interval.start.isoformat(),
        "run_id": run_id,
        "try_number": try_number,
        "job_name": job_name,
        "params": dict(params),
        "timedelta": datetime.timedelta,
    }


def _render_one(field: str, template: str, context: dict) -> str:
    try:
        return _ENVIRONMENT.from_string(template).render(context)
    except Exception as exc:  # syntax, undefined, sandbox, or the expression itself
        raise TemplateProblem(field, str(exc)) from exc


def render(
    entrypoint: Optional[Sequence[str]],
    command: Sequence[str],
    env: Mapping[str, str],
    context: dict,
) -> Rendered:
    """every template rendered; TemplateProblem on the first that fails"""
    return Rendered(
        entrypoint=None
        if entrypoint is None
        else [_render_one(f"entrypoint[{i}]", item, context) for i, item in enumerate(entrypoint)],
        command=[_render_one(f"command[{i}]", item, context) for i, item in enumerate(command)],
        env={name: _render_one(f"env.{name}", value, context) for name, value in env.items()},
    )


def validate_templates(
    entrypoint: Optional[Sequence[str]],
    command: Sequence[str],
    env: Mapping[str, str],
    params: Mapping[str, str],
    job_name: str,
) -> Rendered:
    """render with a sample interval: TemplateProblem if any template is broken"""
    return render(
        entrypoint, command, env, build_context(SAMPLE_INTERVAL, 0, 1, job_name, params)
    )
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_templating.py -q`
Expected: `10 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/docker_jobs_backend/templating.py tests/test_templating.py
git commit -m "feat: sandboxed Jinja templates for job commands" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Storage (`db.py`)

**Files:**
- Create: `src/docker_jobs_backend/db.py`
- Create: `tests/factories.py`
- Modify: `tests/conftest.py` (add the `session_factory` and `session` fixtures)
- Test: `tests/test_db.py`

**Interfaces:**
- Produces:
  - `utcnow() -> datetime` and `new_token() -> str` (32 hex characters).
  - `UTCDateTime`, a SQLAlchemy TypeDecorator.
  - `StrEnum`s:
    - `RunState`: `QUEUED`, `RUNNING`, `RETRYING`, `SUCCESS`, `FAILED`, `CANCELLED`
    - `AttemptState`: `PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `TIMEOUT`, `LOST`, `CANCELLED`
    - `RunType`: `SCHEDULED`, `MANUAL`
    - `Reason`: `SCHEDULED`, `MANUAL`, `RETRY`, `RERUN`
  - `TERMINAL_RUN_STATES`, a frozenset.
  - ORM models `Job`, `Run` and `Attempt`; their columns are listed in the code below. `Run.try_number` is a property, `Run.attempts` is ordered by `try_number`, and `Attempt.token` defaults to `new_token()`.
  - `create_session_factory(database_url: str) -> sessionmaker`. It creates the schema and uses `expire_on_commit=False`.
  - Test helpers in `tests/factories.py`: `T0`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `make_job(session, **overrides) -> Job` (committed) and `make_run(job, start=T0, run_type=..., state=..., attempts=1, attempt_state=...) -> Run` (not added to a session).
  - Fixtures `session_factory` and `session`.

- [ ] **Step 1: Write the test helpers and fixtures**

`tests/factories.py`:

```python
"""Test data with fixed times: jobs, runs, their attempts"""

import datetime

from docker_jobs_backend.db import Attempt, AttemptState, Job, Reason, Run, RunState, RunType

UTC = datetime.UTC
T0 = datetime.datetime(2026, 10, 9, tzinfo=UTC)
SECOND = datetime.timedelta(seconds=1)
MINUTE = datetime.timedelta(minutes=1)
HOUR = datetime.timedelta(hours=1)
DAY = datetime.timedelta(days=1)


def make_job(session, **overrides) -> Job:
    """an hourly job from T0, committed; any column can be overridden"""
    values = dict(
        name="collect",
        description="",
        image="127.0.0.1:8085/maas-collector:3.10.1",
        entrypoint=None,
        command=["--start", "{{ data_interval_start | iso_ms }}"],
        env={},
        params={},
        schedule="0 * * * *",
        start_date=T0,
        end_date=None,
        enabled=True,
        catchup=True,
        max_active_runs=1,
        retries=0,
        retry_delay_seconds=300,
        timeout_seconds=3600,
        networks=[],
        constraints=[],
        secrets=[],
        configs=[],
        mounts=[],
        cpu_limit=None,
        memory_limit_mb=None,
        schedule_cursor=None,
        created_at=T0,
        updated_at=T0,
        updated_by="",
    )
    values.update(overrides)
    job = Job(**values)
    session.add(job)
    session.commit()
    return job


def make_run(
    job,
    start=T0,
    run_type=RunType.SCHEDULED,
    state=RunState.QUEUED,
    attempts=1,
    attempt_state=AttemptState.PENDING,
) -> Run:
    """a run of job over [start, start + 1h), NOT added to a session

    Attempts are numbered from 1; the last is in attempt_state, earlier ones
    failed.
    """
    run = Run(
        job=job,
        logical_date=start,
        data_interval_end=start + HOUR,
        run_type=run_type,
        state=state,
        max_tries=job.retries + 1,
        created_at=start + HOUR,
        requested_by="",
        note="",
    )
    for number in range(1, attempts + 1):
        run.attempts.append(
            Attempt(
                try_number=number,
                reason=Reason.SCHEDULED if number == 1 else Reason.RETRY,
                state=attempt_state if number == attempts else AttemptState.FAILED,
                requested_by="",
                note="",
                created_at=start + HOUR,
            )
        )
    return run
```

Add to `tests/conftest.py`, at the import block and after the `settings` fixture:

```python
from docker_jobs_backend.db import create_session_factory


@pytest.fixture(name="session_factory")
def session_factory_fixture(settings):
    return create_session_factory(settings.database_url)


@pytest.fixture(name="session")
def session_fixture(session_factory):
    with session_factory() as session:
        yield session
```

- [ ] **Step 2: Write the failing tests**

`tests/test_db.py`:

```python
"""Storage: timezones, uniqueness, cascades, concurrent writers"""

import datetime
import threading

import pytest
from factories import HOUR, T0, make_job, make_run
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError, StatementError

from docker_jobs_backend.db import Attempt, Job, Run, RunType


def test_datetimes_come_back_aware_utc(session_factory):
    paris = datetime.timezone(datetime.timedelta(hours=2))
    with session_factory() as session:
        job_id = make_job(session, start_date=datetime.datetime(2026, 10, 9, 2, tzinfo=paris)).id

    with session_factory() as session:
        stored = session.get(Job, job_id)

    assert stored.start_date == T0
    assert stored.start_date.utcoffset() == datetime.timedelta(0)


def test_a_naive_datetime_is_refused(session):
    with pytest.raises(StatementError, match="timezone"):
        make_job(session, start_date=datetime.datetime(2026, 10, 9))


def test_one_scheduled_run_per_interval_but_manual_runs_may_repeat_it(session):
    job = make_job(session)
    session.add_all(
        [
            make_run(job, run_type=RunType.MANUAL),
            make_run(job, run_type=RunType.MANUAL),
            make_run(job),
        ]
    )
    session.commit()

    session.add(make_run(job))
    with pytest.raises(IntegrityError):
        session.commit()


def test_deleting_a_job_deletes_its_runs_and_attempts(session):
    job = make_job(session)
    session.add(make_run(job, attempts=2))
    session.commit()

    session.delete(job)
    session.commit()

    assert session.scalar(select(func.count()).select_from(Run)) == 0
    assert session.scalar(select(func.count()).select_from(Attempt)) == 0


def test_ids_are_never_reused(session):
    job_id = make_job(session).id
    run = make_run(session.get(Job, job_id))
    session.add(run)
    session.commit()
    first = run.id

    session.delete(run)
    session.commit()
    session.expire_all()
    again = make_run(session.get(Job, job_id), start=T0 + HOUR)
    session.add(again)
    session.commit()

    assert again.id > first


def test_attempts_are_ordered_and_tokened(session):
    job = make_job(session)
    run = make_run(job, attempts=3)
    session.add(run)
    session.commit()

    assert [attempt.try_number for attempt in run.attempts] == [1, 2, 3]
    assert run.try_number == 3
    tokens = {attempt.token for attempt in run.attempts}
    assert len(tokens) == 3 and all(len(token) == 32 for token in tokens)


def test_concurrent_read_modify_write_loses_nothing(session_factory):
    with session_factory() as session:
        job_id = make_job(session, max_active_runs=0).id

    def bump():
        for _ in range(20):
            with session_factory() as session:
                job = session.get(Job, job_id)
                job.max_active_runs += 1
                session.commit()

    threads = [threading.Thread(target=bump) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with session_factory() as session:
        assert session.get(Job, job_id).max_active_runs == 40
```

- [ ] **Step 3: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_db.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.db'`.

- [ ] **Step 4: Write the implementation**

`src/docker_jobs_backend/db.py`:

```python
"""Storage: jobs, their runs, and the attempts of each run

SQLite on the backend's volume, shared by the API's worker threads and the
scheduler thread. Three settings make that safe:

* WAL, so readers are not blocked by the writer;
* BEGIN IMMEDIATE for every transaction: the write lock is taken up front, so
  a read-then-write never fails half-way with "database is locked" (a
  deferred transaction cannot upgrade once another connection wrote) and two
  writers queue for busy_timeout instead;
* foreign_keys=ON, for the ON DELETE CASCADE of runs and attempts.

SQLite keeps no timezone: every datetime goes through UTCDateTime, stored as
naive UTC, read back as aware UTC, and a naive one is refused rather than
guessed. Ids are AUTOINCREMENT: a deleted run's id is never given to a new
one, so a service name (<stack>_<job>_<run>_<try>) never means two runs.
"""

import datetime
import enum
import uuid
from typing import List, Optional

from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker
from sqlalchemy.types import TypeDecorator

__all__ = [
    "Attempt",
    "AttemptState",
    "Base",
    "Job",
    "Reason",
    "Run",
    "RunState",
    "RunType",
    "TERMINAL_RUN_STATES",
    "UTCDateTime",
    "create_session_factory",
    "new_token",
    "utcnow",
]

UTC = datetime.UTC


def utcnow() -> datetime.datetime:
    """now, aware, in UTC"""
    return datetime.datetime.now(tz=UTC)


def new_token() -> str:
    """an attempt's identity on its service: a label no other attempt shares"""
    return uuid.uuid4().hex


class UTCDateTime(TypeDecorator):
    """aware UTC datetimes over SQLite's naive ones"""

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(f"naive datetime {value!r}: give it a timezone")
        return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value, dialect):
        return None if value is None else value.replace(tzinfo=UTC)


class RunState(enum.StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_RUN_STATES = frozenset({RunState.SUCCESS, RunState.FAILED, RunState.CANCELLED})


class AttemptState(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    LOST = "lost"
    CANCELLED = "cancelled"


class RunType(enum.StrEnum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"


class Reason(enum.StrEnum):
    """why an attempt was made"""

    SCHEDULED = "scheduled"
    MANUAL = "manual"
    RETRY = "retry"
    RERUN = "rerun"


class Base(DeclarativeBase):
    pass


class Job(Base):
    """a container to run on a cron schedule, and how"""

    __tablename__ = "jobs"
    __table_args__ = {"sqlite_autoincrement": True}

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(32), unique=True)
    description: Mapped[str] = mapped_column(Text, default="")
    image: Mapped[str] = mapped_column(String(512))
    entrypoint: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    command: Mapped[list] = mapped_column(JSON, default=list)
    env: Mapped[dict] = mapped_column(JSON, default=dict)
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    schedule: Mapped[str] = mapped_column(String(128))
    start_date: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    end_date: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    enabled: Mapped[bool] = mapped_column(default=True)
    catchup: Mapped[bool] = mapped_column(default=True)
    max_active_runs: Mapped[int] = mapped_column(default=1)
    retries: Mapped[int] = mapped_column(default=0)
    retry_delay_seconds: Mapped[int] = mapped_column(default=300)
    timeout_seconds: Mapped[int] = mapped_column(default=3600)
    networks: Mapped[list] = mapped_column(JSON, default=list)
    constraints: Mapped[list] = mapped_column(JSON, default=list)
    secrets: Mapped[list] = mapped_column(JSON, default=list)
    configs: Mapped[list] = mapped_column(JSON, default=list)
    mounts: Mapped[list] = mapped_column(JSON, default=list)
    cpu_limit: Mapped[Optional[float]] = mapped_column(nullable=True)
    memory_limit_mb: Mapped[Optional[int]] = mapped_column(nullable=True)
    # end of the last interval the scheduler created a run for, or skipped
    schedule_cursor: Mapped[Optional[datetime.datetime]] = mapped_column(
        UTCDateTime, nullable=True
    )
    created_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    updated_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    updated_by: Mapped[str] = mapped_column(String(128), default="")

    runs: Mapped[List["Run"]] = relationship(
        back_populates="job", cascade="all, delete-orphan", passive_deletes=True
    )


class Run(Base):
    """one interval of a job -- or a manual run of one -- and its outcome"""

    __tablename__ = "runs"
    __table_args__ = (
        # the scheduler's idempotency: one scheduled run per interval;
        # manual runs may repeat one
        Index(
            "uq_runs_scheduled_interval",
            "job_id",
            "logical_date",
            unique=True,
            sqlite_where=text("run_type = 'scheduled'"),
        ),
        Index("ix_runs_state", "state"),
        {"sqlite_autoincrement": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), index=True)
    # = data_interval_start
    logical_date: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    data_interval_end: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    run_type: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    max_tries: Mapped[int]
    created_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    started_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    finished_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    requested_by: Mapped[str] = mapped_column(String(128), default="")
    note: Mapped[str] = mapped_column(Text, default="")

    job: Mapped[Job] = relationship(back_populates="runs")
    attempts: Mapped[List["Attempt"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Attempt.try_number",
    )

    @property
    def try_number(self) -> int:
        """the number of the latest attempt"""
        return max((attempt.try_number for attempt in self.attempts), default=0)


class Attempt(Base):
    """one try of a run: one Swarm service"""

    __tablename__ = "attempts"
    __table_args__ = (
        UniqueConstraint("run_id", "try_number"),
        Index("ix_attempts_state", "state"),
        {"sqlite_autoincrement": True},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"), index=True)
    try_number: Mapped[int]
    token: Mapped[str] = mapped_column(String(32), unique=True, default=new_token)
    reason: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16))
    requested_by: Mapped[str] = mapped_column(String(128), default="")
    note: Mapped[str] = mapped_column(Text, default="")
    not_before: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    service_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    service_name: Mapped[Optional[str]] = mapped_column(String(63), nullable=True)
    rendered_entrypoint: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    rendered_command: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    exit_code: Mapped[Optional[int]] = mapped_column(nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    node_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime.datetime] = mapped_column(UTCDateTime)
    launched_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    started_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)
    finished_at: Mapped[Optional[datetime.datetime]] = mapped_column(UTCDateTime, nullable=True)

    run: Mapped[Run] = relationship(back_populates="attempts")


def _sqlite_connect(dbapi_connection, _record) -> None:
    # SQLAlchemy emits BEGIN itself (_sqlite_begin), not pysqlite
    dbapi_connection.isolation_level = None
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


def _sqlite_begin(connection) -> None:
    connection.exec_driver_sql("BEGIN IMMEDIATE")


def create_session_factory(database_url: str) -> sessionmaker:
    """the engine, the schema if it is missing, and sessions over them"""
    sqlite = database_url.startswith("sqlite")
    engine = create_engine(
        database_url, connect_args={"check_same_thread": False} if sqlite else {}
    )

    if sqlite:
        event.listen(engine, "connect", _sqlite_connect)
        event.listen(engine, "begin", _sqlite_begin)

    Base.metadata.create_all(engine)

    return sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_db.py -q`
Expected: `7 passed`. If `test_concurrent_read_modify_write_loses_nothing` fails with a count below 40, the `begin` listener is not firing. Check that `event.listen(engine, "begin", ...)` is attached before `create_all`.

- [ ] **Step 6: Commit**

```bash
git add src/docker_jobs_backend/db.py tests/factories.py tests/conftest.py tests/test_db.py
git commit -m "feat: SQLite storage for jobs, runs and attempts" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Swarm runner (`runner.py`)

**Files:**
- Create: `src/docker_jobs_backend/runner.py`
- Test: `tests/test_runner.py`

**Interfaces:**
- Consumes: `Settings.run_stack` and `Settings.loki_*`.
- Produces:
  - Constants `LOG_DRIVER`, `STACK_LABEL`, `MANAGED_LABEL`, `JOB_LABEL`, `RUN_LABEL`, `ATTEMPT_LABEL`.
  - Task states `PENDING`, `RUNNING`, `COMPLETE`, `FAILED`, `GONE`.
  - Exceptions `LaunchError` and `RunnerUnavailable`.
  - `LaunchSpec(service_name, image, entrypoint, command, env, labels, networks=(), constraints=(), secrets=(), configs=(), mounts=(), cpu_limit=None, memory_limit_mb=None)`. `labels[ATTEMPT_LABEL]` is the attempt token. secrets, configs and mounts are dicts.
  - `TaskStatus(state, exit_code=None, error=None, node_id=None)`.
  - `Runner` protocol:
    - `launch(spec) -> str` (service id)
    - `status(service_id) -> TaskStatus`
    - `remove(service_id) -> None`
    - `managed_services() -> Dict[str, str]` (service id → attempt token)
    - `missing_objects(secrets, configs, networks) -> List[str]`
  - `SwarmRunner(settings, client_factory=...)`, which implements `Runner` and also provides `service_kwargs(spec, client) -> dict` and `log_options() -> dict`.

- [ ] **Step 1: Write the failing tests**

`tests/test_runner.py`:

```python
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
        self.networks = FakeObjects(**{"ocs_rabbitmq": "net-1"})
        self.api = FakeAPI()


@pytest.fixture(name="docker_client")
def docker_client_fixture():
    return FakeClient()


@pytest.fixture(name="swarm")
def swarm_fixture(docker_client):
    return SwarmRunner(Settings(), client_factory=lambda: docker_client)


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
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_runner.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.runner'`.

- [ ] **Step 3: Write the implementation**

`src/docker_jobs_backend/runner.py`:

```python
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
            "networks": list(spec.networks),
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
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_runner.py -q`
Expected: `25 passed`.

- [ ] **Step 5: Commit**

```bash
git add src/docker_jobs_backend/runner.py tests/test_runner.py
git commit -m "feat: Swarm runner, one replicated-job service per attempt" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Metrics (`telemetry.py`)

**Files:**
- Create: `src/docker_jobs_backend/telemetry.py`
- Modify: `tests/factories.py` (add `points` and `attrs`)
- Modify: `tests/conftest.py` (add the `reader` and `metrics` fixtures)
- Test: `tests/test_telemetry.py`

**Interfaces:**
- Consumes:
  - `db.Job`, `db.Run`, `db.RunState`, `db.AttemptState`
  - `schedule.due_intervals`
  - `Settings.otel_service_name`, `Settings.run_stack`, `Settings.gap_grace_seconds`, `Settings.gap_lookback_hours`
- Produces:
  - `JOB = "docker_job"` and `DURATION_BUCKETS`.
  - `create_meter_provider(settings) -> MeterProvider`.
  - `count_gaps(session, job, now, grace: timedelta, lookback: timedelta) -> int`.
  - `Metrics(meter_provider, settings)`, which has:
    - `register_job(name)` and `register_jobs(names)`
    - `run_created(job, run_type)` and `attempt_started(job, reason)`
    - `attempt_finished(job, outcome, duration_seconds)` and `run_finished(job, state)`
    - `intervals_skipped(job, count)`
    - `refresh(session, now)`, `docker_reachable(bool)` and `tick(now)`
    - the property `last_tick -> Optional[datetime]`
  - Test helpers: `points(reader, name) -> Dict[frozenset, number]`, where a histogram gives its count, and `attrs(**kw) -> frozenset`.
  - Fixtures `reader` (an `InMemoryMetricReader`) and `metrics`.

- [ ] **Step 1: Add the test helpers and fixtures**

Append to `tests/factories.py`:

```python
def attrs(**attributes) -> frozenset:
    """a data point's attributes, as points() keys them"""
    return frozenset(attributes.items())


def points(reader, name) -> dict:
    """{attributes: value} of one metric at a fresh collection; a histogram gives its count"""
    found = {}
    data = reader.get_metrics_data()
    if data is None:
        return found
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != name:
                    continue
                for point in metric.data.data_points:
                    value = point.count if hasattr(point, "bucket_counts") else point.value
                    found[frozenset(point.attributes.items())] = value
    return found
```

Add to `tests/conftest.py`:

```python
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from docker_jobs_backend.telemetry import Metrics


@pytest.fixture(name="reader")
def reader_fixture():
    return InMemoryMetricReader()


@pytest.fixture(name="metrics")
def metrics_fixture(reader, settings):
    return Metrics(MeterProvider(metric_readers=[reader]), settings)
```

- [ ] **Step 2: Write the failing tests**

`tests/test_telemetry.py`:

```python
"""Metrics: counters by job, gauges from the database snapshot"""

from factories import HOUR, MINUTE, T0, attrs, make_job, make_run, points

from docker_jobs_backend.db import AttemptState, Reason, RunState, RunType
from docker_jobs_backend.telemetry import DURATION_BUCKETS

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
```

- [ ] **Step 3: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_telemetry.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.telemetry'`, raised from conftest.

- [ ] **Step 4: Write the implementation**

`src/docker_jobs_backend/telemetry.py`:

```python
"""Metrics, pushed over OTLP/HTTP to Alloy, on to Mimir

Instruments carry OpenTelemetry's dotted names. Mimir's OTLP endpoint turns
them into Prometheus names -- dots to underscores, a unit of seconds adds
_seconds, a counter adds _total -- and those are what the alert rules
(deploy/grafana/docker-jobs-alerts.yaml) query:

    docker_jobs.runs.created         docker_jobs_runs_created_total{docker_job, run_type}
    docker_jobs.runs.finished        docker_jobs_runs_finished_total{docker_job, state}
    docker_jobs.attempts.started     docker_jobs_attempts_started_total{docker_job, reason}
    docker_jobs.attempts.finished    docker_jobs_attempts_finished_total{docker_job, outcome}
    docker_jobs.intervals.skipped    docker_jobs_intervals_skipped_total{docker_job}
    docker_jobs.attempt.duration     docker_jobs_attempt_duration_seconds_*{docker_job, outcome}
    docker_jobs.job.gaps             docker_jobs_job_gaps{docker_job}
    docker_jobs.job.covered_until    docker_jobs_job_covered_until_seconds{docker_job}
    docker_jobs.runs.active          docker_jobs_runs_active{docker_job, state}
    docker_jobs.docker.up            docker_jobs_docker_up
    docker_jobs.scheduler.heartbeat  docker_jobs_scheduler_heartbeat_seconds

The job is the docker_job attribute, never job: Mimir fills job from the
resource's service.name.

Outcome counters start at 0 for every job (register_job): PromQL's increase()
needs a sample before the first failure to see it.

Gauges come from a snapshot the scheduler thread computes at the end of every
tick (refresh); the export thread only reads it. A callback querying the
database itself would have its errors swallowed by the SDK, and the gauge
would silently vanish -- the gaps alert with it.
"""

import dataclasses
import datetime
from typing import Dict, Iterable, Optional, Tuple

from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import CallbackOptions, MeterProvider, Observation
from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from docker_jobs_backend.db import AttemptState, Job, Run, RunState
from docker_jobs_backend.schedule import due_intervals
from docker_jobs_backend.settings import Settings

__all__ = ["DURATION_BUCKETS", "JOB", "Metrics", "count_gaps", "create_meter_provider"]

JOB = "docker_job"
GAP_LIMIT = 10_000
ACTIVE_STATES = (RunState.QUEUED, RunState.RUNNING, RunState.RETRYING)
NOT_DONE_STATES = frozenset({RunState.FAILED, RunState.CANCELLED, RunState.QUEUED, RunState.RETRYING})
RUN_OUTCOMES = (RunState.SUCCESS, RunState.FAILED, RunState.CANCELLED)
ATTEMPT_OUTCOMES = (
    AttemptState.SUCCESS,
    AttemptState.FAILED,
    AttemptState.TIMEOUT,
    AttemptState.LOST,
    AttemptState.CANCELLED,
)
# seconds: from a quick check to a day-long recollect
DURATION_BUCKETS = (1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200, 14400, 28800, 86400)


def create_meter_provider(settings: Settings) -> SDKMeterProvider:
    """OTLP over HTTP, as the standard variables say

    OTEL_EXPORTER_OTLP_ENDPOINT (http://alloy:4318), OTEL_METRIC_EXPORT_INTERVAL
    (milliseconds, 60000 by default), OTEL_SDK_DISABLED=true to turn it off.
    service.instance.id is the run stack: stable across restarts, so a restart
    continues the same series, and two backends never mix theirs.
    """
    resource = Resource.create(
        {"service.name": settings.otel_service_name, "service.instance.id": settings.run_stack}
    )
    reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    return SDKMeterProvider(resource=resource, metric_readers=[reader])


def count_gaps(
    session: Session,
    job: Job,
    now: datetime.datetime,
    grace: datetime.timedelta,
    lookback: datetime.timedelta,
) -> int:
    """intervals of job that ended in (now - lookback, now - grace] with no data

    Two kinds, each interval counted once:

    * not scheduled: ended after the job's cursor -- the scheduler is down, or
      behind. Computed with the current schedule from the cursor on only, so a
      schedule change never invents a past gap;
    * not done: it has runs, none succeeded, and one failed, was cancelled, or
      still waits to start or to retry. A run in progress is no gap: the
      timeouts deal with runs that never end.
    """
    until = now - grace
    since = now - lookback
    lower = since if job.schedule_cursor is None else max(job.schedule_cursor, since)
    unscheduled = {
        interval.start
        for interval in due_intervals(job.schedule, job.start_date, job.end_date, lower, until, GAP_LIMIT)
    }

    rows = session.execute(
        select(Run.logical_date, Run.state).where(
            Run.job_id == job.id,
            Run.data_interval_end > since,
            Run.data_interval_end <= until,
        )
    ).all()
    done = {logical for logical, state in rows if state == RunState.SUCCESS}
    undone = {logical for logical, state in rows if state in NOT_DONE_STATES}

    return len((unscheduled | undone) - done)


@dataclasses.dataclass(frozen=True)
class Snapshot:
    """the gauges, as of the last refresh"""

    gaps: Dict[str, int] = dataclasses.field(default_factory=dict)
    covered_until: Dict[str, float] = dataclasses.field(default_factory=dict)
    active: Dict[Tuple[str, str], int] = dataclasses.field(default_factory=dict)


class Metrics:
    """the instruments the lifecycle and the scheduler record into"""

    def __init__(self, meter_provider: MeterProvider, settings: Settings):
        self._settings = settings
        self._snapshot = Snapshot()
        self._docker_up: Optional[bool] = None
        self._last_tick: Optional[datetime.datetime] = None

        meter = meter_provider.get_meter("docker_jobs_backend")
        self._runs_created = meter.create_counter(
            "docker_jobs.runs.created", description="Runs created, by run_type: scheduled or manual"
        )
        self._runs_finished = meter.create_counter(
            "docker_jobs.runs.finished",
            description="Runs ended, by state: success, failed (no try left), cancelled",
        )
        self._attempts_started = meter.create_counter(
            "docker_jobs.attempts.started",
            description="Attempts started, by reason: scheduled, manual, retry, rerun",
        )
        self._attempts_finished = meter.create_counter(
            "docker_jobs.attempts.finished",
            description="Attempts ended, by outcome: success, failed, timeout, lost, cancelled",
        )
        self._intervals_skipped = meter.create_counter(
            "docker_jobs.intervals.skipped",
            description="Intervals never run because the job does not catch up",
        )
        self._duration = meter.create_histogram(
            "docker_jobs.attempt.duration",
            unit="s",
            description="Attempt duration, launch to outcome",
            explicit_bucket_boundaries_advisory=DURATION_BUCKETS,
        )
        meter.create_observable_gauge(
            "docker_jobs.job.gaps",
            callbacks=[self._observe_gaps],
            description="Intervals that ended in the lookback window with no data",
        )
        meter.create_observable_gauge(
            "docker_jobs.job.covered_until",
            unit="s",
            callbacks=[self._observe_covered_until],
            description="End of the latest interval with a successful run, Unix time",
        )
        meter.create_observable_gauge(
            "docker_jobs.runs.active",
            callbacks=[self._observe_active],
            description="Runs not ended, by state: queued, running, retrying",
        )
        meter.create_observable_gauge(
            "docker_jobs.docker.up",
            callbacks=[self._observe_docker_up],
            description="1 when the last tick reached Docker, 0 when it could not",
        )
        meter.create_observable_gauge(
            "docker_jobs.scheduler.heartbeat",
            unit="s",
            callbacks=[self._observe_heartbeat],
            description="Unix time of the last completed scheduler tick",
        )

    # --- events -----------------------------------------------------------------

    def register_job(self, job: str) -> None:
        """start the job's outcome counters at 0, so increase() sees the first"""
        for state in RUN_OUTCOMES:
            self._runs_finished.add(0, {JOB: job, "state": str(state)})
        for outcome in ATTEMPT_OUTCOMES:
            self._attempts_finished.add(0, {JOB: job, "outcome": str(outcome)})

    def register_jobs(self, jobs: Iterable[str]) -> None:
        for job in jobs:
            self.register_job(job)

    def run_created(self, job: str, run_type: str) -> None:
        self._runs_created.add(1, {JOB: job, "run_type": str(run_type)})

    def attempt_started(self, job: str, reason: str) -> None:
        self._attempts_started.add(1, {JOB: job, "reason": str(reason)})

    def attempt_finished(self, job: str, outcome: str, duration_seconds: float) -> None:
        attributes = {JOB: job, "outcome": str(outcome)}
        self._attempts_finished.add(1, attributes)
        self._duration.record(max(duration_seconds, 0.0), attributes)

    def run_finished(self, job: str, state: str) -> None:
        self._runs_finished.add(1, {JOB: job, "state": str(state)})

    def intervals_skipped(self, job: str, count: int) -> None:
        self._intervals_skipped.add(count, {JOB: job})

    # --- the scheduler's ----------------------------------------------------

    def refresh(self, session: Session, now: datetime.datetime) -> None:
        """recompute the gauges from the database"""
        grace = datetime.timedelta(seconds=self._settings.gap_grace_seconds)
        lookback = datetime.timedelta(hours=self._settings.gap_lookback_hours)
        jobs = session.scalars(select(Job).order_by(Job.name)).all()

        covered = session.execute(
            select(Job.name, func.max(Run.data_interval_end))
            .join(Run, Run.job_id == Job.id)
            .where(Run.state == RunState.SUCCESS)
            .group_by(Job.name)
        ).all()
        active = {(job.name, str(state)): 0 for job in jobs for state in ACTIVE_STATES}
        for name, state, count in session.execute(
            select(Job.name, Run.state, func.count())
            .join(Run, Run.job_id == Job.id)
            .where(Run.state.in_(ACTIVE_STATES))
            .group_by(Job.name, Run.state)
        ):
            active[(name, state)] = count

        self._snapshot = Snapshot(
            gaps={job.name: count_gaps(session, job, now, grace, lookback) for job in jobs if job.enabled},
            covered_until={name: end.timestamp() for name, end in covered},
            active=active,
        )

    def docker_reachable(self, reachable: bool) -> None:
        self._docker_up = reachable

    def tick(self, now: datetime.datetime) -> None:
        """a tick completed"""
        self._last_tick = now

    @property
    def last_tick(self) -> Optional[datetime.datetime]:
        return self._last_tick

    # --- gauge callbacks: memory only ---------------------------------------

    def _observe_gaps(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(count, {JOB: job}) for job, count in self._snapshot.gaps.items()]

    def _observe_covered_until(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [Observation(end, {JOB: job}) for job, end in self._snapshot.covered_until.items()]

    def _observe_active(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [
            Observation(count, {JOB: job, "state": state})
            for (job, state), count in self._snapshot.active.items()
        ]

    def _observe_docker_up(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [] if self._docker_up is None else [Observation(1 if self._docker_up else 0)]

    def _observe_heartbeat(self, _options: CallbackOptions) -> Iterable[Observation]:
        return [] if self._last_tick is None else [Observation(self._last_tick.timestamp())]
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_telemetry.py -q`
Expected: `11 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/docker_jobs_backend/telemetry.py tests/factories.py tests/conftest.py tests/test_telemetry.py
git commit -m "feat: OTLP metrics for runs, attempts, gaps and the scheduler" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Run lifecycle (`lifecycle.py`)

**Files:**
- Create: `src/docker_jobs_backend/lifecycle.py`
- Modify: `tests/factories.py` (add `FakeRunner`)
- Modify: `tests/conftest.py` (add the `runner` and `lifecycle` fixtures)
- Test: `tests/test_lifecycle.py`

**Interfaces:**
- Consumes:
  - `db.*`
  - `runner.LaunchSpec`, `TaskStatus`, the state constants, `LaunchError`, `RunnerUnavailable`, the label constants and the `Runner` protocol
  - `templating.build_context` and `render`
  - `schedule.Interval`
  - `Metrics`, and `Settings.run_stack` / `start_timeout_seconds`
- Produces:
  - `Conflict(Exception)`.
  - `service_name(stack, job_name, run_id, try_number) -> str`.
  - `loki_selector(stack, name) -> str`, of the form `{swarm_stack="djobs", swarm_service="..."}`.
  - `Lifecycle(runner, metrics, settings)`. Its methods:
    - `create_run(session, job, interval, run_type, now, requested_by="", note="") -> Run`. It does not commit.
    - `rerun(run, now, requested_by="", note="") -> Attempt`. It does not commit, and raises `Conflict`.
    - `launch(session, attempt, now) -> None`. It commits, and lets `RunnerUnavailable` propagate.
    - `reconcile(session, attempt, now) -> None`. It commits, and lets `RunnerUnavailable` propagate.
    - `cancel(session, run, now, requested_by="", note="") -> None`. It commits, and raises `Conflict`.
    - `sweep(session) -> int`, the number of services removed.
  - The attributes `lifecycle.runner`, `.metrics` and `.settings`.
  - `FakeRunner`, which keeps `launched`, `services` (id → token), `statuses`, `removed`, `launch_error`, `unavailable` and `existing_objects`. Its helpers are `start(id)` and `finish(id, exit_code)`.

- [ ] **Step 1: Add the fake runner and fixtures**

Add this to the imports at the top of `tests/factories.py`, below the `docker_jobs_backend.db` import:

```python
from docker_jobs_backend.runner import (
    ATTEMPT_LABEL,
    COMPLETE,
    FAILED,
    GONE,
    PENDING,
    RUNNING,
    RunnerUnavailable,
    TaskStatus,
)
```

Then append to `tests/factories.py`:

```python
class FakeRunner:
    """the Runner, in memory: services, their task states, what was removed"""

    def __init__(self):
        self.launched = []  # LaunchSpec, in launch order
        self.services = {}  # service id -> attempt token, as managed_services()
        self.statuses = {}  # service id -> TaskStatus
        self.removed = []
        self.launch_error = None  # an exception launch() raises
        self.unavailable = False
        self.existing_objects = set()  # secret, config and network names

    def _reachable(self):
        if self.unavailable:
            raise RunnerUnavailable("docker unreachable: connection refused")

    def launch(self, spec):
        self._reachable()
        if self.launch_error is not None:
            raise self.launch_error
        self.launched.append(spec)
        service_id = f"svc-{len(self.launched)}"
        self.services[service_id] = spec.labels[ATTEMPT_LABEL]
        self.statuses[service_id] = TaskStatus(PENDING)
        return service_id

    def start(self, service_id):
        """the service's task runs"""
        self.statuses[service_id] = TaskStatus(RUNNING, node_id="node-1")

    def finish(self, service_id, exit_code=0):
        """the service's task exits"""
        if exit_code == 0:
            self.statuses[service_id] = TaskStatus(COMPLETE, exit_code=0, node_id="node-1")
        else:
            self.statuses[service_id] = TaskStatus(
                FAILED, exit_code=exit_code, error=f"task: non-zero exit ({exit_code})", node_id="node-1"
            )

    def status(self, service_id):
        self._reachable()
        if service_id not in self.services:
            return TaskStatus(GONE, error="service no longer exists")
        return self.statuses.get(service_id, TaskStatus(PENDING))

    def remove(self, service_id):
        self._reachable()
        self.removed.append(service_id)
        self.services.pop(service_id, None)
        self.statuses.pop(service_id, None)

    def managed_services(self):
        self._reachable()
        return dict(self.services)

    def missing_objects(self, secrets, configs, networks):
        self._reachable()
        wanted = (("secret", secrets), ("config", configs), ("network", networks))
        return [
            f"{kind} {name!r} does not exist"
            for kind, names in wanted
            for name in names
            if name not in self.existing_objects
        ]
```

Add to `tests/conftest.py`:

```python
from factories import FakeRunner

from docker_jobs_backend.lifecycle import Lifecycle


@pytest.fixture(name="runner")
def runner_fixture():
    return FakeRunner()


@pytest.fixture(name="lifecycle")
def lifecycle_fixture(runner, metrics, settings):
    return Lifecycle(runner, metrics, settings)
```

- [ ] **Step 2: Write the failing tests**

`tests/test_lifecycle.py`:

```python
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
```

- [ ] **Step 3: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_lifecycle.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.lifecycle'`, raised from conftest.

- [ ] **Step 4: Write the implementation**

`src/docker_jobs_backend/lifecycle.py`:

```python
"""What happens to a run: created, launched, finished, retried, rerun, cancelled

The scheduler thread and the API both go through here, so a run follows one
set of rules whoever moves it:

    Run      queued --launch--> running --success--> success
                                   |------failure--> retrying --launch--> running
                                   \------failure--> failed      (no try left)
             queued/running/retrying --cancel--> cancelled
             success/failed/cancelled --rerun--> queued          (fresh tries)

    Attempt  pending -> running -> success | failed | timeout | lost
             pending/running -> cancelled

Concurrency. The API and the scheduler share the database, one transaction
at a time (BEGIN IMMEDIATE, db.py). None is held while Docker answers:
launch() and reconcile() commit first, then re-read the attempt (_still)
before writing what Docker said -- the API may have cancelled the run, or
deleted its job, meanwhile.

Order of effects. An outcome is committed before its service is removed: if
the process dies in between, the sweep removes the service later, whereas the
other order would lose the outcome. A service is created only once its name
and token are committed: if the process dies in between, the next launch of
the same attempt adopts it (SwarmRunner.launch).
"""

import datetime
import logging
from typing import Optional, Tuple

from sqlalchemy import select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

from docker_jobs_backend.db import (
    TERMINAL_RUN_STATES,
    Attempt,
    AttemptState,
    Job,
    Reason,
    Run,
    RunState,
    RunType,
)
from docker_jobs_backend.runner import (
    ATTEMPT_LABEL,
    COMPLETE,
    FAILED,
    GONE,
    JOB_LABEL,
    PENDING,
    RUN_LABEL,
    RUNNING,
    LaunchError,
    LaunchSpec,
    Runner,
    RunnerUnavailable,
    TaskStatus,
)
from docker_jobs_backend.schedule import Interval
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics
from docker_jobs_backend.templating import TemplateProblem, build_context, render

__all__ = ["Conflict", "Lifecycle", "loki_selector", "service_name"]

LOGGER = logging.getLogger(__name__)


class Conflict(Exception):
    """the request contradicts the run's state: the API answers 409"""


def service_name(stack: str, job_name: str, run_id: int, try_number: int) -> str:
    """<stack>_<job>_<run>_<try>: at most 10+1+32+1+10+1+2 = 57 characters, Swarm allows 63"""
    return f"{stack}_{job_name}_{run_id}_{try_number}"


def loki_selector(stack: str, name: str) -> str:
    """the LogQL selector of one attempt's logs, from the labels the loki driver sets"""
    return f'{{swarm_stack="{stack}", swarm_service="{name}"}}'


class Lifecycle:
    """the rules, and the runner and metrics they drive"""

    def __init__(self, runner: Runner, metrics: Metrics, settings: Settings):
        self.runner = runner
        self.metrics = metrics
        self.settings = settings

    # --- new work: the caller commits -----------------------------------------

    def create_run(
        self,
        session: Session,
        job: Job,
        interval: Interval,
        run_type: str,
        now: datetime.datetime,
        requested_by: str = "",
        note: str = "",
    ) -> Run:
        """a queued run of job over interval, its first attempt pending"""
        run = Run(
            job=job,
            logical_date=interval.start,
            data_interval_end=interval.end,
            run_type=run_type,
            state=RunState.QUEUED,
            max_tries=job.retries + 1,
            created_at=now,
            requested_by=requested_by,
            note=note,
        )
        run.attempts.append(
            Attempt(
                try_number=1,
                reason=Reason.SCHEDULED if run_type == RunType.SCHEDULED else Reason.MANUAL,
                state=AttemptState.PENDING,
                requested_by=requested_by,
                note=note,
                created_at=now,
            )
        )
        session.add(run)
        self.metrics.run_created(job.name, run_type)
        return run

    def rerun(
        self, run: Run, now: datetime.datetime, requested_by: str = "", note: str = ""
    ) -> Attempt:
        """queue an ended run again, with the job's retries afresh"""
        if run.state not in TERMINAL_RUN_STATES:
            raise Conflict(f"run {run.id} is {run.state}: only an ended run can be rerun")
        if not run.job.enabled:
            raise Conflict(f"job {run.job.name} is disabled: enable it to rerun its runs")

        tried = run.try_number
        attempt = Attempt(
            try_number=tried + 1,
            reason=Reason.RERUN,
            state=AttemptState.PENDING,
            requested_by=requested_by,
            note=note,
            created_at=now,
        )
        run.attempts.append(attempt)
        run.max_tries = tried + 1 + run.job.retries
        run.state = RunState.QUEUED
        run.finished_at = None
        return attempt

    # --- Docker: these commit -------------------------------------------------

    def launch(self, session: Session, attempt: Attempt, now: datetime.datetime) -> None:
        """start a pending attempt's service; RunnerUnavailable leaves it pending"""
        if not self._still(session, attempt, AttemptState.PENDING):
            session.commit()
            return

        run = attempt.run
        job = run.job
        context = build_context(
            Interval(run.logical_date, run.data_interval_end),
            run.id,
            attempt.try_number,
            job.name,
            job.params,
        )
        try:
            rendered = render(job.entrypoint, job.command, job.env, context)
        except TemplateProblem as exc:
            self._finish(attempt, AttemptState.FAILED, now, error=f"template {exc}")
            session.commit()
            return

        attempt.service_name = service_name(self.settings.run_stack, job.name, run.id, attempt.try_number)
        attempt.rendered_entrypoint = rendered.entrypoint
        attempt.rendered_command = rendered.command
        session.commit()  # the name and the token, before the service exists

        spec = LaunchSpec(
            service_name=attempt.service_name,
            image=job.image,
            entrypoint=rendered.entrypoint,
            command=rendered.command,
            env=rendered.env,
            labels={JOB_LABEL: job.name, RUN_LABEL: str(run.id), ATTEMPT_LABEL: attempt.token},
            networks=job.networks,
            constraints=job.constraints,
            secrets=job.secrets,
            configs=job.configs,
            mounts=job.mounts,
            cpu_limit=job.cpu_limit,
            memory_limit_mb=job.memory_limit_mb,
        )
        try:
            service_id = self.runner.launch(spec)
        except LaunchError as exc:
            if self._still(session, attempt, AttemptState.PENDING):
                self._finish(attempt, AttemptState.FAILED, now, error=str(exc))
            session.commit()
            return

        if not self._still(session, attempt, AttemptState.PENDING):
            # cancelled, or its job deleted, while Docker was creating it
            session.commit()
            self._remove(service_id)
            return

        attempt.service_id = service_id
        attempt.state = AttemptState.RUNNING
        attempt.launched_at = now
        run.state = RunState.RUNNING
        if run.started_at is None:
            run.started_at = now
        session.commit()
        self.metrics.attempt_started(job.name, attempt.reason)

    def reconcile(self, session: Session, attempt: Attempt, now: datetime.datetime) -> None:
        """bring a running attempt up to date with its service"""
        status = self.runner.status(attempt.service_id)

        if not self._still(session, attempt, AttemptState.RUNNING):
            session.commit()
            return

        if status.node_id:
            attempt.node_id = status.node_id

        outcome = self._outcome(attempt, status, now)
        if outcome is None:
            if status.state == RUNNING and attempt.started_at is None:
                attempt.started_at = now
            if status.state == PENDING and status.error:
                attempt.error = status.error  # e.g. "no suitable node", while it waits
            session.commit()
            return

        state, exit_code, error = outcome
        self._finish(attempt, state, now, exit_code=exit_code, error=error)
        session.commit()
        if status.state != GONE:
            self._remove(attempt.service_id)

    def cancel(
        self,
        session: Session,
        run: Run,
        now: datetime.datetime,
        requested_by: str = "",
        note: str = "",
    ) -> None:
        """end a run that has not ended; its service, if any, is removed"""
        if run.state in TERMINAL_RUN_STATES:
            raise Conflict(f"run {run.id} is {run.state}: there is nothing to cancel")

        attempt = run.attempts[-1]
        service_id = attempt.service_id if attempt.state == AttemptState.RUNNING else None
        error = f"cancelled by {requested_by}" + (f": {note}" if note else "")
        self._finish(attempt, AttemptState.CANCELLED, now, error=error)
        session.commit()

        if service_id:
            self._remove(service_id)

    def sweep(self, session: Session) -> int:
        """remove this stack's services whose attempt is over or unknown

        Whatever left one behind -- a removal that failed, a job deleted while
        its attempt launched, a database restored from a backup. The service of
        a pending attempt stays: its next launch adopts it.
        """
        services = self.runner.managed_services()
        if not services:
            return 0

        live = set(
            session.scalars(
                select(Attempt.token).where(
                    Attempt.state.in_([AttemptState.PENDING, AttemptState.RUNNING])
                )
            )
        )
        session.commit()

        removed = 0
        for service_id, token in services.items():
            if token not in live:
                LOGGER.warning("Removing service %s: its attempt is over or unknown", service_id)
                self._remove(service_id)
                removed += 1
        return removed

    # --- internals ------------------------------------------------------------

    @staticmethod
    def _still(session: Session, attempt: Attempt, state: str) -> bool:
        """re-read the attempt's state: is it still state? (False once deleted)"""
        try:
            session.refresh(attempt, ["state"])
        except InvalidRequestError:
            return False
        return attempt.state == state

    def _outcome(
        self, attempt: Attempt, status: TaskStatus, now: datetime.datetime
    ) -> Optional[Tuple[str, Optional[int], Optional[str]]]:
        """(state, exit code, error) once the attempt is over, else None"""
        if status.state == COMPLETE:
            return AttemptState.SUCCESS, status.exit_code, None
        if status.state == FAILED:
            return AttemptState.FAILED, status.exit_code, status.error
        if status.state == GONE:
            return AttemptState.LOST, None, status.error

        waited = (now - attempt.launched_at).total_seconds()
        start_timeout = self.settings.start_timeout_seconds
        if status.state == PENDING and attempt.started_at is None and waited >= start_timeout:
            return (
                AttemptState.TIMEOUT,
                None,
                f"not started after {start_timeout}s: {status.error or 'still pending'}",
            )

        timeout = attempt.run.job.timeout_seconds
        if waited >= timeout:
            return AttemptState.TIMEOUT, None, f"timed out after {timeout}s"
        return None

    def _finish(
        self,
        attempt: Attempt,
        state: str,
        now: datetime.datetime,
        exit_code: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """record an attempt's outcome, then the run's: ended, or a retry"""
        run = attempt.run
        job = run.job
        attempt.state = state
        attempt.exit_code = exit_code
        attempt.error = error
        attempt.finished_at = now
        started = attempt.launched_at or now
        self.metrics.attempt_finished(job.name, state, (now - started).total_seconds())

        if state == AttemptState.SUCCESS:
            self._end_run(run, RunState.SUCCESS, now)
        elif state == AttemptState.CANCELLED:
            self._end_run(run, RunState.CANCELLED, now)
        elif attempt.try_number < run.max_tries:
            run.state = RunState.RETRYING
            run.attempts.append(
                Attempt(
                    try_number=attempt.try_number + 1,
                    reason=Reason.RETRY,
                    state=AttemptState.PENDING,
                    not_before=now + datetime.timedelta(seconds=job.retry_delay_seconds),
                    requested_by="",
                    note="",
                    created_at=now,
                )
            )
        else:
            self._end_run(run, RunState.FAILED, now)

    def _end_run(self, run: Run, state: str, now: datetime.datetime) -> None:
        run.state = state
        run.finished_at = now
        self.metrics.run_finished(run.job.name, state)

    def _remove(self, service_id: str) -> None:
        """best effort: what stays behind, the sweep removes"""
        try:
            self.runner.remove(service_id)
        except RunnerUnavailable as exc:
            LOGGER.warning("Service %s not removed (%s): the sweep will", service_id, exc)
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_lifecycle.py -q`
Expected: `23 passed`.

- [ ] **Step 6: Commit**

```bash
git add src/docker_jobs_backend/lifecycle.py tests/factories.py tests/conftest.py tests/test_lifecycle.py
git commit -m "feat: run lifecycle with retries, reruns, cancels and the sweep" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Scheduler (`scheduler.py`)

**Files:**
- Create: `src/docker_jobs_backend/scheduler.py`
- Modify: `tests/conftest.py` (add the `scheduler` fixture)
- Test: `tests/test_scheduler.py`

**Interfaces:**
- Consumes:
  - `Lifecycle` (`create_run`, `launch`, `reconcile`, `sweep`)
  - `Metrics` (`intervals_skipped`, `docker_reachable`, `refresh`, `tick`)
  - `schedule.due_intervals` and `latest_complete_interval`
  - `RunnerUnavailable`
- Produces:
  - `Scheduler(session_factory, lifecycle, settings, metrics)`, with `tick(now=None)`, `schedule(now)`, `reconcile(now)` and `launch(now)`. All four are public, and the API tests call them.
  - `SchedulerThread(scheduler, interval_seconds)`, with `start()` and `stop(timeout=30.0)`.
  - `exclusive_lock(path)`, a context manager that raises `RuntimeError("another scheduler holds …")`.
  - The `scheduler` fixture.

- [ ] **Step 1: Add the fixture**

Add to `tests/conftest.py`:

```python
from docker_jobs_backend.scheduler import Scheduler


@pytest.fixture(name="scheduler")
def scheduler_fixture(session_factory, lifecycle, settings, metrics):
    return Scheduler(session_factory, lifecycle, settings, metrics)
```

- [ ] **Step 2: Write the failing tests**

`tests/test_scheduler.py`:

```python
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
```

- [ ] **Step 3: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_scheduler.py -q`
Expected: `ModuleNotFoundError: No module named 'docker_jobs_backend.scheduler'`, raised from conftest.

- [ ] **Step 4: Write the implementation**

`src/docker_jobs_backend/scheduler.py`:

```python
"""The scheduler: one thread, a tick every TICK_SECONDS

A tick, in order:

1. schedule   for every enabled job, a run per interval that ended since its
              cursor: all of them with catchup (CATCHUP_BATCH at most per
              tick), only the latest without (the others counted as skipped);
2. reconcile  sweep this stack's leftover services, then bring every running
              attempt up to date: succeeded, failed, lost, timed out;
3. launch     start pending attempts whose time has come, oldest interval
              first, within the job's max_active_runs and MAX_RUNNING.

Reconcile comes before launch, so a slot freed this tick is used this tick.
Each step starts from the database, so a failed tick (Docker unreachable) is
just retried by the next. Whatever happens the gauges are refreshed; the
heartbeat only moves when a tick completes -- the "scheduler down" alert
watches it, and docker_up tells whether Docker was the reason.
"""

import contextlib
import datetime
import fcntl
import logging
import threading
from typing import Dict, Iterator, Optional

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload, sessionmaker

from docker_jobs_backend.db import Attempt, AttemptState, Job, Run, RunState, RunType, utcnow
from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.runner import RunnerUnavailable
from docker_jobs_backend.schedule import due_intervals, latest_complete_interval
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics

__all__ = ["Scheduler", "SchedulerThread", "exclusive_lock"]

LOGGER = logging.getLogger(__name__)

# what launch and reconcile read from an attempt: its run, the run's job and attempts
_WITH_RUN = (
    selectinload(Attempt.run).selectinload(Run.job),
    selectinload(Attempt.run).selectinload(Run.attempts),
)


class Scheduler:
    """the three steps of a tick"""

    def __init__(
        self,
        session_factory: sessionmaker,
        lifecycle: Lifecycle,
        settings: Settings,
        metrics: Metrics,
    ):
        self.session_factory = session_factory
        self.lifecycle = lifecycle
        self.settings = settings
        self.metrics = metrics

    def tick(self, now: Optional[datetime.datetime] = None) -> None:
        """schedule, reconcile, launch; RunnerUnavailable propagates"""
        now = now or utcnow()
        reachable = True
        try:
            self.schedule(now)
            self.reconcile(now)
            self.launch(now)
        except RunnerUnavailable:
            reachable = False
            raise
        finally:
            self.metrics.docker_reachable(reachable)
            with self.session_factory() as session:
                self.metrics.refresh(session, now)
        self.metrics.tick(now)

    # --- 1. schedule ------------------------------------------------------------

    def schedule(self, now: datetime.datetime) -> None:
        """create the runs of the intervals that ended"""
        with self.session_factory() as session:
            jobs = session.scalars(
                select(Job).where(Job.enabled.is_(True)).order_by(Job.id)
            ).all()
            for job in jobs:
                self._schedule_job(session, job, now)
            session.commit()

    def _schedule_job(self, session: Session, job: Job, now: datetime.datetime) -> None:
        due = due_intervals(
            job.schedule,
            job.start_date,
            job.end_date,
            job.schedule_cursor,
            now,
            self.settings.catchup_batch,
        )
        if not due:
            return

        if not job.catchup:
            upper = now if job.end_date is None else min(now, job.end_date)
            latest = latest_complete_interval(job.schedule, upper)
            # exact below a batch, a lower bound above
            skipped = len(due) - 1 if due[-1] == latest else len(due)
            if skipped:
                self.metrics.intervals_skipped(job.name, skipped)
            due = [latest]

        existing = set(
            session.scalars(
                select(Run.logical_date).where(
                    Run.job_id == job.id,
                    Run.run_type == RunType.SCHEDULED,
                    Run.logical_date >= due[0].start,
                    Run.logical_date <= due[-1].start,
                )
            )
        )
        for interval in due:
            if interval.start not in existing:
                self.lifecycle.create_run(session, job, interval, RunType.SCHEDULED, now)

        job.schedule_cursor = due[-1].end

    # --- 2. reconcile -----------------------------------------------------------

    def reconcile(self, now: datetime.datetime) -> None:
        """sweep, then update every running attempt from its service"""
        with self.session_factory() as session:
            self.lifecycle.sweep(session)
            running = session.scalars(
                select(Attempt)
                .where(Attempt.state == AttemptState.RUNNING)
                .order_by(Attempt.id)
                .options(*_WITH_RUN)
            ).all()
            session.commit()

            for attempt in running:
                self.lifecycle.reconcile(session, attempt, now)

    # --- 3. launch --------------------------------------------------------------

    def launch(self, now: datetime.datetime) -> None:
        """start pending attempts, oldest interval first, within the limits"""
        with self.session_factory() as session:
            pending = session.scalars(
                select(Attempt)
                .join(Attempt.run)
                .join(Run.job)
                .where(
                    Attempt.state == AttemptState.PENDING,
                    Job.enabled.is_(True),
                    or_(Attempt.not_before.is_(None), Attempt.not_before <= now),
                )
                .order_by(Run.logical_date, Attempt.id)
                .options(*_WITH_RUN)
            ).all()
            running = session.scalar(
                select(func.count()).select_from(Attempt).where(Attempt.state == AttemptState.RUNNING)
            )
            active: Dict[int, int] = dict(
                session.execute(
                    select(Run.job_id, func.count())
                    .where(Run.state.in_([RunState.RUNNING, RunState.RETRYING]))
                    .group_by(Run.job_id)
                ).all()
            )
            session.commit()

            limit = self.settings.max_running
            for attempt in pending:
                if limit and running >= limit:
                    break
                run = attempt.run
                job = run.job
                # a retrying run holds its slot already; a queued one needs one
                holds_slot = run.state == RunState.RETRYING
                if not holds_slot and active.get(job.id, 0) >= job.max_active_runs:
                    continue

                self.lifecycle.launch(session, attempt, now)

                if attempt.state == AttemptState.RUNNING:
                    running += 1
                if not holds_slot and run.state in (RunState.RUNNING, RunState.RETRYING):
                    active[job.id] = active.get(job.id, 0) + 1


class SchedulerThread(threading.Thread):
    """ticks until stopped; a failed tick is logged, never fatal"""

    def __init__(self, scheduler: Scheduler, interval_seconds: float):
        super().__init__(name="scheduler", daemon=True)
        self.scheduler = scheduler
        self.interval_seconds = interval_seconds
        # not _stop: threading.Thread has a method of that name
        self._stopping = threading.Event()

    def run(self) -> None:
        while not self._stopping.is_set():
            try:
                self.scheduler.tick()
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Scheduler tick failed")
            self._stopping.wait(self.interval_seconds)

    def stop(self, timeout: float = 30.0) -> None:
        self._stopping.set()
        self.join(timeout)


@contextlib.contextmanager
def exclusive_lock(path: str) -> Iterator[None]:
    """hold an exclusive flock on path, or fail at once: one scheduler per database"""
    with open(path, "a", encoding="UTF-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"another scheduler holds {path}: run one backend process per database"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
```

- [ ] **Step 5: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_scheduler.py -q`
Expected: `16 passed`.

- [ ] **Step 6: Run the whole suite**

Run: `.venv/bin/pytest -q`
Expected: every test passes.

- [ ] **Step 7: Commit**

```bash
git add src/docker_jobs_backend/scheduler.py tests/conftest.py tests/test_scheduler.py
git commit -m "feat: scheduler ticks with catch-up, limits and outage tolerance" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 9: Application wiring (`main.py`, `deps.py`)

**Files:**
- Modify: `src/docker_jobs_backend/main.py` (replace it entirely)
- Modify: `src/docker_jobs_backend/deps.py` (replace it entirely)
- Modify: `tests/conftest.py` (replace the `make_app` fixture)
- Test: `tests/test_app.py` (add tests)

**Interfaces:**
- Consumes: everything from Tasks 1–8.
- Produces:
  - `create_app(settings=None, session_factory=None, runner=None, metrics=None, start_scheduler=None) -> FastAPI`.
  - `app.state` holds `settings`, `session_factory`, `runner`, `metrics`, `lifecycle` and `scheduler`.
  - `deps.get_session` (one session per request), `deps.get_lifecycle`, `deps.get_runner` and `deps.get_metrics`.
  - `/health` returns `{"status": "ok", "last_tick": iso8601 | null}`.

- [ ] **Step 1: Point `make_app` at the fakes, and write the failing tests**

In `tests/conftest.py`, replace the `make_app` fixture with:

```python
@pytest.fixture(name="make_app")
def make_app_fixture(settings, session_factory, runner, metrics):
    """builds the application when called, on the test's fakes, scheduler thread off"""
    return lambda: create_app(
        settings,
        session_factory=session_factory,
        runner=runner,
        metrics=metrics,
        start_scheduler=False,
    )
```

Add to `tests/test_app.py`. The new imports go at the top of the file:

```python
import time

import pytest
from factories import HOUR, attrs, make_job, points

from docker_jobs_backend.db import utcnow
from docker_jobs_backend.main import create_app
from docker_jobs_backend.schedule import interval_containing
from docker_jobs_backend.scheduler import exclusive_lock


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
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_app.py -q`
Expected: `TypeError: create_app() got an unexpected keyword argument 'session_factory'`.

- [ ] **Step 3: Write the implementation**

`src/docker_jobs_backend/deps.py`:

```python
"""FastAPI dependencies: what create_app() keeps on app.state"""

from typing import Iterator

from fastapi import Request
from sqlalchemy.orm import Session

from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.runner import Runner
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics

__all__ = ["get_app_settings", "get_lifecycle", "get_metrics", "get_runner", "get_session"]


def get_app_settings(request: Request) -> Settings:
    """the settings the application was built with"""
    return request.app.state.settings


def get_session(request: Request) -> Iterator[Session]:
    """a session for the request, closed after it"""
    with request.app.state.session_factory() as session:
        yield session


def get_lifecycle(request: Request) -> Lifecycle:
    return request.app.state.lifecycle


def get_runner(request: Request) -> Runner:
    return request.app.state.runner


def get_metrics(request: Request) -> Metrics:
    return request.app.state.metrics
```

`src/docker_jobs_backend/main.py`:

```python
"""The FastAPI application, and the scheduler thread it runs

    /health            liveness, and the last scheduler tick
    /api/v1/whoami     the caller, as the backend sees them
    /docs              OpenAPI UI

One process per database: `uvicorn --factory docker_jobs_backend.main:create_app`,
never with --workers. The lifespan takes LOCK_FILE before it starts the
scheduler thread, so a second process fails to start instead of running every
job twice.
"""

import contextlib
import logging
from typing import Optional

from fastapi import Depends, FastAPI
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from docker_jobs_backend.auth import User, get_current_user, require_token
from docker_jobs_backend.db import Job, create_session_factory
from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.runner import Runner, SwarmRunner
from docker_jobs_backend.scheduler import Scheduler, SchedulerThread, exclusive_lock
from docker_jobs_backend.settings import Settings, get_settings
from docker_jobs_backend.telemetry import Metrics, create_meter_provider

__all__ = ["API_PREFIX", "create_app"]

API_PREFIX = "/api/v1"


def create_app(
    settings: Optional[Settings] = None,
    session_factory: Optional[sessionmaker] = None,
    runner: Optional[Runner] = None,
    metrics: Optional[Metrics] = None,
    start_scheduler: Optional[bool] = None,
) -> FastAPI:
    """build the application

    Every argument defaults to production: settings from the environment,
    SQLite at DATABASE_URL, Swarm through DOCKER_HOST, OTLP to
    OTEL_EXPORTER_OTLP_ENDPOINT, the scheduler as SCHEDULER_ENABLED says.
    """
    settings = settings or get_settings()
    session_factory = session_factory or create_session_factory(settings.database_url)
    runner = runner or SwarmRunner(settings)
    meter_provider = None
    if metrics is None:
        meter_provider = create_meter_provider(settings)
        metrics = Metrics(meter_provider, settings)
    lifecycle = Lifecycle(runner, metrics, settings)
    scheduler = Scheduler(session_factory, lifecycle, settings, metrics)
    start = settings.scheduler_enabled if start_scheduler is None else start_scheduler

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        # unwound in reverse: the thread stops, the lock goes, metrics flush
        with contextlib.ExitStack() as stack:
            if meter_provider is not None:
                stack.callback(meter_provider.shutdown)
            with session_factory() as session:
                metrics.register_jobs(session.scalars(select(Job.name)).all())
            if start:
                stack.enter_context(exclusive_lock(settings.lock_file))
                thread = SchedulerThread(scheduler, settings.tick_seconds)
                thread.start()
                stack.callback(thread.stop)
            yield

    application = FastAPI(
        title="docker-jobs-backend",
        description="Scheduled jobs on Docker Swarm",
        root_path=settings.root_path,
        lifespan=lifespan,
    )
    application.state.settings = settings
    application.state.session_factory = session_factory
    application.state.runner = runner
    application.state.metrics = metrics
    application.state.lifecycle = lifecycle
    application.state.scheduler = scheduler

    @application.get("/health", tags=["health"])
    def health() -> dict:
        last_tick = metrics.last_tick
        return {"status": "ok", "last_tick": None if last_tick is None else last_tick.isoformat()}

    @application.get(
        f"{API_PREFIX}/whoami", tags=["auth"], dependencies=[Depends(require_token)]
    )
    def whoami(user: User = Depends(get_current_user)) -> User:
        return user

    return application


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
```

- [ ] **Step 4: Run the whole suite**

Run: `.venv/bin/pytest -q`
Expected: every test passes, including the 7 tests in `test_app.py`.

- [ ] **Step 5: Commit**

```bash
git add src/docker_jobs_backend/main.py src/docker_jobs_backend/deps.py tests/conftest.py tests/test_app.py
git commit -m "feat: wire the scheduler thread, metrics and runner into the app" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Jobs API (`schemas.py`, `jobs.py`)

**Files:**
- Create: `src/docker_jobs_backend/schemas.py`, `src/docker_jobs_backend/jobs.py`
- Modify: `src/docker_jobs_backend/main.py` (include the router)
- Test: `tests/test_jobs_api.py`

**Interfaces:**
- Consumes:
  - `deps.*`, `auth.get_current_user`
  - `Lifecycle.create_run`, `Runner.missing_objects`, `Metrics.register_job`
  - `schedule.*`, `templating.*`
- Produces: the HTTP contract the Grafana app plan relies on.
  - `GET /api/v1/jobs` returns `[JobOut]`.
  - `POST /api/v1/jobs` takes `JobCreate` and returns 201 `JobOut`. It answers 409 for a taken name and 422 for an invalid body.
  - `GET /api/v1/jobs/{name}` returns `JobOut`, or 404.
  - `PUT /api/v1/jobs/{name}` takes `JobSpec` (no name) and returns `JobOut`.
  - `DELETE /api/v1/jobs/{name}` returns 204, or 409 while an attempt is running.
  - `POST /api/v1/jobs/{name}/trigger` takes `{logical_date?, note?}` and returns 201 `RunOut` with its attempts. It answers 409 when the job is disabled and 422 when the interval has not ended.
  - `POST /api/v1/preview` takes `{job: JobCreate, logical_date?}` and returns `PreviewOut`.
  - `schemas.py` also provides `RunOut`, `AttemptOut`, `NoteRequest`, `run_out(run, stack, with_attempts=False)`, `job_out(job, now)` and `job_columns(spec)`.

- [ ] **Step 1: Write the failing tests**

`tests/test_jobs_api.py`:

```python
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
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_jobs_api.py -q`
Expected: failures with 404 responses, because no `/api/v1/jobs` route exists yet.

- [ ] **Step 3: Write the schemas**

`src/docker_jobs_backend/schemas.py`:

```python
"""Request and response bodies of the API

What the Grafana app sends and reads. Datetimes are ISO 8601 with a timezone
(2026-10-09T00:00:00Z) both ways: one without is refused, never guessed.
"""

import datetime
import re
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from docker_jobs_backend.db import Attempt, Job, Run
from docker_jobs_backend.lifecycle import loki_selector
from docker_jobs_backend.schedule import next_fire_times, validate_cron
from docker_jobs_backend.templating import TemplateProblem, validate_templates

__all__ = [
    "AttemptOut",
    "ConfigRef",
    "JobCreate",
    "JobOut",
    "JobSpec",
    "MountSpec",
    "NAME_PATTERN",
    "NoteRequest",
    "PreviewOut",
    "PreviewRequest",
    "RunOut",
    "SecretRef",
    "TriggerRequest",
    "aware_utc",
    "job_columns",
    "job_out",
    "run_out",
]

# lowercase, digits, inner dashes, 32 at most: it goes in service names
NAME_PATTERN = r"^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$"
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def aware_utc(value: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
    """the datetime in UTC; ValueError when it has no timezone"""
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("give a timezone, e.g. 2026-10-09T00:00:00Z")
    return value.astimezone(datetime.UTC)


class SecretRef(BaseModel):
    """a swarm secret, at /run/secrets/<target, or source when empty>"""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    target: str = ""
    uid: str = "10001"
    gid: str = "10001"
    mode: int = 0o400


class ConfigRef(BaseModel):
    """a swarm config, at target"""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    target: str = Field(min_length=1)
    uid: str = "10001"
    gid: str = "10001"
    mode: int = 0o444


class MountSpec(BaseModel):
    """a volume, tmpfs, or -- with ALLOW_BIND_MOUNTS -- a host path"""

    model_config = ConfigDict(extra="forbid")

    type: Literal["volume", "bind", "tmpfs"] = "volume"
    source: str = ""
    target: str = Field(min_length=1)
    read_only: bool = False


class JobSpec(BaseModel):
    """a job's definition, as PUT /jobs/{name} takes it"""

    model_config = ConfigDict(extra="forbid")

    description: str = ""
    image: str = Field(min_length=1)
    entrypoint: Optional[List[str]] = None
    command: List[str] = Field(default_factory=list)
    env: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, str] = Field(default_factory=dict)
    schedule: str
    start_date: Optional[datetime.datetime] = None
    end_date: Optional[datetime.datetime] = None
    enabled: bool = True
    catchup: bool = True
    max_active_runs: int = Field(default=1, ge=1, le=16)
    retries: int = Field(default=0, ge=0, le=10)
    retry_delay_seconds: int = Field(default=300, ge=0, le=86_400)
    timeout_seconds: int = Field(default=3600, ge=60, le=7 * 86_400)
    networks: List[str] = Field(default_factory=list)
    constraints: List[str] = Field(default_factory=list)
    secrets: List[SecretRef] = Field(default_factory=list)
    configs: List[ConfigRef] = Field(default_factory=list)
    mounts: List[MountSpec] = Field(default_factory=list)
    cpu_limit: Optional[float] = Field(default=None, gt=0)
    memory_limit_mb: Optional[int] = Field(default=None, gt=0)

    @field_validator("schedule")
    @classmethod
    def _schedule(cls, value: str) -> str:
        return validate_cron(value)

    @field_validator("start_date", "end_date")
    @classmethod
    def _dates(cls, value):
        return aware_utc(value)

    @field_validator("env")
    @classmethod
    def _env_names(cls, value: Dict[str, str]) -> Dict[str, str]:
        bad = sorted(name for name in value if not _ENV_NAME.match(name))
        if bad:
            raise ValueError(f"not environment variable names: {bad}")
        return value

    @model_validator(mode="after")
    def _consistent(self):
        if self.start_date and self.end_date and self.end_date <= self.start_date:
            raise ValueError("end_date must come after start_date")
        try:
            validate_templates(self.entrypoint, self.command, self.env, self.params, "job")
        except TemplateProblem as exc:
            raise ValueError(f"template {exc}") from exc
        return self


class JobCreate(JobSpec):
    """a job, as POST /jobs takes it"""

    name: str = Field(pattern=NAME_PATTERN)


class JobOut(JobCreate):
    """a job, as the API returns it"""

    model_config = ConfigDict(extra="ignore")

    start_date: datetime.datetime
    schedule_cursor: Optional[datetime.datetime] = None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    updated_by: str
    # when its next run will be created: the end of its next interval
    next_fire_at: Optional[datetime.datetime] = None


class AttemptOut(BaseModel):
    """one try of a run"""

    try_number: int
    reason: str
    state: str
    requested_by: str
    note: str
    not_before: Optional[datetime.datetime]
    created_at: datetime.datetime
    launched_at: Optional[datetime.datetime]
    started_at: Optional[datetime.datetime]
    finished_at: Optional[datetime.datetime]
    service_name: Optional[str]
    node_id: Optional[str]
    rendered_entrypoint: Optional[List[str]]
    rendered_command: Optional[List[str]]
    exit_code: Optional[int]
    error: Optional[str]
    # LogQL selector of this try's logs; with launched_at/finished_at, a Loki query
    loki_selector: Optional[str]


class RunOut(BaseModel):
    """one interval of a job, or a manual run of one"""

    id: int
    job: str
    run_type: str
    state: str
    logical_date: datetime.datetime
    data_interval_start: datetime.datetime
    data_interval_end: datetime.datetime
    try_number: int
    max_tries: int
    created_at: datetime.datetime
    started_at: Optional[datetime.datetime]
    finished_at: Optional[datetime.datetime]
    requested_by: str
    note: str
    last_attempt: Optional[AttemptOut]
    # only GET /runs/{id} and the actions fill it
    attempts: Optional[List[AttemptOut]] = None


class TriggerRequest(BaseModel):
    """POST /jobs/{name}/trigger: the interval logical_date falls in, or the latest that ended"""

    model_config = ConfigDict(extra="forbid")

    logical_date: Optional[datetime.datetime] = None
    note: str = ""

    @field_validator("logical_date")
    @classmethod
    def _date(cls, value):
        return aware_utc(value)


class NoteRequest(BaseModel):
    """POST /runs/{id}/rerun and /cancel: why"""

    model_config = ConfigDict(extra="forbid")

    note: str = ""


class PreviewRequest(BaseModel):
    """POST /preview: a definition to render, for an interval"""

    model_config = ConfigDict(extra="forbid")

    job: JobCreate
    logical_date: Optional[datetime.datetime] = None

    @field_validator("logical_date")
    @classmethod
    def _date(cls, value):
        return aware_utc(value)


class PreviewOut(BaseModel):
    """what an attempt of that definition would run"""

    data_interval_start: datetime.datetime
    data_interval_end: datetime.datetime
    entrypoint: Optional[List[str]]
    command: List[str]
    env: Dict[str, str]
    next_fire_times: List[datetime.datetime]
    # missing secrets, configs, networks: what would fail the launch
    problems: List[str]


def job_columns(spec: JobSpec) -> dict:
    """the Job columns a spec sets; the caller decides about start_date"""
    return spec.model_dump(exclude={"name", "start_date"})


def _next_fire(job: Job, now: datetime.datetime) -> Optional[datetime.datetime]:
    if not job.enabled:
        return None
    [fire] = next_fire_times(job.schedule, max(now, job.start_date), 1)
    if job.end_date is not None and fire > job.end_date:
        return None
    return fire


def job_out(job: Job, now: datetime.datetime) -> JobOut:
    values = {column.key: getattr(job, column.key) for column in Job.__table__.columns}
    return JobOut(**values, next_fire_at=_next_fire(job, now))


_ATTEMPT_COLUMNS = [name for name in AttemptOut.model_fields if name != "loki_selector"]


def _attempt_out(attempt: Attempt, stack: str) -> AttemptOut:
    return AttemptOut(
        **{name: getattr(attempt, name) for name in _ATTEMPT_COLUMNS},
        loki_selector=loki_selector(stack, attempt.service_name) if attempt.service_name else None,
    )


def run_out(run: Run, stack: str, with_attempts: bool = False) -> RunOut:
    attempts = [_attempt_out(attempt, stack) for attempt in run.attempts]
    return RunOut(
        id=run.id,
        job=run.job.name,
        run_type=run.run_type,
        state=run.state,
        logical_date=run.logical_date,
        data_interval_start=run.logical_date,
        data_interval_end=run.data_interval_end,
        try_number=run.try_number,
        max_tries=run.max_tries,
        created_at=run.created_at,
        started_at=run.started_at,
        finished_at=run.finished_at,
        requested_by=run.requested_by,
        note=run.note,
        last_attempt=attempts[-1] if attempts else None,
        attempts=attempts if with_attempts else None,
    )
```

- [ ] **Step 4: Write the router and include it**

`src/docker_jobs_backend/jobs.py`:

```python
"""Jobs: define, change, delete, run now, preview

    GET    /jobs                    every job, by name
    POST   /jobs                    create; 409 if the name is taken
    GET    /jobs/{name}
    PUT    /jobs/{name}             replace the definition; the name stays
    DELETE /jobs/{name}             with its runs; 409 while an attempt runs
    POST   /jobs/{name}/trigger     a manual run: of {logical_date}'s interval,
                                    or of the latest that ended
    POST   /preview                 render a definition, check its swarm objects
"""

import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from docker_jobs_backend.auth import User, get_current_user
from docker_jobs_backend.db import Attempt, AttemptState, Job, Run, RunType, utcnow
from docker_jobs_backend.deps import (
    get_app_settings,
    get_lifecycle,
    get_metrics,
    get_runner,
    get_session,
)
from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.runner import Runner, RunnerUnavailable
from docker_jobs_backend.schedule import (
    Interval,
    interval_containing,
    latest_complete_interval,
    next_fire_times,
)
from docker_jobs_backend.schemas import (
    JobCreate,
    JobOut,
    JobSpec,
    PreviewOut,
    PreviewRequest,
    RunOut,
    TriggerRequest,
    job_columns,
    job_out,
    run_out,
)
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics
from docker_jobs_backend.templating import TemplateProblem, build_context, render

__all__ = ["router"]

router = APIRouter(tags=["jobs"])


def find_job(session: Session, name: str) -> Job:
    job = session.scalar(select(Job).where(Job.name == name))
    if job is None:
        raise HTTPException(status_code=404, detail=f"no job {name!r}")
    return job


def check_mounts(spec: JobSpec, settings: Settings) -> None:
    """a bind mount is root on the node: refused unless ALLOW_BIND_MOUNTS"""
    if not settings.allow_bind_mounts and any(mount.type == "bind" for mount in spec.mounts):
        raise HTTPException(
            status_code=422, detail="bind mounts are disabled (ALLOW_BIND_MOUNTS): use a volume"
        )


def interval_for(
    schedule: str, logical_date: Optional[datetime.datetime], now: datetime.datetime
) -> Interval:
    """the interval logical_date falls in, or the latest that ended"""
    if logical_date is None:
        return latest_complete_interval(schedule, now)
    return interval_containing(schedule, logical_date)


@router.get("/jobs", response_model=List[JobOut])
def list_jobs(session: Session = Depends(get_session)) -> List[JobOut]:
    now = utcnow()
    return [job_out(job, now) for job in session.scalars(select(Job).order_by(Job.name))]


@router.post("/jobs", status_code=201, response_model=JobOut)
def create_job(
    body: JobCreate,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    metrics: Metrics = Depends(get_metrics),
    settings: Settings = Depends(get_app_settings),
) -> JobOut:
    check_mounts(body, settings)
    now = utcnow()
    job = Job(
        name=body.name,
        # by default the first interval is the current one: it runs at its end
        start_date=body.start_date or interval_containing(body.schedule, now).start,
        schedule_cursor=None,
        created_at=now,
        updated_at=now,
        updated_by=user.login,
        **job_columns(body),
    )
    session.add(job)
    try:
        session.commit()
    except IntegrityError as exc:
        raise HTTPException(status_code=409, detail=f"job {body.name!r} exists") from exc
    metrics.register_job(job.name)
    return job_out(job, now)


@router.get("/jobs/{name}", response_model=JobOut)
def get_job(name: str, session: Session = Depends(get_session)) -> JobOut:
    return job_out(find_job(session, name), utcnow())


@router.put("/jobs/{name}", response_model=JobOut)
def update_job(
    name: str,
    body: JobSpec,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    settings: Settings = Depends(get_app_settings),
) -> JobOut:
    check_mounts(body, settings)
    job = find_job(session, name)
    now = utcnow()
    for key, value in job_columns(body).items():
        setattr(job, key, value)
    if body.start_date is not None:
        job.start_date = body.start_date
    job.updated_at = now
    job.updated_by = user.login
    session.commit()
    return job_out(job, now)


@router.delete("/jobs/{name}", status_code=204)
def delete_job(name: str, session: Session = Depends(get_session)) -> Response:
    job = find_job(session, name)
    running = session.scalar(
        select(func.count())
        .select_from(Attempt)
        .join(Run)
        .where(Run.job_id == job.id, Attempt.state == AttemptState.RUNNING)
    )
    if running:
        raise HTTPException(
            status_code=409,
            detail=f"job {name!r} has {running} attempt(s) running: cancel them, or wait",
        )
    session.delete(job)
    session.commit()
    return Response(status_code=204)


@router.post("/jobs/{name}/trigger", status_code=201, response_model=RunOut)
def trigger_job(
    name: str,
    body: Optional[TriggerRequest] = None,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    lifecycle: Lifecycle = Depends(get_lifecycle),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    body = body or TriggerRequest()
    job = find_job(session, name)
    if not job.enabled:
        raise HTTPException(status_code=409, detail=f"job {name!r} is disabled: enable it to run it")

    now = utcnow()
    interval = interval_for(job.schedule, body.logical_date, now)
    if interval.end > now:
        raise HTTPException(
            status_code=422,
            detail=f"the interval {interval.start.isoformat()} - {interval.end.isoformat()} has not ended yet",
        )

    run = lifecycle.create_run(
        session, job, interval, RunType.MANUAL, now, requested_by=user.login, note=body.note
    )
    session.commit()
    return run_out(run, settings.run_stack, with_attempts=True)


@router.post("/preview", response_model=PreviewOut)
def preview(
    body: PreviewRequest,
    runner: Runner = Depends(get_runner),
    settings: Settings = Depends(get_app_settings),
) -> PreviewOut:
    job = body.job
    check_mounts(job, settings)
    now = utcnow()
    interval = interval_for(job.schedule, body.logical_date, now)
    try:
        rendered = render(
            job.entrypoint, job.command, job.env, build_context(interval, 0, 1, job.name, job.params)
        )
    except TemplateProblem as exc:
        raise HTTPException(status_code=422, detail=f"template {exc}") from exc

    try:
        problems = runner.missing_objects(
            [secret.source for secret in job.secrets],
            [config.source for config in job.configs],
            job.networks,
        )
    except RunnerUnavailable as exc:
        problems = [f"swarm objects not checked: {exc}"]

    first = job.start_date if job.start_date and job.start_date > now else now
    return PreviewOut(
        data_interval_start=interval.start,
        data_interval_end=interval.end,
        entrypoint=rendered.entrypoint,
        command=rendered.command,
        env=rendered.env,
        next_fire_times=next_fire_times(job.schedule, first, 5),
        problems=problems,
    )
```

In `src/docker_jobs_backend/main.py`, add `jobs` to the imports (`from docker_jobs_backend import jobs`). Add these lines just before `return application`:

```python
    application.include_router(
        jobs.router, prefix=API_PREFIX, dependencies=[Depends(require_token)]
    )
```

Also add `/api/v1/jobs...` and `/api/v1/preview` to the route list in main's module docstring.

- [ ] **Step 5: Run the tests to see them pass**

Run: `.venv/bin/pytest tests/test_jobs_api.py -q`
Expected: `31 passed`. Then run `.venv/bin/pytest -q`: everything passes.

- [ ] **Step 6: Commit**

```bash
git add src/docker_jobs_backend/schemas.py src/docker_jobs_backend/jobs.py src/docker_jobs_backend/main.py tests/test_jobs_api.py
git commit -m "feat: jobs API with trigger and preview" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Runs API (`runs.py`)

**Files:**
- Create: `src/docker_jobs_backend/runs.py`
- Modify: `src/docker_jobs_backend/main.py` (include the router)
- Test: `tests/test_runs_api.py`

**Interfaces:**
- Consumes:
  - `schemas.RunOut`, `NoteRequest`, `run_out`
  - `Lifecycle.rerun` and `cancel`, and `Conflict`
  - `deps.*` and `auth.get_current_user`
- Produces:
  - `GET /api/v1/runs?job=&state=&offset=0&size=50` returns `[RunOut]`, newest `logical_date` first. `attempts` is null and `last_attempt` is filled. An unknown state is a 422, and `size` is at most 500.
  - `GET /api/v1/runs/{id}` returns `RunOut` with `attempts`, or 404.
  - `POST /api/v1/runs/{id}/rerun` takes `{note?}` and returns `RunOut`. It answers 409 unless the run is success, failed or cancelled, and 409 when the job is disabled.
  - `POST /api/v1/runs/{id}/cancel` takes `{note?}` and returns `RunOut`. It answers 409 once the run has ended.

- [ ] **Step 1: Write the failing tests**

`tests/test_runs_api.py`:

```python
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
    assert client.get("/api/v1/runs/999").status_code == 404


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
```

- [ ] **Step 2: Run them to see them fail**

Run: `.venv/bin/pytest tests/test_runs_api.py -q`
Expected: failures with 404 responses on `/api/v1/runs`.

- [ ] **Step 3: Write the router and include it**

`src/docker_jobs_backend/runs.py`:

```python
"""Runs: browse, inspect, rerun, cancel

    GET  /runs?job=&state=&offset=&size=   newest interval first
    GET  /runs/{id}                         with every attempt
    POST /runs/{id}/rerun                   an ended run, again; 409 otherwise
    POST /runs/{id}/cancel                  a run not ended yet; 409 otherwise
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from docker_jobs_backend.auth import User, get_current_user
from docker_jobs_backend.db import Job, Run, RunState, utcnow
from docker_jobs_backend.deps import get_app_settings, get_lifecycle, get_session
from docker_jobs_backend.lifecycle import Conflict, Lifecycle
from docker_jobs_backend.schemas import NoteRequest, RunOut, run_out
from docker_jobs_backend.settings import Settings

__all__ = ["router"]

router = APIRouter(tags=["runs"])

_WITH_ATTEMPTS = (selectinload(Run.job), selectinload(Run.attempts))


def find_run(session: Session, run_id: int) -> Run:
    run = session.get(Run, run_id, options=list(_WITH_ATTEMPTS))
    if run is None:
        raise HTTPException(status_code=404, detail=f"no run {run_id}")
    return run


@router.get("/runs", response_model=List[RunOut])
def list_runs(
    job: Optional[str] = None,
    state: Optional[RunState] = None,
    offset: int = Query(default=0, ge=0),
    size: int = Query(default=50, ge=1, le=500),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_app_settings),
) -> List[RunOut]:
    query = (
        select(Run)
        .join(Run.job)
        .options(*_WITH_ATTEMPTS)
        .order_by(Run.logical_date.desc(), Run.id.desc())
    )
    if job is not None:
        query = query.where(Job.name == job)
    if state is not None:
        query = query.where(Run.state == state)
    return [run_out(run, settings.run_stack) for run in session.scalars(query.offset(offset).limit(size))]


@router.get("/runs/{run_id}", response_model=RunOut)
def get_run(
    run_id: int,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    return run_out(find_run(session, run_id), settings.run_stack, with_attempts=True)


@router.post("/runs/{run_id}/rerun", response_model=RunOut)
def rerun_run(
    run_id: int,
    body: Optional[NoteRequest] = None,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    lifecycle: Lifecycle = Depends(get_lifecycle),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    body = body or NoteRequest()
    run = find_run(session, run_id)
    try:
        lifecycle.rerun(run, utcnow(), requested_by=user.login, note=body.note)
    except Conflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    session.commit()
    return run_out(run, settings.run_stack, with_attempts=True)


@router.post("/runs/{run_id}/cancel", response_model=RunOut)
def cancel_run(
    run_id: int,
    body: Optional[NoteRequest] = None,
    session: Session = Depends(get_session),
    user: User = Depends(get_current_user),
    lifecycle: Lifecycle = Depends(get_lifecycle),
    settings: Settings = Depends(get_app_settings),
) -> RunOut:
    body = body or NoteRequest()
    run = find_run(session, run_id)
    try:
        lifecycle.cancel(session, run, utcnow(), requested_by=user.login, note=body.note)
    except Conflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return run_out(run, settings.run_stack, with_attempts=True)
```

In `src/docker_jobs_backend/main.py`, import `runs` beside `jobs` (`from docker_jobs_backend import jobs, runs`). Add this after the jobs router:

```python
    application.include_router(
        runs.router, prefix=API_PREFIX, dependencies=[Depends(require_token)]
    )
```

Also add `/api/v1/runs...` to the route list in main's module docstring.

- [ ] **Step 4: Run the tests to see them pass**

Run: `.venv/bin/pytest -q`
Expected: every test passes, including the 9 tests in `test_runs_api.py`.

- [ ] **Step 5: Commit**

```bash
git add src/docker_jobs_backend/runs.py src/docker_jobs_backend/main.py tests/test_runs_api.py
git commit -m "feat: runs API with rerun and cancel" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: Container image and CI

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `sonar-project.properties`, `.github/workflows/docker-jobs-backend.yml`

**Interfaces:**
- Consumes: `tox` (default env, `build`, `sbom`) from Task 1, and `create_app` from Task 9.
- Produces:
  - The image `docker-jobs-backend`. It serves on port 8000 as uid 10001, with the volume `/data`. `CMD` runs `uvicorn --factory docker_jobs_backend.main:create_app`.
  - The CI calls `python-ci.yml`, `sbom.yml` and `docker-build.yml` from `workflow-catalog@develop`.

- [ ] **Step 1: Write the files**

`Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1
#
# docker-jobs-backend: installs the wheel `tox -e build` produces locally, or
# the python-ci workflow hands over as the `dist` artifact. The backend is an
# application: its wheel is never published, the image is the release.
#
# Build locally (drop the netrc secret if you have no ~/.netrc):
#   tox -e build
#   DOCKER_CONTEXT=default docker build -t docker-jobs-backend:dev \
#     --secret id=pip_index_url,env=PIP_INDEX_URL \
#     --secret id=netrc,src=$HOME/.netrc .
#
ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS runtime
LABEL Author="Serco - Space unit"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies come from the Nexus PyPI index when the pip_index_url secret is
# given (CI: mount-pip-index), from pip's default index otherwise.
COPY dist/ /tmp/dist/
RUN --mount=type=secret,id=pip_index_url \
    --mount=type=secret,id=netrc,target=/root/.netrc \
    set -eu; \
    if [ -s /run/secrets/pip_index_url ]; then \
        index_url="$(tr -d '\r\n' < /run/secrets/pip_index_url)"; \
        hostpart="${index_url#*://}"; hostpart="${hostpart%%/*}"; \
        index_host="${hostpart##*@}"; index_host="${index_host%%:*}"; \
        echo "Nexus index host: $index_host"; \
        PIP_INDEX_URL="$index_url"; PIP_TRUSTED_HOST="$index_host"; \
        export PIP_INDEX_URL PIP_TRUSTED_HOST; \
    fi; \
    pip install --no-cache-dir --no-input /tmp/dist/*.whl; \
    rm -rf /tmp/dist

# uid/gid 10001 like every house image. /data holds the SQLite file and the
# scheduler lock; owned by appuser, so a fresh named volume starts writable.
RUN groupadd -g 10001 appuser \
    && useradd -u 10001 -g appuser -M -s /usr/sbin/nologin appuser \
    && mkdir /data && chown appuser:appuser /data

USER appuser
VOLUME ["/data"]

ENV PORT=8000

EXPOSE 8000

# python rather than curl: the slim image ships no curl
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD python -c 'import os, sys, urllib.request; sys.exit(0 if urllib.request.urlopen("http://127.0.0.1:" + os.environ["PORT"] + "/health", timeout=5).status == 200 else 1)'

# ONE process, never --workers: the scheduler thread must be alone (LOCK_FILE)
CMD ["sh", "-c", "exec uvicorn --factory docker_jobs_backend.main:create_app --host 0.0.0.0 --port ${PORT} --proxy-headers --forwarded-allow-ips='*'"]
```

`.dockerignore`:

```gitignore
**
!dist/
```

`sonar-project.properties`:

```properties
sonar.projectName=docker-jobs-backend
sonar.projectKey=docker-jobs-backend

sonar.scm.provider=git

sonar.python.version=3.11

# Reports produced by `tox` (see tox.ini [testenv]) at the repo root.
sonar.python.coverage.reportPaths=coverage.xml
sonar.python.xunit.reportPaths=pytest-report.xml

sonar.sources=src
sonar.tests=tests
```

`.github/workflows/docker-jobs-backend.yml`:

```yaml
name: docker-jobs-backend CI/CD

# Thin caller of the workflow-catalog building blocks, like
# download-manager-backend.yml:
#   python-ci.yml    — tox (pytest + reports) + SonarQube -> wheel/sdist (`dist`)
#   sbom.yml         — package CycloneDX SBOM -> Dependency-Track
#   docker-build.yml — image from the `dist` artifact -> Nexus Docker registry
# The backend is an application: the wheel is never published (publish: false).
#
#   push to develop   -> ci + package SBOM (dev) + dev image (branch + sha tags)
#   push tag x.y.z    -> ci + package SBOM (prod) + release image (semver tags)
#   workflow_dispatch -> manual run

on:
  push:
    branches:
      - "develop"
    tags:
      - "[0-9]+.[0-9]+.[0-9]+"
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  ci:
    permissions:
      contents: read
    uses: CSC-Operations-Coordination-Service/workflow-catalog/.github/workflows/python-ci.yml@develop
    with:
      project-dir: .
      python-version: "3.11"
      publish: false
    secrets: inherit

  sbom:
    needs: ci
    uses: CSC-Operations-Coordination-Service/workflow-catalog/.github/workflows/sbom.yml@develop
    with:
      project-dir: .
      project-name: docker-jobs-backend
      project-version: ${{ needs.ci.outputs.version }}
      python-version: "3.11"
      context: ${{ startsWith(github.ref, 'refs/tags/') && 'prod' || 'dev' }}
    secrets: inherit

  docker:
    needs: ci
    if: github.ref == 'refs/heads/develop' || startsWith(github.ref, 'refs/tags/')
    permissions:
      contents: read
      packages: write
    uses: CSC-Operations-Coordination-Service/workflow-catalog/.github/workflows/docker-build.yml@develop
    with:
      image-name: docker-jobs-backend
      context: .
      dockerfile: Dockerfile
      artifact-name: dist
      artifact-path: dist/
      mount-pip-index: true
      release: ${{ startsWith(github.ref, 'refs/tags/') }}
      environment: ${{ startsWith(github.ref, 'refs/tags/') && 'prod' || 'dev' }}
      # the Nexus Docker connector is plain HTTP
      insecure-registry: true
      generate-sbom: ${{ startsWith(github.ref, 'refs/tags/') }}
      sbom-project-name: docker-jobs-backend-image
      sbom-project-version: ${{ needs.ci.outputs.version }}
    secrets: inherit
```

- [ ] **Step 2: Run tox the way CI does**

Run: `.venv/bin/pip install tox && .venv/bin/tox`
Expected: the tests pass, and `coverage.xml` and `pytest-report.xml` exist at the repository root.

- [ ] **Step 3: Build the wheel and the image**

```bash
.venv/bin/tox -e build
ls dist/   # docker_jobs_backend-<version>-py3-none-any.whl and the sdist
DOCKER_CONTEXT=default docker build -t docker-jobs-backend:dev \
  --secret id=pip_index_url,env=PIP_INDEX_URL .
```

Expected: the build ends with `naming to docker.io/library/docker-jobs-backend:dev`. If `PIP_INDEX_URL` is not set in your shell, drop the `--secret` flag, and pip uses your default index.

- [ ] **Step 4: Smoke-test the container**

```bash
DOCKER_CONTEXT=default docker run -d --rm --name djb-smoke -p 18000:8000 \
  -e SCHEDULER_ENABLED=false -e OTEL_SDK_DISABLED=true docker-jobs-backend:dev
sleep 3
curl -fsS localhost:18000/health          # {"status":"ok","last_tick":null}
DOCKER_CONTEXT=default docker exec djb-smoke id -u   # 10001
DOCKER_CONTEXT=default docker stop djb-smoke
```

Expected: the outputs shown in the comments. The API starts without Docker access, because the Swarm client is only created on first use.

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore sonar-project.properties .github
git commit -m "build: image and CI workflow" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: Swarm stack, alert rules, README, end-to-end check

**Files:**
- Create: `deploy/docker-compose.yml`, `deploy/stack.yml`, `deploy/.env.example`, `deploy/deploy.sh`, `deploy/README.md`, `deploy/secrets/.gitkeep`, `deploy/grafana/docker-jobs-alerts.yaml`
- Modify: `README.md` (write it out in full)
- Test: `tests/test_alert_rules.py`

**Interfaces:**
- Consumes: the image from Task 12, and the metric names from Task 6 (`telemetry.py` docstring).
- Produces:
  - The `docker-jobs` stack. Grafana reaches it at `http://docker-jobs_backend:8000` over `MONITORING_NETWORK`, which is the value for the app plan's `backendUrl`.
  - The API token is the swarm secret `${DJ_API_TOKEN_SECRET_NAME}`. The app plan puts the same value in its `secureJsonData.apiToken`.
  - The Grafana alert rules.

- [ ] **Step 1: Write the failing alert-rule test**

`tests/test_alert_rules.py`:

```python
"""The alert rules query metrics the backend exports, under Mimir's names"""

import pathlib
import re

import yaml
from factories import HOUR, T0, make_job
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import Histogram, InMemoryMetricReader, Sum

from docker_jobs_backend.db import AttemptState, Reason, RunState, RunType
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics

RULES = pathlib.Path(__file__).parent.parent / "deploy" / "grafana" / "docker-jobs-alerts.yaml"


def rules():
    document = yaml.safe_load(RULES.read_text(encoding="UTF-8"))
    return [rule for group in document["groups"] for rule in group["rules"]]


def exported_names(session):
    """every Prometheus name of the backend's metrics, translated the way Mimir does"""
    reader = InMemoryMetricReader()
    metrics = Metrics(MeterProvider(metric_readers=[reader]), Settings())
    make_job(session)
    metrics.register_job("collect")
    metrics.run_created("collect", RunType.SCHEDULED)
    metrics.attempt_started("collect", Reason.SCHEDULED)
    metrics.attempt_finished("collect", AttemptState.SUCCESS, 1.0)
    metrics.run_finished("collect", RunState.SUCCESS)
    metrics.intervals_skipped("collect", 1)
    metrics.refresh(session, T0 + HOUR)
    metrics.docker_reachable(True)
    metrics.tick(T0 + HOUR)

    names = set()
    for resource_metrics in reader.get_metrics_data().resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                base = metric.name.replace(".", "_") + ("_seconds" if metric.unit == "s" else "")
                if isinstance(metric.data, Sum) and metric.data.is_monotonic:
                    names.add(f"{base}_total")
                elif isinstance(metric.data, Histogram):
                    names.update({f"{base}_bucket", f"{base}_sum", f"{base}_count"})
                else:
                    names.add(base)
    return names


def test_every_metric_an_alert_queries_is_exported(session):
    expressions = [
        query["model"]["expr"] for rule in rules() for query in rule["data"] if "expr" in query["model"]
    ]
    used = {name for expression in expressions for name in re.findall(r"docker_jobs_[a-z_]+", expression)}

    assert used == {
        "docker_jobs_runs_finished_total",
        "docker_jobs_job_gaps",
        "docker_jobs_scheduler_heartbeat_seconds",
    }
    assert used <= exported_names(session)


def test_the_rules_route_as_docker_jobs():
    assert {rule["title"] for rule in rules()} == {
        "Docker job failed",
        "Docker job intervals missing",
        "Docker jobs scheduler down",
    }
    assert all(rule["labels"]["component"] == "docker-jobs" for rule in rules())


def test_a_silent_scheduler_alerts():
    [down] = [rule for rule in rules() if rule["uid"] == "docker-jobs-scheduler-down"]

    assert down["noDataState"] == "Alerting"
```

Run: `.venv/bin/pytest tests/test_alert_rules.py -q`
Expected: `FileNotFoundError` on `deploy/grafana/docker-jobs-alerts.yaml`.

- [ ] **Step 2: Write the alert rules**

`deploy/grafana/docker-jobs-alerts.yaml`:

```yaml
# Grafana alert rules of the docker-jobs backend, as a provisioning file. Mount
# it in the Grafana that holds the Mimir datasource, at
# /etc/grafana/provisioning/alerting/docker-jobs-alerts.yaml, and give that
# Grafana MIMIR_DATASOURCE_UID in its environment (Grafana expands ${...} in
# provisioning files). Every rule carries component=docker-jobs: Alertmanager
# needs a route for it (csc-ocs-ansible stacks/monitoring/alertmanager), or the
# alerts go to blackhole. Metric names: src/docker_jobs_backend/telemetry.py.
apiVersion: 1
groups:
  - orgId: 1
    name: docker-jobs
    folder: Docker jobs
    interval: 1m
    rules:
      - uid: docker-jobs-run-failed
        title: Docker job failed
        condition: B
        data:
          - refId: A
            relativeTimeRange: {from: 900, to: 0}
            datasourceUid: ${MIMIR_DATASOURCE_UID}
            model:
              refId: A
              instant: true
              expr: sum by (docker_job) (increase(docker_jobs_runs_finished_total{state="failed"}[15m]))
          - refId: B
            datasourceUid: __expr__
            model:
              refId: B
              type: threshold
              expression: A
              conditions:
                - evaluator: {type: gt, params: [0]}
        noDataState: OK
        execErrState: Error
        for: 0s
        labels: {component: docker-jobs, origin: metric}
        annotations:
          summary: A docker job run failed after its retries
          description: >-
            Fix the cause, then rerun it from the Docker jobs app; each attempt
            of the run links to its logs in Loki.
      - uid: docker-jobs-gaps
        title: Docker job intervals missing
        condition: B
        data:
          - refId: A
            relativeTimeRange: {from: 600, to: 0}
            datasourceUid: ${MIMIR_DATASOURCE_UID}
            model:
              refId: A
              instant: true
              expr: max by (docker_job) (docker_jobs_job_gaps)
          - refId: B
            datasourceUid: __expr__
            model:
              refId: B
              type: threshold
              expression: A
              conditions:
                - evaluator: {type: gt, params: [0]}
        noDataState: OK
        execErrState: Error
        for: 15m
        labels: {component: docker-jobs, origin: metric}
        annotations:
          summary: Intervals of a docker job ended without data
          description: >-
            Runs failed or were cancelled, never started, or were never
            scheduled (the scheduler is behind). Rerun or trigger them from the
            Docker jobs app.
      - uid: docker-jobs-scheduler-down
        title: Docker jobs scheduler down
        condition: B
        data:
          - refId: A
            relativeTimeRange: {from: 600, to: 0}
            datasourceUid: ${MIMIR_DATASOURCE_UID}
            model:
              refId: A
              instant: true
              expr: time() - max(docker_jobs_scheduler_heartbeat_seconds)
          - refId: B
            datasourceUid: __expr__
            model:
              refId: B
              type: threshold
              expression: A
              conditions:
                - evaluator: {type: gt, params: [300]}
        # no heartbeat at all is the scheduler down too
        noDataState: Alerting
        execErrState: Alerting
        for: 5m
        labels: {component: docker-jobs, origin: metric}
        annotations:
          summary: The docker-jobs scheduler has not completed a tick for 5 minutes
          description: >-
            No job is being scheduled or launched. docker_jobs_docker_up says
            whether Docker (the socket-proxy) is the reason.
```

Run: `.venv/bin/pytest tests/test_alert_rules.py -q`
Expected: `3 passed`.

- [ ] **Step 3: Write the stack**

`deploy/docker-compose.yml`:

```yaml
# ===========================================================================
# docker-jobs stack -- runs scheduled jobs as swarm services
#
# PLAIN compose in the csc-ocs-ansible style: no Jinja, everything variable in
# ./.env, the secret an external object with a versioned name. Deploy by hand
# with ./deploy.sh, or copy this folder to
# csc-ocs-ansible/stacks/platform/docker-jobs and let the docker_swarm_stack
# role do the same from stack.yml.
#
#   backend        docker-jobs-backend: the Grafana app's API, and the
#                  scheduler. ONE replica, on the node labelled
#                  docker-jobs=true: its SQLite file is on that node's volume.
#                  Not routed by Traefik: Grafana calls it, with the token.
#   socket-proxy   the only holder of the docker socket. Reachable from the
#                  backend only (non-attachable overlay).
#
# The jobs are services of the pseudo-stack ${DJ_RUN_STACK}
# (djobs_<job>_<run>_<try>), created and removed by the backend. They are not
# in this file, and `docker stack rm ${DJ_RUN_STACK}` would kill them.
# ===========================================================================

x-logging-loki: &logging-loki
  driver: loki
  options:
    loki-url: "http://127.0.0.1:3100/loki/api/v1/push"
    loki-retries: "5"
    loki-batch-size: "400"
    mode: "non-blocking"

services:
  backend:
    image: ${DJ_IMAGE_REGISTRY}/docker-jobs-backend:${DJ_BACKEND_VERSION}
    environment:
      DATABASE_URL: sqlite:////data/docker-jobs.db
      LOCK_FILE: /data/scheduler.lock
      DOCKER_HOST: tcp://socket-proxy:2375
      RUN_STACK: ${DJ_RUN_STACK}
      MAX_RUNNING: ${DJ_MAX_RUNNING}
      API_TOKEN_FILE: /run/secrets/dj_api_token
      USER_HEADER: X-Grafana-User
      OTEL_EXPORTER_OTLP_ENDPOINT: ${OTEL_EXPORTER_OTLP_ENDPOINT}
      OTEL_EXPORTER_OTLP_PROTOCOL: http/protobuf
      OTEL_METRIC_EXPORT_INTERVAL: "15000"
      OTEL_SERVICE_NAME: docker-jobs-backend
    secrets:
      - source: dj_api_token
        target: dj_api_token
        uid: "10001"
        gid: "10001"
        mode: 0400
    volumes:
      - dj-data:/data
    read_only: true
    tmpfs: [/tmp]
    # internal: the socket-proxy. monitoring: Alloy (OTLP), and the Grafana
    # that runs the app.
    networks: [internal, monitoring]
    logging: *logging-loki
    deploy:
      replicas: 1
      update_config:
        parallelism: 1
        delay: 5s
        # never two schedulers: the old one stops before the new one starts
        order: stop-first
      restart_policy:
        condition: any
      placement:
        constraints:
          # the volume with the database is on this node
          - node.labels.docker-jobs == true

  socket-proxy:
    image: ${SOCKET_PROXY_IMAGE}
    environment:
      SERVICES: "1"
      TASKS: "1"
      SECRETS: "1"
      CONFIGS: "1"
      NETWORKS: "1"
      # POST is global in socket-proxy: it lets the backend create and remove
      # services -- and, with the sections above, secrets, configs and
      # networks. Only the backend can reach this proxy.
      POST: "1"
    volumes:
      - type: bind
        source: /var/run/docker.sock
        target: /var/run/docker.sock
        read_only: true
    networks: [internal]
    logging: *logging-loki
    deploy:
      replicas: 1
      restart_policy:
        condition: any
        delay: 5s
      resources:
        limits:
          memory: 64M
        reservations:
          memory: 16M
      placement:
        constraints:
          # /services and /tasks only answer on a manager
          - node.role == manager

secrets:
  dj_api_token:
    external: true
    name: ${DJ_API_TOKEN_SECRET_NAME}

volumes:
  dj-data:

networks:
  # stack-owned, becomes docker-jobs_internal; not attachable
  internal:
    driver: overlay
    attachable: false
  # Alloy (alias alloy, OTLP/HTTP on 4318) and Grafana: ocs-monitoring in prod
  monitoring:
    external: true
    name: ${MONITORING_NETWORK}
```

`deploy/stack.yml`:

```yaml
---
# Ansible-only manifest, in the format of csc-ocs-ansible's docker_swarm_stack
# role: copy this folder to csc-ocs-ansible/stacks/platform/docker-jobs, vault
# the token as secrets/api-token.vault, and add
# playbooks/deploy_platform/docker-jobs.<env>.yml whose stack_env fills the
# variables of .env.example. Not read by deploy.sh.
stack_name: docker-jobs
stack_src: "{{ playbook_dir }}/../../stacks/platform/docker-jobs"

stack_external_networks:
  - "{{ monitoring_network }}"

stack_secrets:
  - name: docker_jobs_api_token
    src: secrets/api-token.vault
    env_var: DJ_API_TOKEN_SECRET_NAME
```

`deploy/.env.example`:

```bash
# docker-jobs stack settings. Copy to .env (gitignored) and fill in.
# `docker stack deploy` does NOT read .env: deploy.sh sources it first.

# --- Images -------------------------------------------------------------------
DJ_IMAGE_REGISTRY=127.0.0.1:8085
DJ_BACKEND_VERSION=0.1.0
SOCKET_PROXY_IMAGE=tecnativa/docker-socket-proxy:v0.5.0

# --- Jobs ---------------------------------------------------------------------
# Prefix and pseudo-stack of every job service. Another backend on the same
# swarm (a dev one) needs another value.
DJ_RUN_STACK=djobs
# Attempts running at once, all jobs together.
DJ_MAX_RUNNING=8

# --- Networks, telemetry ------------------------------------------------------
MONITORING_NETWORK=ocs-monitoring
OTEL_EXPORTER_OTLP_ENDPOINT=http://alloy:4318

# --- Swarm objects ------------------------------------------------------------
# Written by deploy.sh (content-hashed name), or by the docker_swarm_stack role
# from stack.yml. Leave empty with deploy.sh.
DJ_API_TOKEN_SECRET_NAME=
```

`deploy/deploy.sh` (then `chmod +x deploy/deploy.sh`):

```bash
#!/usr/bin/env bash
# Deploy the docker-jobs stack by hand, from a swarm manager:
#
#   ./deploy.sh check      validate .env, the token file and the compose file
#   ./deploy.sh deploy     create the token secret if new, then deploy
#   ./deploy.sh status     the stack's services, and the jobs running now
#
# The API token is a swarm secret named after a hash of its content: a new
# token is a new secret, and the backend rolls onto it.
#
# DOCKER_CONTEXT decides WHICH swarm. Check it before deploying.
set -euo pipefail

STACK="${STACK:-docker-jobs}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

TOKEN=secrets/api-token

REQUIRED=(
    DJ_IMAGE_REGISTRY DJ_BACKEND_VERSION SOCKET_PROXY_IMAGE DJ_RUN_STACK
    DJ_MAX_RUNNING MONITORING_NETWORK OTEL_EXPORTER_OTLP_ENDPOINT
)

die() { echo "ERROR: $*" >&2; exit 1; }

load_env() {
    [ -f .env ] || die "No .env here -- cp .env.example .env and fill it in."
    set -a
    # shellcheck disable=SC1091
    . ./.env
    set +a

    local name missing=()
    for name in "${REQUIRED[@]}"; do
        [ -n "${!name:-}" ] || missing+=("$name")
    done
    [ ${#missing[@]} -eq 0 ] || die "Empty in .env: ${missing[*]}"
}

check_files() {
    [ -s "$TOKEN" ] || die "No $TOKEN -- create it: openssl rand -hex 32 > $TOKEN"
}

# name_for <prefix> <file>: a content-addressed object name
name_for() {
    echo "${1}_$(sha256sum "$2" | cut -c1-12)"
}

ensure_secret() {
    if docker secret inspect "$1" > /dev/null 2>&1; then
        echo "  secret $1 exists"
    else
        docker secret create --label "com.docker.stack.namespace=$STACK" "$1" "$2" > /dev/null
        echo "  secret $1 created"
    fi
}

objects() {
    DJ_API_TOKEN_SECRET_NAME="$(name_for "${STACK//-/_}_api_token" "$TOKEN")"
    export DJ_API_TOKEN_SECRET_NAME
}

cmd_check() {
    load_env
    check_files
    objects
    docker stack config -c docker-compose.yml > /dev/null
    echo "OK: .env, the token and docker-compose.yml are consistent"
    echo "Target: DOCKER_CONTEXT=${DOCKER_CONTEXT:-$(docker context show)}"
}

cmd_deploy() {
    cmd_check
    docker network inspect "$MONITORING_NETWORK" > /dev/null 2>&1 \
        || die "External network $MONITORING_NETWORK does not exist on this swarm."
    [ -n "$(docker node ls --filter node.label=docker-jobs=true -q)" ] \
        || die "No node labelled docker-jobs=true: docker node update --label-add docker-jobs=true <node>"
    echo "Swarm objects:"
    ensure_secret "$DJ_API_TOKEN_SECRET_NAME" "$TOKEN"
    docker stack deploy -c docker-compose.yml --prune --with-registry-auth \
        --resolve-image changed --detach=false "$STACK"
}

cmd_status() {
    load_env
    docker stack services "$STACK"
    docker stack ps --no-trunc --format 'table {{.Name}}\t{{.CurrentState}}\t{{.Error}}' "$STACK" | head -30
    echo "Jobs running now ($DJ_RUN_STACK):"
    docker service ls --filter "label=com.docker.stack.namespace=$DJ_RUN_STACK"
}

case "${1:-}" in
    check)  cmd_check ;;
    deploy) cmd_deploy ;;
    status) cmd_status ;;
    *) sed -n '2,12p' "$0"; exit 2 ;;
esac
```

`deploy/secrets/.gitkeep`: an empty file.

Check that the stack renders:

```bash
cd deploy
cp .env.example .env
openssl rand -hex 32 > secrets/api-token
DOCKER_CONTEXT=default ./deploy.sh check
cd ..
```

Expected: `OK: .env, the token and docker-compose.yml are consistent`. `deploy/.env` and `deploy/secrets/api-token` are gitignored.

- [ ] **Step 4: Write `deploy/README.md`**

```markdown
# Docker Swarm deployment

One stack, `docker-jobs`, written the way csc-ocs-ansible writes stacks: plain compose,
everything variable in `.env`, the API token an external secret with a content-hashed name.

| Service | Image | Networks | Notes |
| --- | --- | --- | --- |
| `backend` | `docker-jobs-backend` | `internal`, monitoring | API + scheduler. **1 replica**, `stop-first`, on the node labelled `docker-jobs=true` (its SQLite volume). Not routed by Traefik |
| `socket-proxy` | `tecnativa/docker-socket-proxy:v0.5.0` | `internal` | holds the docker socket, on a manager; services/tasks/secrets/configs/networks + POST |

The jobs themselves are services `djobs_<job>_<run>_<try>` of the pseudo-stack `djobs`
(`DJ_RUN_STACK`): `docker stack services djobs` lists what runs now. Never
`docker stack rm djobs`.

## Before the first deploy

- **The loki driver on every node.** The csc-ocs-ansible `install_docker` playbook installs it
  (`grafana/loki-docker-driver`, alias `loki`). A node without it fails to start the job's task.
- **A node label.** One node holds the database:
  `docker node update --label-add docker-jobs=true <node>`. Its volume `docker-jobs_dj-data` is
  the backend's state; back it up like any database (`sqlite3 .backup`).
- **The API token.** `openssl rand -hex 32 > secrets/api-token`. The Grafana app sends it from its
  `secureJsonData`; give the app the same value.
- **OTLP to Mimir.** The backend pushes to Alloy (`http://alloy:4318`). Alloy's
  `otelcol.exporter.otlphttp "mimir"` in csc-ocs-ansible points at `http://mimir:4318`, where
  Mimir does not listen: it must be `http://mimir:9009/otlp`, or the metrics never arrive.
  Check with `docker_jobs_scheduler_heartbeat_seconds` in Grafana Explore after the deploy.
- **Images** in Nexus: CI pushes `docker-jobs-backend`; the socket-proxy image comes through the
  Docker Hub connector.

## Deploy by hand

From a swarm manager, with `DOCKER_CONTEXT` pointing at the right swarm:

    cp .env.example .env          # fill in
    openssl rand -hex 32 > secrets/api-token
    ./deploy.sh check
    ./deploy.sh deploy
    ./deploy.sh status

## Deploy with Ansible

Copy this folder to `csc-ocs-ansible/stacks/platform/docker-jobs` (without `.env`,
`secrets/api-token` and `deploy.sh`), vault the token as `secrets/api-token.vault`, and add
`playbooks/deploy_platform/docker-jobs.<env>.yml` whose `stack_env` fills
[.env.example](.env.example). [stack.yml](stack.yml) declares the network and the secret.

## The Grafana side

- **Alerts.** Mount [grafana/docker-jobs-alerts.yaml](grafana/docker-jobs-alerts.yaml) at
  `/etc/grafana/provisioning/alerting/docker-jobs-alerts.yaml` in the Grafana that holds the
  Mimir datasource, with `MIMIR_DATASOURCE_UID` in its environment, and add an Alertmanager
  route for `component=docker-jobs` (unmatched alerts go to blackhole).
- **The app.** The Grafana app (`../../docker-jobs-app`, its own plan) reaches the backend at
  `http://docker-jobs_backend:8000` over `MONITORING_NETWORK`, with `GF_DATAPROXY_SEND_USER_HEADER=true`
  and the token above.
```

- [ ] **Step 5: Write `README.md` in full**

```markdown
# docker-jobs-backend

Scheduled jobs on Docker Swarm, the way Kubernetes CronJobs and Airflow run them: a job is an
image, a cron schedule and a templated command; every interval of the schedule becomes a run;
every try of a run is a one-shot Swarm service, logged through the `loki` driver; runs are
retried, rerun and cancelled from the API; OTLP metrics and Grafana alerts tell when a run
failed or never happened. The UI is the Grafana app in [`../docker-jobs-app`](../docker-jobs-app).
It replaces the host crontabs (and the Ofelia plan) of csc-ocs-ansible's ROADMAP.

## How it runs a job

- **Intervals.** The schedule is 5-field cron, in UTC. Consecutive fire times delimit data
  intervals; the run for `[start, end)` fires at `end`. An hourly job firing at 01:00 runs with
  `data_interval_start` = 00:00: `--start {{ data_interval_start | iso_ms }}` renders
  `--start 2026-10-09T00:00:00.000Z`.
- **Catch-up** (default on): after downtime, or with a `start_date` in the past, every missed
  interval gets its run, oldest first, at most `CATCHUP_BATCH` per tick. Off: only the latest
  interval runs, the others are counted as skipped.
- **Attempts.** A run's try is a `replicated-job` service `djobs_<job>_<run>_<try>`, never
  restarted by Swarm. On failure (non-zero exit, rejected task, lost service, timeout) the run
  retries after `retry_delay_seconds`, up to `retries` times, then fails.
- **Limits.** `max_active_runs` per job (1: no overlap; a retrying run keeps its slot) and
  `MAX_RUNNING` across all jobs. `timeout_seconds` per attempt; `START_TIMEOUT_SECONDS` for a
  task that never starts (no node matches, image missing).
- **By hand.** Trigger a run of any ended interval (`logical_date` snaps to its interval; the
  latest ended one by default); rerun an ended run (success, failed or cancelled) with fresh
  retries; cancel a run that has not ended. Who asked, and why, is recorded.
- **Disabled** jobs schedule nothing and launch nothing; their runs wait.

## Writing a job

    POST /api/v1/jobs
    {
      "name": "recollect-lta-acri",
      "image": "127.0.0.1:8085/maas-collector:3.10.1",
      "command": ["maas_collector.rawdata.cli.odata",
                  "--replay-interface-name", "{{ params.interface }}",
                  "--replay-start-date", "{{ (data_interval_end - timedelta(days=1)) | iso_ms }}",
                  "--replay-end-date", "{{ data_interval_end | iso_ms }}",
                  "--replay-suffix", "systematic-replay-{{ params.interface }}-{{ ds }}"],
      "params": {"interface": "LTA_Acri"},
      "schedule": "0 1 * * *",
      "retries": 2, "retry_delay_seconds": 600, "timeout_seconds": 21600,
      "networks": ["ocs_rabbitmq"],
      "constraints": ["node.labels.role == maas"],
      "secrets": [{"source": "maas_collector_credentials_v3", "target": "credentials.json"}],
      "configs": [{"source": "maas_collector_odata_conf_v2", "target": "/conf/collector/odata.json"}]
    }

Templates (each list element and env value is rendered on its own -- no shell, so a value with
spaces stays one argument; an unknown variable is an error, not an empty string):

| In the context | |
| --- | --- |
| `data_interval_start`, `data_interval_end`, `logical_date` | aware UTC datetimes (`logical_date` = start) |
| `ds`, `ts` | `2026-10-09`, `2026-10-09T00:00:00+00:00` |
| `run_id`, `try_number`, `job_name`, `params.<key>` | |
| `timedelta(...)` | window arithmetic |
| filters `iso_ms`, `iso` | `2026-10-09T00:00:00.000Z`, `2026-10-09T00:00:00Z` |

`entrypoint` replaces the image's ENTRYPOINT, `command` its CMD. Secrets mount at
`/run/secrets/<target or source>` as uid/gid 10001, mode 0400; configs at `target`, mode 0444.
Swarm objects are referenced by their exact (versioned) names; networks created by a stack by
their full name (`stack_network`). Bind mounts are refused unless `ALLOW_BIND_MOUNTS=true`.
`POST /api/v1/preview` renders a definition and lists the secrets, configs and networks it
would not find. Use immutable image tags: Swarm resolves them when each attempt starts.

## API

| Route | |
| --- | --- |
| `GET /health` | liveness, `last_tick` |
| `GET /api/v1/whoami` | the caller (`X-Grafana-User`) |
| `GET/POST /api/v1/jobs`, `GET/PUT/DELETE /api/v1/jobs/{name}` | definitions; the name is fixed; DELETE refuses while an attempt runs |
| `POST /api/v1/jobs/{name}/trigger` | `{logical_date?, note?}`: a manual run |
| `POST /api/v1/preview` | `{job, logical_date?}`: rendered command, next fire times, problems |
| `GET /api/v1/runs?job=&state=&offset=&size=` | newest interval first |
| `GET /api/v1/runs/{id}` | with every attempt: rendered command, exit code, error, node, `loki_selector` |
| `POST /api/v1/runs/{id}/rerun`, `POST /api/v1/runs/{id}/cancel` | `{note?}` |
| `/docs` | OpenAPI |

Datetimes are ISO 8601 with a timezone, both ways. With `API_TOKEN` set, every `/api/v1` route
needs `Authorization: Bearer <token>` -- the Grafana app's proxy route adds it.

## Logs

Every job container logs through the `loki` driver, house block (`loki-url`
`http://127.0.0.1:3100/loki/api/v1/push`, retries 5, batch 400, non-blocking), not
overridable per job. One attempt: `{swarm_stack="djobs", swarm_service="djobs_<job>_<run>_<try>"}`
(the API returns it as `loki_selector`); every run of a job:
`{swarm_stack="djobs", swarm_service=~"djobs_<job>_.*"}`.

## Metrics and alerts

OTLP/HTTP to Alloy (`OTEL_EXPORTER_OTLP_ENDPOINT`), on to Mimir; `docker_job` is the job:

| Prometheus name | |
| --- | --- |
| `docker_jobs_runs_created_total{run_type}` | scheduled or manual |
| `docker_jobs_runs_finished_total{state}` | success, failed (no try left), cancelled -- 0 from the start |
| `docker_jobs_attempts_started_total{reason}` | scheduled, manual, retry, rerun |
| `docker_jobs_attempts_finished_total{outcome}`, `docker_jobs_attempt_duration_seconds_*` | success, failed, timeout, lost, cancelled |
| `docker_jobs_intervals_skipped_total` | not caught up |
| `docker_jobs_job_gaps` | intervals ended (`GAP_GRACE_SECONDS` ago, within `GAP_LOOKBACK_HOURS`) without data |
| `docker_jobs_job_covered_until_seconds` | end of the latest interval with a success |
| `docker_jobs_runs_active{state}` | queued, running, retrying |
| `docker_jobs_docker_up`, `docker_jobs_scheduler_heartbeat_seconds` | the scheduler itself |

[deploy/grafana/docker-jobs-alerts.yaml](deploy/grafana/docker-jobs-alerts.yaml): a run failed;
intervals missing for 15 min; no scheduler tick for 5 min (or no heartbeat at all).

## Configuration

| Variable | Default | |
| --- | --- | --- |
| `DATABASE_URL`, `LOCK_FILE` | `sqlite:////data/docker-jobs.db`, `/data/scheduler.lock` | |
| `DOCKER_HOST` | docker-py's | `tcp://socket-proxy:2375` in the stack |
| `SCHEDULER_ENABLED`, `TICK_SECONDS` | `true`, `10` | |
| `MAX_RUNNING`, `CATCHUP_BATCH`, `START_TIMEOUT_SECONDS` | `8`, `100`, `600` | |
| `RUN_STACK` | `djobs` | one per backend per swarm |
| `ALLOW_BIND_MOUNTS` | `false` | |
| `LOKI_URL`, `LOKI_RETRIES`, `LOKI_BATCH_SIZE`, `LOKI_MODE` | the house block | |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_SDK_DISABLED` | OTel's | |
| `OTEL_SERVICE_NAME` | `docker-jobs-backend` | |
| `GAP_GRACE_SECONDS`, `GAP_LOOKBACK_HOURS` | `300`, `168` | |
| `API_TOKEN` / `API_TOKEN_FILE`, `USER_HEADER`, `ROOT_PATH` | none, `X-Grafana-User`, empty | |

## Development

    python3.11 -m venv .venv
    .venv/bin/pip install -e '.[testing]'
    .venv/bin/pytest            # or tox, as CI does

Against the local Docker (a single-node swarm, the loki plugin, a Loki on 3100):

    DOCKER_HOST=unix:///var/run/docker.sock DATABASE_URL=sqlite:///./dev.db LOCK_FILE=./dev.lock \
      RUN_STACK=djdev OTEL_SDK_DISABLED=true \
      .venv/bin/uvicorn --factory docker_jobs_backend.main:create_app --host 0.0.0.0 --port 18000

## Image, CI, deployment

`tox -e build`, then the [Dockerfile](Dockerfile) installs the wheel; CI:
[.github/workflows/docker-jobs-backend.yml](.github/workflows/docker-jobs-backend.yml). The stack:
[deploy/](deploy/README.md).

## Limits

- One process, one replica, one node's disk: the database is SQLite on a local volume, and the
  scheduler refuses to start twice on it (`LOCK_FILE`). That node is a single point of failure,
  like the crontabs it replaces -- but the jobs themselves run anywhere in the swarm.
- The schema is created, not migrated: a column change needs a migration written by hand.
- UTC only.
```

- [ ] **Step 6: Run the whole suite and tox**

Run: `.venv/bin/tox`
Expected: all tests pass, and the reports are written.

- [ ] **Step 7: Check the whole thing end to end on a local single-node swarm**

This uses the image from Task 12. Everything runs on the local daemon, with a throwaway Loki and OTLP collector. The backend runs as root here only so it can use the mounted socket; the stack uses the socket-proxy instead.

```bash
export DOCKER_CONTEXT=default
docker info --format '{{.Swarm.LocalNodeState}}'     # "active"; else: docker swarm init
docker plugin ls --format '{{.Name}}' | grep -q '^loki' \
  || docker plugin install grafana/loki-docker-driver:3.5.0-amd64 --alias loki --grant-all-permissions
docker run -d --name dj-loki -p 3100:3100 grafana/loki:3.5.0
mkdir -p /tmp/dj-e2e && cat > /tmp/dj-e2e/otel.yaml <<'EOF'
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [debug]
EOF
docker run -d --name dj-otel -p 4318:4318 -v /tmp/dj-e2e/otel.yaml:/etc/otelcol/config.yaml \
  otel/opentelemetry-collector:0.110.0
docker run -d --name dj-backend -p 18000:8000 --user 0 \
  -v /var/run/docker.sock:/var/run/docker.sock -e DOCKER_HOST=unix:///var/run/docker.sock \
  --add-host host.docker.internal:host-gateway \
  -e OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4318 -e OTEL_METRIC_EXPORT_INTERVAL=5000 \
  -e RUN_STACK=djtest docker-jobs-backend:dev

api() { curl -fsS -H 'Content-Type: application/json' -H 'X-Grafana-User: e2e' "$@"; }

# 1. a job every minute, through the loki driver
api -X POST localhost:18000/api/v1/jobs -d '{"name": "e2e-echo", "image": "python:3.11-slim",
  "entrypoint": ["sh", "-c"], "schedule": "* * * * *",
  "command": ["echo collecting {{ data_interval_start | iso_ms }} to {{ data_interval_end | iso_ms }}"]}'
sleep 150
api 'localhost:18000/api/v1/runs?job=e2e-echo' | python3 -m json.tool | grep '"state"'
docker service ls --filter label=com.docker.stack.namespace=djtest
curl -fsSG localhost:3100/loki/api/v1/query_range --data-urlencode 'query={swarm_stack="djtest"}' \
  --data-urlencode "start=$(date -d '-10 min' +%s)000000000" | grep -o 'collecting [^"]*' | head -3

# 2. a failing job: one retry, then failed; then a rerun by hand
api -X POST localhost:18000/api/v1/jobs -d '{"name": "e2e-fail", "image": "python:3.11-slim",
  "entrypoint": ["sh", "-c"], "command": ["exit 3"], "schedule": "* * * * *",
  "retries": 1, "retry_delay_seconds": 0, "catchup": false}'
sleep 150
RUN=$(api 'localhost:18000/api/v1/runs?job=e2e-fail&state=failed' | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')
api localhost:18000/api/v1/runs/$RUN | python3 -m json.tool | grep -E '"(state|exit_code|reason)"'
api -X POST localhost:18000/api/v1/runs/$RUN/rerun -d '{"note": "e2e"}' | python3 -m json.tool | grep '"state"'

# 3. a long run, cancelled
api -X PUT localhost:18000/api/v1/jobs/e2e-echo -d '{"image": "python:3.11-slim",
  "entrypoint": ["sh", "-c"], "command": ["sleep 600"], "schedule": "* * * * *"}'
sleep 80
LONG=$(api 'localhost:18000/api/v1/runs?job=e2e-echo&state=running' | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')
api -X POST localhost:18000/api/v1/runs/$LONG/cancel -d '{"note": "e2e"}' | python3 -m json.tool | grep '"state"'
docker service ls --filter label=com.docker.stack.namespace=djtest --format '{{.Name}}' | grep "_${LONG}_" \
  || echo "cancelled run's service is gone"

# 4. metrics reached the collector
docker logs dj-otel 2>&1 | grep -oE 'docker_jobs\.[a-z_.]+' | sort -u
```

Expected at each point:
1. The `e2e-echo` runs are `success`. No `djtest` service is left over. Loki returns `collecting 2026-…T…:00.000Z to 2026-…:00.000Z`.
2. The run's attempts are `failed`, with exit code 3 and reasons `scheduled` then `retry`, and the run is `failed`. After the rerun the run is `queued`, and it shortly runs again.
3. The cancel answers `cancelled`, and that run's service is gone. The next queued run of `e2e-echo` may already have started: that's expected.
4. All eleven `docker_jobs.*` instrument names from the `telemetry.py` docstring are listed.

Clean up. The backend's database is in its anonymous `/data` volume, and `-v` removes it:

```bash
docker rm -f -v dj-backend dj-otel dj-loki
docker service ls -q --filter label=com.docker.stack.namespace=djtest | xargs -r docker service rm
rm -rf /tmp/dj-e2e
```

- [ ] **Step 8: Commit**

```bash
git add deploy README.md tests/test_alert_rules.py
git commit -m "feat: swarm stack, Grafana alert rules and documentation" \
  -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Verification

- **Unit and API:** `.venv/bin/tox` passes. CI's `python-ci` runs the same command.
- **Image:** the Task 12 smoke run answers `/health` as uid 10001 with no Docker access.
- **End to end:** the Task 13 Step 7 run on a local single-node swarm shows:
  - scheduled runs succeeding, with their logs in Loki under `swarm_stack`/`swarm_service`;
  - retries, then failure, then a manual rerun;
  - a cancel that removes the service;
  - the metrics reaching an OTLP collector.
- **Production:** done outside this plan, and only on the user's go-ahead. Steps: deploy `deploy/` to the dev swarm, fix Alloy's Mimir OTLP endpoint, provision the alert rules, then check `docker_jobs_scheduler_heartbeat_seconds` and a test job's runs in Grafana.

## Out of scope (follow-ups)

- **The Grafana app** at `../docker-jobs-app`: its own plan, against the API above. Use maas-dm-app as the template:
  - a plugin.json `routes` proxy with `reqRole` (Viewer for GET, Editor for writes), a `jsonData.backendUrl`, and `secureJsonData.apiToken` sent as `Authorization: Bearer`;
  - `GF_DATAPROXY_SEND_USER_HEADER=true`.
- **csc-ocs-ansible:**
  - Alloy's `otelcol.exporter.otlphttp "mimir"` endpoint (`http://mimir:9009/otlp`);
  - an Alertmanager route for `component=docker-jobs`;
  - the `platform/docker-jobs` stack and its playbooks.
- **Moving the 25 recollect crontab lines and the satruman jobs onto job definitions.** Their configs and credentials must first exist as swarm configs and secrets (ROADMAP § Ofelia, phase B).
- **Keycloak:** it replaces `auth.get_current_user` and `require_token`.
- **Schema migrations** (Alembic), when the first column changes.
