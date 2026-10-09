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

OTLP/HTTP to Alloy (`OTEL_EXPORTER_OTLP_ENDPOINT`), on to Mimir; `docker_job` is the job. The
names below carry the `_total`/`_seconds` suffixes Mimir adds when `otel_metric_suffixes_enabled`
is on; without it they end at the base name (`docker_jobs_runs_finished`), and the alert rules
match both:

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
