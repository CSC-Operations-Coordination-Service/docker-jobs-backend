# Docker Swarm deployment

One stack, `docker-jobs`, written the way csc-ocs-ansible writes stacks: plain compose,
everything variable in `.env`, the API token an external secret with a content-hashed name.

| Service | Image | Networks | Notes |
| --- | --- | --- | --- |
| `backend` | `docker-jobs-backend` | `internal`, monitoring | API + scheduler. **1 replica**, `stop-first`, on the node labelled `docker-jobs=true` (its SQLite volume). Not routed by Traefik |
| `socket-proxy` | `tecnativa/docker-socket-proxy:v0.5.0` | `internal` | holds the docker socket, on a manager; services/tasks/secrets/configs/networks + POST |

The `internal` network is named `docker-jobs-socket` and the volume `docker-jobs-data`: the
backend refuses both to every job (`PROTECTED_NETWORKS`, `PROTECTED_VOLUMES`), because a swarm
service can join a non-attachable overlay, and from the socket-proxy a job could create any
service. Rename them here and in those two settings together.

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
  Check with `{__name__=~"docker_jobs_scheduler_heartbeat(_seconds)?"}` in Grafana Explore after
  the deploy: which of the two names comes back depends on Mimir's `otel_metric_suffixes_enabled`;
  the alert rules accept both.
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
