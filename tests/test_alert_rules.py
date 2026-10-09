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
