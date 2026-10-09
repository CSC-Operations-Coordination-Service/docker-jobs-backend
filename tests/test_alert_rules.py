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


def exported_metrics(session):
    """(name, unit, kind) of every metric the backend exports"""
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

    found = []
    for resource_metrics in reader.get_metrics_data().resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if isinstance(metric.data, Sum) and metric.data.is_monotonic:
                    kind = "counter"
                elif isinstance(metric.data, Histogram):
                    kind = "histogram"
                else:
                    kind = "gauge"
                found.append((metric.name, metric.unit, kind))
    return found


def prometheus_names(metrics, suffixes):
    """the names Mimir stores them under: with otel_metric_suffixes_enabled, a
    unit of seconds adds _seconds and a counter _total; without, neither"""
    names = set()
    for name, unit, kind in metrics:
        base = name.replace(".", "_")
        if suffixes and unit == "s":
            base += "_seconds"
        if kind == "histogram":
            names.update({f"{base}_bucket", f"{base}_sum", f"{base}_count"})
        elif kind == "counter" and suffixes:
            names.add(f"{base}_total")
        else:
            names.add(base)
    return names


def selected(expression, names):
    """the names among names that an expression's selectors pick"""
    regexes = re.findall(r'__name__=~"([^"]+)"', expression)
    plain = re.findall(r"docker_jobs_[a-z_]+", re.sub(r'__name__=~"[^"]+"', "", expression))
    picked = {name for name in names for regex in regexes if re.fullmatch(regex, name)}
    return picked | (set(plain) & names), len(regexes) + len(plain)


def test_every_alert_finds_its_metric_with_or_without_mimir_suffixes(session):
    metrics = exported_metrics(session)
    expressions = [
        query["model"]["expr"] for rule in rules() for query in rule["data"] if "expr" in query["model"]
    ]

    for suffixes in (True, False):
        names = prometheus_names(metrics, suffixes)
        picked = set()
        for expression in expressions:
            found, selectors = selected(expression, names)
            assert len(found) == selectors, (suffixes, expression, found)
            picked |= found
        assert {name.removesuffix("_total").removesuffix("_seconds") for name in picked} == {
            "docker_jobs_runs_finished",
            "docker_jobs_job_gaps",
            "docker_jobs_scheduler_heartbeat",
        }


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
