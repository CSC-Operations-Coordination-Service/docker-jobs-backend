"""Fixtures: settings on a temporary database, and the application around them"""

import pytest
from factories import FakeRunner
from fastapi.testclient import TestClient
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from docker_jobs_backend.db import create_session_factory
from docker_jobs_backend.lifecycle import Lifecycle
from docker_jobs_backend.main import create_app
from docker_jobs_backend.scheduler import Scheduler
from docker_jobs_backend.settings import Settings
from docker_jobs_backend.telemetry import Metrics


@pytest.fixture(name="settings")
def settings_fixture(tmp_path):
    """a temporary SQLite file and lock, the scheduler thread off"""
    return Settings(
        database_url=f"sqlite:///{tmp_path / 'jobs.db'}",
        lock_file=str(tmp_path / "scheduler.lock"),
        scheduler_enabled=False,
    )


@pytest.fixture(name="session_factory")
def session_factory_fixture(settings):
    return create_session_factory(settings.database_url)


@pytest.fixture(name="session")
def session_fixture(session_factory):
    with session_factory() as session:
        yield session


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


@pytest.fixture(name="client")
def client_fixture(make_app):
    with TestClient(make_app()) as client:
        yield client


@pytest.fixture(name="reader")
def reader_fixture():
    return InMemoryMetricReader()


@pytest.fixture(name="metrics")
def metrics_fixture(reader, settings):
    return Metrics(MeterProvider(metric_readers=[reader]), settings)


@pytest.fixture(name="runner")
def runner_fixture():
    return FakeRunner()


@pytest.fixture(name="lifecycle")
def lifecycle_fixture(runner, metrics, settings):
    return Lifecycle(runner, metrics, settings)


@pytest.fixture(name="scheduler")
def scheduler_fixture(session_factory, lifecycle, settings, metrics):
    return Scheduler(session_factory, lifecycle, settings, metrics)
