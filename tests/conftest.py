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
