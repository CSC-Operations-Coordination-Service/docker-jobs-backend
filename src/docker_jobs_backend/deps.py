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
