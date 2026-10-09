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
