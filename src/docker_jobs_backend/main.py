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
