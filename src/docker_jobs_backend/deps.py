"""FastAPI dependencies: what create_app() keeps on app.state"""

from fastapi import Request

from docker_jobs_backend.settings import Settings

__all__ = ["get_app_settings"]


def get_app_settings(request: Request) -> Settings:
    """the settings the application was built with"""
    return request.app.state.settings
