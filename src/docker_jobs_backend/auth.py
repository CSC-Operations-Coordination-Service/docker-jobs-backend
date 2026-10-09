"""Who is calling

The backend trusts a header naming the user, which Grafana's plugin proxy sets
(X-Grafana-User) when the Grafana app calls it, overwriting any the browser
sent. With API_TOKEN set, a call must also carry the token the proxy route adds
from the app's secureJsonData: anything else on the overlay network that
reaches the backend gets a 401, so it cannot forge the header either.

Keycloak replaces these two functions and nothing else: every route that
needs a user depends on get_current_user, every /api/v1 route on
require_token.
"""

import hmac

from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

from docker_jobs_backend.deps import get_app_settings
from docker_jobs_backend.settings import Settings

__all__ = ["ANONYMOUS", "User", "get_current_user", "require_token"]

ANONYMOUS = "anonymous"


class User(BaseModel):
    """the caller"""

    login: str


def get_current_user(
    request: Request, settings: Settings = Depends(get_app_settings)
) -> User:
    """the caller, from the configured header"""
    login = request.headers.get(settings.user_header, "").strip()

    return User(login=login or ANONYMOUS)


def require_token(
    request: Request, settings: Settings = Depends(get_app_settings)
) -> None:
    """401 unless the request carries the API token, when one is set"""
    if not settings.api_token:
        return

    supplied = request.headers.get("Authorization", "").encode()
    expected = f"Bearer {settings.api_token}".encode()

    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(
            status_code=401,
            detail="missing or wrong API token",
            headers={"WWW-Authenticate": "Bearer"},
        )
