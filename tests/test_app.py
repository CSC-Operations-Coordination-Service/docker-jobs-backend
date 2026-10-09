"""The application shell: health, the caller, the API token"""

from fastapi.testclient import TestClient


def test_health_answers(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_the_caller_is_the_grafana_user(client):
    response = client.get("/api/v1/whoami", headers={"X-Grafana-User": "aagazzi"})

    assert response.json() == {"login": "aagazzi"}


def test_without_the_header_the_caller_is_anonymous(client):
    assert client.get("/api/v1/whoami").json() == {"login": "anonymous"}


def test_with_a_token_set_the_api_requires_it(settings, make_app):
    settings.api_token = "s3cret"

    with TestClient(make_app()) as client:
        assert client.get("/api/v1/whoami").status_code == 401
        wrong = {"Authorization": "Bearer nope"}
        assert client.get("/api/v1/whoami", headers=wrong).status_code == 401
        right = {"Authorization": "Bearer s3cret"}
        assert client.get("/api/v1/whoami", headers=right).status_code == 200
        assert client.get("/health").status_code == 200
