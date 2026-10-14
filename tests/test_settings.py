"""Settings come from the environment"""

import pytest
from pydantic import ValidationError

from docker_jobs_backend.settings import Settings


def test_the_loki_defaults_are_the_house_block():
    settings = Settings()

    assert settings.loki_url == "http://127.0.0.1:3100/loki/api/v1/push"
    assert (settings.loki_retries, settings.loki_batch_size, settings.loki_mode) == (
        5,
        400,
        "non-blocking",
    )


def test_each_field_reads_its_upper_case_variable(monkeypatch):
    monkeypatch.setenv("MAX_RUNNING", "3")
    monkeypatch.setenv("RUN_STACK", "djobs-dev")
    monkeypatch.setenv("ALLOW_BIND_MOUNTS", "true")

    settings = Settings()

    assert (settings.max_running, settings.run_stack, settings.allow_bind_mounts) == (
        3,
        "djobs-dev",
        True,
    )


@pytest.mark.parametrize("stack", ["Djobs", "djobs_x", "-djobs", "a-stack-name-too-long"])
def test_a_run_stack_that_cannot_prefix_a_service_name_is_refused(stack):
    with pytest.raises(ValidationError):
        Settings(run_stack=stack)


def test_the_api_token_can_be_a_swarm_secret(tmp_path, monkeypatch):
    secret = tmp_path / "token"
    secret.write_text("from-file\n")
    monkeypatch.setenv("API_TOKEN", "from-env")
    monkeypatch.setenv("API_TOKEN_FILE", str(secret))

    assert Settings().api_token == "from-file"


def test_the_protected_objects_are_comma_separated(monkeypatch):
    monkeypatch.setenv("PROTECTED_NETWORKS", "docker-jobs-socket, other-net")

    assert Settings().protected_networks == ["docker-jobs-socket", "other-net"]
    assert Settings().protected_volumes == ["docker-jobs-data"]


def test_the_allow_lists_are_comma_separated_and_empty_by_default(monkeypatch):
    assert (Settings().allowed_networks, Settings().allowed_secrets) == ([], [])

    monkeypatch.setenv("ALLOWED_SECRETS", "app_secret, app_*")
    monkeypatch.setenv("ALLOWED_CONFIGS", "")

    assert Settings().allowed_secrets == ["app_secret", "app_*"]
    assert Settings().allowed_configs == []
