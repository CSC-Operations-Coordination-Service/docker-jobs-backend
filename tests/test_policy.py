"""The allow lists of policy.py: what a job may name"""

import pytest

from docker_jobs_backend.policy import allowed, refusals
from docker_jobs_backend.settings import Settings


@pytest.mark.parametrize(
    "name, entries, expected",
    [
        ("app_conf", ["app_conf"], True),
        # the versions a stack gives its objects
        ("app_conf_v1", ["app_conf"], True),
        ("app_conf_v12", ["app_conf"], True),
        # another object that only starts the same way
        ("app_conf_extra", ["app_conf"], False),
        ("app_conf_v", ["app_conf"], False),
        ("app_conf_v1x", ["app_conf"], False),
        ("app_confv1", ["app_conf"], False),
        ("other_app_conf", ["app_conf"], False),
        # a prefix
        ("app_conf_extra", ["app_*"], True),
        ("other", ["app_*"], False),
        ("anything", ["*"], True),
        # fails closed
        ("app_conf", [], False),
    ],
)
def test_an_entry_admits_its_name_its_versions_or_its_prefix(name, entries, expected):
    assert allowed(name, entries) is expected


def test_the_lists_fail_closed():
    settings = Settings()

    assert refusals(
        ["jobs-net"],
        [{"type": "volume", "source": "jobs-cache", "target": "/cache"}],
        settings,
        secrets=[{"source": "app_secret"}],
        configs=[{"source": "app_conf_v3"}],
    ) == [
        "network 'jobs-net' is not allowed (ALLOWED_NETWORKS)",
        "volume 'jobs-cache' is not allowed (ALLOWED_VOLUMES)",
        "secret 'app_secret' is not allowed (ALLOWED_SECRETS)",
        "config 'app_conf_v3' is not allowed (ALLOWED_CONFIGS)",
    ]


def test_a_job_naming_only_listed_objects_may_run():
    settings = Settings(
        allowed_networks=["jobs-net"],
        allowed_volumes=["jobs-cache"],
        allowed_secrets=["app_secret"],
        allowed_configs=["app_*"],
    )

    assert (
        refusals(
            ["jobs-net"],
            [{"type": "volume", "source": "jobs-cache", "target": "/cache"}],
            settings,
            secrets=[{"source": "app_secret_v2"}],
            configs=[{"source": "app_conf_v3"}, {"source": "app_model"}],
        )
        == []
    )


def test_host_is_a_network_like_any_other():
    assert refusals(["host"], [], Settings(allowed_networks=["jobs-net"])) == [
        "network 'host' is not allowed (ALLOWED_NETWORKS)"
    ]


def test_tmpfs_and_anonymous_volumes_need_no_list():
    mounts = [{"type": "tmpfs", "target": "/tmp"}, {"type": "volume", "source": "", "target": "/scratch"}]

    assert refusals([], mounts, Settings()) == []


def test_the_backends_own_objects_stay_refused_even_when_listed():
    settings = Settings(allowed_networks=["*"], allowed_volumes=["*"])

    assert refusals(
        ["docker-jobs-socket"], [{"type": "volume", "source": "docker-jobs-data", "target": "/x"}], settings
    ) == [
        "network 'docker-jobs-socket' is the backend's own",
        "volume 'docker-jobs-data' is the backend's own",
    ]
