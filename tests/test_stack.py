"""The swarm stack: what `docker stack deploy` would silently drop"""

import pathlib

import yaml

STACK = pathlib.Path(__file__).parent.parent / "deploy" / "docker-compose.yml"


def services():
    return yaml.safe_load(STACK.read_text(encoding="UTF-8"))["services"]


def test_no_service_relies_on_the_tmpfs_key_swarm_ignores():
    # docker stack deploy drops `tmpfs:` without a word: a read-only service
    # then has no writable /tmp (the app's nginx cannot write its pid, exit 1)
    assert sorted(name for name, service in services().items() if "tmpfs" in service) == []


def test_read_only_services_get_a_tmpfs_tmp_as_a_volume():
    for name, service in services().items():
        if service.get("read_only"):
            assert {"type": "tmpfs", "target": "/tmp"} in service.get("volumes", []), name
