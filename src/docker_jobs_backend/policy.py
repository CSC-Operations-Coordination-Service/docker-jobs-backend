"""What a job may not ask for: whatever would make it root on the swarm, or
hand it what is meant for other services

* a bind mount -- a host path -- unless ALLOW_BIND_MOUNTS;
* a network of the backend's own (PROTECTED_NETWORKS): the socket-proxy
  listens there, and from it a job could create any service, one that
  bind-mounts / included;
* a volume of the backend's own (PROTECTED_VOLUMES): its database;
* any other network, named volume, secret or config outside its allow list
  (ALLOWED_NETWORKS, ALLOWED_VOLUMES, ALLOWED_SECRETS, ALLOWED_CONFIGS). The
  lists fail closed: an empty one refuses every name. A network is refused by
  name whatever its driver, so `host` too unless listed.

Checked when a job is saved or previewed (422), and again at every launch (the
attempt fails), so a setting tightened later applies to jobs saved before.
SwarmRunner also resolves each network reference -- the daemon accepts ids and
id prefixes -- and refuses one whose name turns out protected or not allowed.
"""

import re
from typing import List, Sequence

from docker_jobs_backend.settings import Settings

__all__ = ["allowed", "network_refusal", "refusals"]

_VERSION = re.compile(r"_v[0-9]+")


def allowed(name: str, entries: Sequence[str]) -> bool:
    """whether an allow list admits name: an entry matches itself and its stack
    versions (<entry>_v<n>); one ending in * matches by prefix"""
    for entry in entries:
        if entry.endswith("*"):
            if name.startswith(entry[:-1]):
                return True
        elif name == entry or (
            name.startswith(entry) and _VERSION.fullmatch(name[len(entry):])
        ):
            return True
    return False


def network_refusal(name: str, settings: Settings) -> str:
    """why a job may not join the network called name; empty when it may"""
    if name in settings.protected_networks:
        return f"network {name!r} is the backend's own"
    if not allowed(name, settings.allowed_networks):
        return f"network {name!r} is not allowed (ALLOWED_NETWORKS)"
    return ""


def refusals(
    networks: Sequence[str],
    mounts: Sequence[dict],
    settings: Settings,
    secrets: Sequence[dict] = (),
    configs: Sequence[dict] = (),
) -> List[str]:
    """one line per thing the job may not have; empty when it may run"""
    problems = []

    if not settings.allow_bind_mounts and any(
        mount.get("type", "volume") == "bind" for mount in mounts
    ):
        problems.append("bind mounts are disabled (ALLOW_BIND_MOUNTS): use a volume")

    for name in networks:
        problem = network_refusal(name, settings)
        if problem:
            problems.append(problem)

    for mount in mounts:
        # an anonymous volume (no source) is the job's own: nothing to check
        source = mount.get("source")
        if mount.get("type", "volume") != "volume" or not source:
            continue
        if source in settings.protected_volumes:
            problems.append(f"volume {source!r} is the backend's own")
        elif not allowed(source, settings.allowed_volumes):
            problems.append(f"volume {source!r} is not allowed (ALLOWED_VOLUMES)")

    for secret in secrets:
        if not allowed(secret["source"], settings.allowed_secrets):
            problems.append(f"secret {secret['source']!r} is not allowed (ALLOWED_SECRETS)")

    for config in configs:
        if not allowed(config["source"], settings.allowed_configs):
            problems.append(f"config {config['source']!r} is not allowed (ALLOWED_CONFIGS)")

    return problems
