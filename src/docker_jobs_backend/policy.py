"""What a job may not ask for: whatever would make it root on the swarm

* a bind mount -- a host path -- unless ALLOW_BIND_MOUNTS;
* a network of the backend's own (PROTECTED_NETWORKS): the socket-proxy
  listens there, and from it a job could create any service, one that
  bind-mounts / included;
* a volume of the backend's own (PROTECTED_VOLUMES): its database.

Checked when a job is saved or previewed (422), and again at every launch (the
attempt fails), so a setting tightened later applies to jobs saved before.
SwarmRunner also resolves each network reference -- the daemon accepts ids and
id prefixes -- and refuses one that turns out to be protected.
"""

from typing import List, Sequence

from docker_jobs_backend.settings import Settings

__all__ = ["refusals"]


def refusals(networks: Sequence[str], mounts: Sequence[dict], settings: Settings) -> List[str]:
    """one line per thing the job may not have; empty when it may run"""
    problems = []

    if not settings.allow_bind_mounts and any(
        mount.get("type", "volume") == "bind" for mount in mounts
    ):
        problems.append("bind mounts are disabled (ALLOW_BIND_MOUNTS): use a volume")

    for name in networks:
        if name in settings.protected_networks:
            problems.append(f"network {name!r} is the backend's own")

    for mount in mounts:
        if mount.get("type", "volume") == "volume" and mount.get("source") in settings.protected_volumes:
            problems.append(f"volume {mount['source']!r} is the backend's own")

    return problems
