"""Per-environment playout deployment config.

A slim, self-contained EnvConfig holding only the fields the playout
deployment needs — the same shape as the obs repo's config.py.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

_VERSIONS_FILE = Path(__file__).resolve().parent / "versions.yaml"


@lru_cache(maxsize=1)
def _versions() -> dict:
    return yaml.safe_load(_VERSIONS_FILE.read_text()) or {}


# The fleet-wide supported-platform set, owned by platform-gateway (its Go
# adapter registry is the source of truth) and synced into this repo's
# platforms.json via `task platforms:sync`. Every env's `platforms` must be a
# subset of it (validated below). Never hand-edit platforms.json — add an
# adapter in the gateway + re-sync.
_PLATFORMS_FILE = Path(__file__).resolve().parents[1] / "platforms.json"


def _load_supported_platforms() -> tuple[str, ...]:
    import json

    with _PLATFORMS_FILE.open() as f:
        return tuple(json.load(f)["platforms"])


SUPPORTED_PLATFORMS = _load_supported_platforms()


@dataclass(frozen=True)
class EnvConfig:
    name: str
    namespace: str
    image_tag: str  # floating tag (main) for components without a pin

    # tripbot env token in the NATS command subjects (tripbot.<nats_env>.vlc.*),
    # matching what vlc-server and cmd/tripbot use — NOT the k8s env name.
    nats_env: str = "development"

    # Platform instances to render.
    platforms: tuple[str, ...] = ("youtube",)

    # Which PVC holds the dashcam corpus: the NFS-backed `vlc-dashcam` or the
    # node-local copy `vlc-dashcam-local` (same claims vlc-server mounts).
    dashcam_claim: str = "vlc-dashcam"

    # x264enc | vah264enc (VAAPI — needs gpu) | passthrough (stream-copy;
    # publishes the corpus's compressed H.264 without re-encoding — needs
    # every clip on the uniform corpus spec)
    encoder: str = "x264enc"
    gpu: bool = False  # request gpu.intel.com/i915 (VAAPI encode)
    cpu_request: str = "500m"
    priority_class: str = ""  # prod-stream on prod; "" elsewhere

    def tag_for(self, component: str) -> str:
        """Pinned release tag from versions.yaml when present, else the floating tag."""
        return _versions().get(self.name, {}).get(component, self.image_tag)

    def pull_policy_for(self, component: str) -> str:
        """Pinned tags are immutable → IfNotPresent; floating tags → Always."""
        return "IfNotPresent" if self.is_pinned(component) else "Always"

    def is_pinned(self, component: str) -> bool:
        """True when this env deploys an immutable release tag (from
        versions.yaml) rather than the floating tag. A pinned tag can be a
        brand-new version whose image isn't built yet — the case the PreSync
        image gate guards."""
        return component in _versions().get(self.name, {})


ENVS: dict[str, EnvConfig] = {
    "prod-1": EnvConfig(
        name="prod-1",
        namespace="prod-1",
        nats_env="production",
        # Every platform's playout births parked at replicas:0; a console
        # scale-up brings one live and sticks (Argo ignores .spec.replicas).
        # Only twitch feeds a live encoder today — youtube waits on the pending
        # YouTube Data API quota extension, facebook on a go-live. Parking frees
        # the instance's CPU request on the minipc until scaled up.
        platforms=("youtube", "twitch", "facebook"),
        image_tag="latest",  # overridden by the versions.yaml pin
        dashcam_claim="vlc-dashcam-local",  # corpus served off the minipc NVMe copy
        cpu_request="2",
        priority_class="prod-stream",
        # Stream-copy the uniform corpus straight to MediaMTX. x264 is
        # disqualified here: it can't hold 1080p60 realtime on this box
        # (2026-07-14 youtube A/B, 2026-07-15 twitch runaway).
        encoder="passthrough",
    ),
    "stage-1": EnvConfig(
        name="stage-1",
        namespace="stage-1",
        nats_env="staging",
        image_tag="main",
        # facebook is the active stage platform (feeds obs-facebook via the
        # mediamtx-facebook relay); both births parked at replicas:0 and come
        # live via a console scale-up.
        platforms=("youtube", "facebook"),
        # Same encode mode as prod so the stage soak transfers.
        cpu_request="2",
        encoder="passthrough",
    ),
}


# Guard: an env can only run platforms the gateway has an adapter for.
for _name, _env in ENVS.items():
    _unknown = tuple(p for p in _env.platforms if p not in SUPPORTED_PLATFORMS)
    if _unknown:
        raise ValueError(
            f"{_name}: platforms {_unknown} not in SUPPORTED_PLATFORMS "
            f"{SUPPORTED_PLATFORMS} — add an adapter in platform-gateway + run `task platforms:sync`"
        )
