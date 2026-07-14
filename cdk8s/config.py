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


@dataclass(frozen=True)
class EnvConfig:
    name: str
    namespace: str
    image_tag: str  # floating tag (main) for components without a pin

    # tripbot env token in the NATS command subjects (tripbot.<nats_env>.vlc.*),
    # matching what vlc-server and cmd/tripbot use — NOT the k8s env name.
    nats_env: str = "development"

    # Platform instances to render. The A/B phase runs youtube only (the
    # unwatched stream); twitch joins at cutover.
    platforms: tuple[str, ...] = ("youtube",)

    # Which PVC holds the dashcam corpus: the NFS-backed `vlc-dashcam` or the
    # node-local copy `vlc-dashcam-local` (same claims vlc-server mounts).
    dashcam_claim: str = "vlc-dashcam"

    encoder: str = "x264enc"  # x264enc | vah264enc (VAAPI — needs gpu)
    gpu: bool = False  # request gpu.intel.com/i915 (VAAPI encode)
    cpu_request: str = "500m"
    priority_class: str = ""  # prod-stream on prod; "" elsewhere

    def tag_for(self, component: str) -> str:
        """Pinned release tag from versions.yaml when present, else the floating tag."""
        return _versions().get(self.name, {}).get(component, self.image_tag)

    def pull_policy_for(self, component: str) -> str:
        """Pinned tags are immutable → IfNotPresent; floating tags → Always."""
        pinned = component in _versions().get(self.name, {})
        return "IfNotPresent" if pinned else "Always"


ENVS: dict[str, EnvConfig] = {
    "prod-1": EnvConfig(
        name="prod-1",
        namespace="prod-1",
        nats_env="production",
        image_tag="latest",  # overridden by the versions.yaml pin
        dashcam_claim="vlc-dashcam-local",  # corpus served off the minipc NVMe copy
        cpu_request="2",
        priority_class="prod-stream",
        encoder="vah264enc",
        gpu=True,
    ),
    "stage-1": EnvConfig(
        name="stage-1",
        namespace="stage-1",
        nats_env="staging",
        image_tag="main",
        cpu_request="500m",
    ),
}
