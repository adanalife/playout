"""Synthesizes cdk8s/dist/<env>-playout-<platform>.k8s.yaml per instance.

Run via `task cdk8s:synth` (uv run --group cdk8s python cdk8s/main.py). Plain
python — no cdk8s-cli needed; jsii brings its own node runtime requirement,
pinned in .tool-versions. One Chart per (env, platform) → one dist file each,
matching how Argo applies them.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import cdk8s

sys.path.insert(0, str(Path(__file__).parent))

from config import ENVS, _versions  # noqa: E402
from playout_app import IMAGE, PlayoutInstance, emit_sentry  # noqa: E402

# release-please bumps the pin in versions.yaml on each release; it also has
# to bump the same tag in the committed dist/ that Argo applies, via a generic
# updater keyed on this annotation. cdk8s can't emit trailing comments, so we
# re-stamp it after synth — otherwise re-synth would strip the marker and the
# next release couldn't find the line to bump.
_RP_MARKER = "x-release-please-version"


def _stamp_release_please_markers() -> None:
    dist = Path(__file__).parent / "dist"
    for env_name, pins in _versions().items():
        tag = pins.get("playout")
        if not tag:
            continue
        for path in dist.glob(f"{env_name}-playout-*.k8s.yaml"):
            text = path.read_text()
            # Every rendering of the pin needs the marker: the container's
            # `image:` line and the PreSync image gate's crane arg. A pin
            # release-please can't find goes stale and fails the synth gate.
            for pinned_line in (f"image: {IMAGE}:{tag}", f"- {IMAGE}:{tag}"):
                text = text.replace(pinned_line, f"{pinned_line} # {_RP_MARKER}")
            path.write_text(text)


def main() -> None:
    app = cdk8s.App(outdir=str(Path(__file__).parent / "dist"))
    for env in ENVS.values():
        for i, platform in enumerate(env.platforms):
            chart = cdk8s.Chart(app, f"{env.name}-playout-{platform}")
            PlayoutInstance(chart, platform, env=env)
            if i == 0:
                emit_sentry(chart, env)  # once per namespace
    app.synth()
    _stamp_release_please_markers()

    # Discovery index for infra's playout ApplicationSet: one tiny JSON per deploy
    # unit at dist/apps/<env>-<app>.json. infra's git-files generator globs these
    # to self-discover one Application per unit. Byte-identical to what the
    # gateway/tripbot emit.
    apps_dir = Path(__file__).parent / "dist" / "apps"
    apps_dir.mkdir(parents=True, exist_ok=True)
    for env in ENVS.values():
        for platform in env.platforms:
            entry = {"env": env.name, "app": f"playout-{platform}"}
            (apps_dir / f"{env.name}-playout-{platform}.json").write_text(
                json.dumps(entry, indent=2, sort_keys=True) + "\n"
            )


if __name__ == "__main__":
    main()
