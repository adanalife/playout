# playout

The playout server for the [A Dana Life](https://twitch.tv/ADanaLife_) dashcam
slow-TV stream: it loops a directory of dashcam clips and publishes them as
**one continuous, gapless RTSP stream** that OBS composites and restreams.

It replaces the libvlc-based `vlc-server` in
[tripbot](https://github.com/adanalife/tripbot). libvlc's stream output
terminates the RTP stream at every clip boundary, forcing the consumer to
reconnect per clip (a 1.5–3.5s visible seam); splicing clips *without*
re-encoding corrupts the decoder instead (inter-frames referencing content
from the previous clip). This server removes both failure modes structurally:

- clips are **decoded**, normalized, and fed through **one long-lived
  encoder** — inputs swap in front of it, so the output is a single unbroken
  H.264 stream with no per-clip EOF and no stale-reference corruption.

## Architecture

Rust on [gstreamer-rs](https://gitlab.freedesktop.org/gstreamer/gstreamer-rs).

```text
playlist manager
  → uridecodebin3 (gapless input swaps)
  → videoconvert ! videoscale ! videorate ! capsfilter (1920×1080 @ 60fps)
  → tee
      ├─ encode: x264enc / vah264enc → h264parse → rtspclientsink → MediaMTX
      └─ window (optional local preview): queue → autovideosink
```

[MediaMTX](https://github.com/bluenviron/mediamtx) sits between playout and
its consumers so the OBS-facing RTSP endpoint survives playout restarts, and
off-cluster viewers get TCP transport.

## Status

Early. Current scope is the gapless media pipeline; the control plane
(NATS playback commands, `/vlc/current`, resume cache, health probes,
metrics) ports over next, wire-compatible with vlc-server so nothing
upstream changes at cutover.

## Local development

```sh
brew install mise go-task gstreamer mediamtx ffmpeg
mise install            # rust, pinned in .tool-versions
pre-commit install

task mediamtx           # terminal 1: local RTSP server on :8554
VIDEO_DIR=~/clips task run   # terminal 2: publish the loop
task play               # terminal 3: watch it
```

`task probe` streams packet timestamps off the RTSP feed — the check for
boundary EOFs and PTS discontinuities.

`curl localhost:8080/debug/pipeline | dot -Tsvg > pipe.svg` dumps the live
pipeline topology (elements, pads, negotiated caps) — handy for confirming
the passthrough-vs-encode wiring on a running pod. Or point
[`gst-dots-viewer`](https://gstreamer.freedesktop.org) at the saved `.dot`.

## Releasing

Trunk-based `main` + [release-please](https://github.com/googleapis/release-please), with towncrier changelog fragments:

1. Feature PRs target `main` (squash-merge, conventional title); each adds a
   fragment (`task changelog:add TYPE=<type>` — no PR number needed, CI fills it
   in on push) or carries the `skip-changelog` label.
2. `dev-image.yml` floats `ghcr.io/adanalife/playout:main` on every main push —
   what stage deploys.
3. `release-please.yml` maintains a standing release PR that bumps the version,
   the prod pin (`cdk8s/versions.yaml`), and the committed dist from the
   conventional commits, and collates the `changelog.d/` fragments into
   `CHANGELOG.md` on the PR branch.
4. **To ship: squash-merge the release PR.** That tags `vX.Y.Z`, creates the
   GitHub Release, and dispatches `release.yml` to build the image to GHCR. No
   manual version/changelog steps — the version follows from the commit types
   (`feat:` → minor, `fix:` → patch, `feat!:`/`BREAKING CHANGE` → major).
