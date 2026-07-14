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
