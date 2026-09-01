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
- `ENCODER=passthrough` skips decode and encode entirely: the airing corpus is
  transcoded to one uniform spec (identical params, IDR-leading closed GOPs),
  which is what makes splicing the compressed streams safe, and `h264parse`
  re-sends SPS/PPS at every IDR so each splice and every late joiner resyncs.
  Stage and prod both run this — x264 can't hold 1080p60 realtime on the minipc.

## Architecture

Rust on [gstreamer-rs](https://gitlab.freedesktop.org/gstreamer/gstreamer-rs).

```text
playlist manager (active clip + prerolled next)
  → uridecodebin3 per clip (gapless input swaps)
  → concat (rewrites segments so running time never resets across boundaries)
  → decode path only: videoconvert ! videoscale ! videorate ! capsfilter
                      (1920×1080 @ 60fps)
  → tee
      ├─ encode:   [x264enc / vah264enc →] h264parse → rtspclientsink → MediaMTX
      ├─ map-only: fakesink, when the MediaMTX relay is parked — the pipeline
      │            keeps playing so the console map still advances
      └─ window:   queue → autovideosink (optional local preview)
```

[MediaMTX](https://github.com/bluenviron/mediamtx) sits between playout and
its consumers so the OBS-facing RTSP endpoint survives playout restarts, and
off-cluster viewers get TCP transport.

## Control plane

- **NATS commands** on `tripbot.<env>.playout.<verb>.<platform>`
  (fire-and-forget): `play.random`, `play.file`, `play.at`, `skip`, `back`,
  `seek`. The platform leaf keeps instances isolated — a Twitch-triggered skip
  can't advance the YouTube stream. tripbot's `playout-client` is the publisher.
- **Resume**: the active clip + position are republished every 5s to the
  `TRIPBOT_PLAYOUT_LASTPLAYED` JetStream last-value cache, which a restarting
  instance reads to pick up where it left off. NATS being down degrades to
  looping the corpus uncommanded, never to no stream.
- **HTTP** on `:8080`: `/health/live`, `/health/ready` (ready = pipeline
  PLAYING), `/version`, `/playout/current` (bare basename of the active clip),
  `/debug/pipeline` (live topology as Graphviz).
- **Legacy wire names**: every name above is also served under the `vlc`
  token vlc-server used — `tripbot.<env>.vlc.*` subjects,
  `TRIPBOT_VLC_LASTPLAYED`, `/vlc/current` — because tripbot and the console
  still speak it. Commands land on either; the cache is written to both and
  read `playout`-first. The legacy set goes once every consumer has moved.
- **Metrics**: OTLP push to Grafana Cloud, gated on
  `OTEL_EXPORTER_OTLP_ENDPOINT` so local runs export nothing.
- **Watchdog**: an RTSP DESCRIBE probe every 30s, since `rtspclientsink` in
  RECORD mode reports PLAYING without proving data flow. Three consecutive
  failures exit non-zero for a k8s restart.

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
