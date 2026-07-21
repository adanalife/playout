# Changelog

<!-- towncrier release notes start -->

## [v0.15.0] — 2026-07-21

### Changed

- The supported-platform set now comes from platform-gateway's generated `platforms.json` (synced via `task platforms:sync`) rather than a hardcoded per-env list — prod-1 and stage-1 synthesize the full supported set (adding parked `instagram`/`tiktok`), and a future platform is picked up by re-syncing. A `platforms-contract` CI check keeps the synced copy matched to the gateway. ([#86](https://github.com/adanalife/playout/pull/86))

### Fixed

- Match prod-1 playout deploy units by glob in release-please's version-pin set, so every prod platform's image tag bumps in lockstep on release and new platforms are picked up without editing the config. ([#88](https://github.com/adanalife/playout/pull/88))

## [v0.14.0] — 2026-07-21

### Changed

- Playout Deployments now birth parked at `replicas: 0` for every platform and env — a platform comes online via the console's per-platform scale-up, which sticks because Argo ignores `.spec.replicas`. Replaces the `parked_platforms` cdk8s knob (replica count is now runtime-owned). ([#78](https://github.com/adanalife/playout/pull/78))

## [v0.13.1] — 2026-07-20

### Fixed

- Retry the initial NATS connection instead of disabling the control plane for the life of the pod. A boot-race — playout starting before NATS is reachable — no longer silently drops every playback command (`!find`/`!goto`/`!timewarp`/`!skip`) while the stream keeps looping; the queued subscriptions now flush once NATS answers. A new `playout_nats_connected` gauge (1 up / 0 down) surfaces the connection state on the dashboard. ([#76](https://github.com/adanalife/playout/pull/76))

## [v0.13.0] — 2026-07-17

### Added

- Add a parked prod-1 playout-facebook instance (replicas:0) feeding the mediamtx-facebook relay; renders at the pinned image and unparks for a Facebook go-live. ([#73](https://github.com/adanalife/playout/pull/73))

## [v0.12.0] — 2026-07-17

### Added

- Stage runs playout-facebook (publishing to the mediamtx-facebook relay) with playout-youtube parked ([#71](https://github.com/adanalife/playout/pull/71))

### Changed

- Add a `parked_platforms` cdk8s knob (same shape as the tripbot/obs repos) and park prod playout-youtube at replicas:0 while the YouTube Data API quota extension is pending ([#70](https://github.com/adanalife/playout/pull/70))

## [v0.11.3] — 2026-07-17

### Fixed

- Frame-gap detection now keys off DTS instead of PTS. In the passthrough path the tee-sink probe sees H.264 access units in decode order, where PTS is non-monotonic (B-frame reordering) — so the PTS-based check false-fired on roughly half of all frames, making `playout_output_frame_gaps_total` read ~1800/min on a healthy 60fps stream. DTS is monotonic in decode order, so a jump is a genuine late frame; raw video carries no DTS and falls back to PTS (already in presentation order there). ([#68](https://github.com/adanalife/playout/pull/68))

## [v0.11.2] — 2026-07-17

### Fixed

- Stamp `service.platform` onto every metric data point, not just the OTLP resource. Grafana Cloud promotes a data-point attribute to a per-series `service_platform` label but files a custom *resource* attribute into `target_info` only, so the shared "playout ↔ MediaMTX" dashboard's `service_platform=~"$platform"` filter matched no playout series and every playout panel read empty. Mirrors the Go fleet's per-record platform stamp. ([#66](https://github.com/adanalife/playout/pull/66))

## [v0.11.1] — 2026-07-16

### Fixed

- Long seeks no longer burst-parse the corpus: a seek Discoverer-probes at most 30 uncached clips and estimates the rest from the mean duration seen so far, and the full-corpus duration warm at startup is gone. An unbounded walk could parse all 4406 clips at once (10+ cores), starving the encoders sharing the box. ([#62](https://github.com/adanalife/playout/pull/62))
- Add an Argo PreSync hook that verifies the pinned image exists in the registry before a sync tears down the running pod, preventing an ImagePullBackOff outage when a deploy is synced ahead of its image build. ([#63](https://github.com/adanalife/playout/pull/63))

## [v0.11.0] — 2026-07-16

### Added

- `seek` command verb: move the playhead by a signed duration (`delta_ms`), walking real clip durations across boundaries in either direction and wrapping moves longer than the corpus modulo its total length — the backend for duration-based `!skip`/`!back`. ([#55](https://github.com/adanalife/playout/pull/55))
- Releases now post a Discord notification linking the tagged `CHANGELOG.md`. ([#59](https://github.com/adanalife/playout/pull/59))

### Fixed

- Tag OTLP metrics with `deployment.environment` set to the k8s namespace (`prod-1`/`stage-1`) to match the rest of the fleet, instead of the NATS env (`production`/`staging`). Playout's series now match the shared Grafana dashboards' and alert rules' env filter. ([#61](https://github.com/adanalife/playout/pull/61))

### Misc

- Extract the clip/playlist engine from `main.rs` into a `player` module, and rename the behavior test harness from `parity` to `behavior`. No behavior change. ([#56](https://github.com/adanalife/playout/pull/56))

## [v0.10.0] — 2026-07-16

### Added

- Output-frame telemetry: `playout_output_frames_total` (rate is true output fps) and `playout_output_frame_gaps_total` (PTS jumps past 1.5 frame intervals — visible stalls/drops), tapped at the output tee's sink pad. ([#52](https://github.com/adanalife/playout/pull/52))
- `/debug/pipeline` HTTP endpoint dumps the live GStreamer topology as Graphviz `.dot` (`debug_to_dot_data`) for on-demand pipeline inspection on a running pod. ([#54](https://github.com/adanalife/playout/pull/54))

## [v0.9.1] — 2026-07-16

### Fixed

- A corrupt or unplayable clip no longer kills the pipeline (and, via resume-from-lastplayed, crash-loops on it): the failed clip bin is torn down and playback skips to the next clip, like vlc-server rolling past bad files. Encoder/sink errors stay fatal, and an all-bad playlist still gives up instead of spinning. ([#50](https://github.com/adanalife/playout/pull/50))

### Misc

- Drop `--edit` from the `changelog:add` task so it no longer opens $EDITOR and hangs in non-interactive (Claude/CI) sessions. ([#46](https://github.com/adanalife/playout/pull/46))

## [v0.9.0] — 2026-07-16

### Added

- Tag the Sentry scope with `platform` (twitch/youtube) so per-platform errors are filterable within the shared project. ([#48](https://github.com/adanalife/playout/pull/48))

## [v0.8.0] — 2026-07-15

### Changed

- prod-1 encodes with `ENCODER=passthrough` (stream-copy) — x264 could not hold 1080p60 realtime (2026-07-14 youtube A/B; 2026-07-15 twitch 11.7-core runaway that starved OBS). ([#44](https://github.com/adanalife/playout/pull/44))

## [v0.7.0] — 2026-07-15

### Added

- prod-1 renders a `playout-twitch` instance alongside youtube — the second (and last) platform ahead of the vlc-server cutover. Publishes into `mediamtx-twitch`; same VAAPI/iGPU/priority shape as youtube. ([#38](https://github.com/adanalife/playout/pull/38))
- `ENCODER=passthrough`: publish the corpus's compressed H.264 straight to MediaMTX with no decode and no re-encode — the uniform corpus spec (identical params, IDR-leading closed 2s GOPs) makes compressed splicing safe. Resume/play.at seeks snap to the keyframe at/before the target (≤2s early). Stage runs passthrough as the soak bed; prod stays on x264 until it proves out. ([#43](https://github.com/adanalife/playout/pull/43))

### Fixed

- Track the prod-1 playout-twitch dist manifest in release-please `extra-files` so its image pin is bumped at release time alongside the youtube instance. ([#40](https://github.com/adanalife/playout/pull/40))
- Encode with x264 on CPU instead of VAAPI — a 4th concurrent VAAPI session saturated the iGPU and dropped ~90% of OBS output frames; the minipc has ample CPU headroom. Pods pin to the minipc via nodeSelector (the i915 claim used to do this as a side effect). ([#42](https://github.com/adanalife/playout/pull/42), [#43](https://github.com/adanalife/playout/pull/43))

## [v0.6.1] — 2026-07-15

### Added

- CI behavioral-parity harness (`tests/parity.rs`): every PR boots the real binary against a real MediaMTX + NATS JetStream with synthetic clips and asserts over HTTP/NATS/RTSP — cold-boot publish and byte-exact `/vlc/current`, resume from a pre-seeded lastplayed (the 0.4.0 wedge regression test) with tail-guard and missing-file variants, every command verb plus other-platform isolation and edge payloads, boundary wrap, lastplayed ticker advance, and clean SIGTERM exit. A corrupt-clip resilience test ships ignored, documenting a known gap. ([#34](https://github.com/adanalife/playout/pull/34))

### Fixed

- OTLP metrics now carry the fleet's `service_namespace` / `service_platform` / `deployment_environment` labels (was `platform` / `deployment_environment_name`, with no namespace). Playout's series now line up with the shared Grafana dashboards and the `by (service_platform, deployment_environment)` alert rules like the rest of the fleet. ([#36](https://github.com/adanalife/playout/pull/36))

## [v0.6.0] — 2026-07-15

### Added

- Sentry error reporting: `tracing` ERROR events become Sentry events (WARN/INFO attach as breadcrumbs), tagged with the release and the `ENV` environment. Enabled by the `SENTRY_DSN` env var, delivered via a per-namespace ESO secret; local runs without it are unaffected. ([#29](https://github.com/adanalife/playout/pull/29))
- cdk8s: a `playout-<platform>` Service exposes the HTTP control surface on :8080 (the name tripbot's `VLC_SERVER_HOST` dials after cutover), and the Deployment gains liveness/readiness probes against `/health/live` and `/health/ready`. ([#30](https://github.com/adanalife/playout/pull/30))
- RTSP publish watchdog (vlc-server parity): DESCRIBE-probes the MediaMTX path every 30s and exits non-zero after 3 consecutive failures, so k8s restarts the pod and playback resumes from JetStream. Catches the dead-publish-while-PLAYING failure mode that readiness probes can't see. ([#32](https://github.com/adanalife/playout/pull/32))
- OTLP metrics push to Grafana Cloud (the Rust counterpart of the Go fleet's `pkg/telemetry`): playhead position and pipeline running time sampled every 5s, plus clip-spawn and per-verb command counters, tagged with service version, platform, and environment. Gates off when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset; the deployment reads the shared `grafana-cloud-otlp` secret. ([#33](https://github.com/adanalife/playout/pull/33))

### Changed

- Startup matches vlc-server: a cold boot with no resume state plays a random clip instead of always the first, and the corpus scan walks subdirectories recursively. An empty corpus still exits loudly (deliberate divergence — a crash-looping pod beats a silent dead stream). ([#31](https://github.com/adanalife/playout/pull/31))

## [v0.5.2] — 2026-07-15

### Fixed

- Resume and `play.at` seeks now actually take effect: the seek is issued off the streaming thread once the clip is fully up, its flush is contained inside the clip bin, and teardown of a prerolled clip no longer can deadlock the control plane. The lastplayed playhead is now clock-derived so cached positions neither freeze nor race ahead. ([#25](https://github.com/adanalife/playout/pull/25))

## [v9.9.9] — 2026-07-15

### Misc

- Adopt towncrier changelog fragments. ([#99](https://github.com/adanalife/playout/pull/99))

## [0.5.1](https://github.com/adanalife/playout/compare/v0.5.0...v0.5.1) (2026-07-15)


### Bug Fixes

- request concat pads in spawn order, not preroll order ([#22](https://github.com/adanalife/playout/issues/22)) ([d2c4ceb](https://github.com/adanalife/playout/commit/d2c4cebaae6efcb8ecb29fbc35e49dd2766e6e87))

## [0.5.0](https://github.com/adanalife/playout/compare/v0.4.0...v0.5.0) (2026-07-15)


### Features

- add /version endpoint ([#18](https://github.com/adanalife/playout/issues/18)) ([b5332dc](https://github.com/adanalife/playout/commit/b5332dc4303ea08baec3f58ea8f270603e3133f0))
- graceful shutdown on SIGTERM ([#20](https://github.com/adanalife/playout/issues/20)) ([619f33e](https://github.com/adanalife/playout/commit/619f33e8fa2e877b703d3717ccec39ac2d401567))
- split /health into /health/live and /health/ready ([#16](https://github.com/adanalife/playout/issues/16)) ([4c0b4bb](https://github.com/adanalife/playout/commit/4c0b4bb5477b96112b2dfe4daa2c70a27823f33b))
- structured logging via tracing ([#15](https://github.com/adanalife/playout/issues/15)) ([d83cd9c](https://github.com/adanalife/playout/commit/d83cd9c0063fd67c5784dce5c56d152493810ce3))


### Bug Fixes

- seek resume offset before linking the clip into concat ([#17](https://github.com/adanalife/playout/issues/17)) ([de4eaf7](https://github.com/adanalife/playout/commit/de4eaf74f25e0dd0b1fbc044e7f79b67a409d775))
- subscribe per-platform leafed NATS command subjects ([#21](https://github.com/adanalife/playout/issues/21)) ([242110e](https://github.com/adanalife/playout/commit/242110e11968cc8bf8e188f010600f44145e829a))

## [0.4.0](https://github.com/adanalife/playout/compare/v0.3.0...v0.4.0) (2026-07-15)


### Features

- **cdk8s:** stage playout on VAAPI encode with the iGPU claim ([#12](https://github.com/adanalife/playout/issues/12)) ([359299f](https://github.com/adanalife/playout/commit/359299fb03207b5e6ebed52fed54ba75a275a8be))
- **control-plane:** vlc-server-compatible NATS commands, /vlc/current, and lastplayed resume ([#10](https://github.com/adanalife/playout/issues/10)) ([c99486b](https://github.com/adanalife/playout/commit/c99486b908020c62e762c0d3d528afe72eb9ca87))

## [0.3.0](https://github.com/adanalife/playout/compare/v0.2.0...v0.3.0) (2026-07-14)


### Features

- **playout:** Enable VAAPI encoding and add pipeline queues ([#9](https://github.com/adanalife/playout/issues/9)) ([02b9a6e](https://github.com/adanalife/playout/commit/02b9a6ea9a9e85e9f3c29a9c7fb4434496ca0e51))


### Bug Fixes

- **ci:** drop component prefix from release tags ([#7](https://github.com/adanalife/playout/issues/7)) ([2276382](https://github.com/adanalife/playout/commit/22763825b8c81ef91b5b20e4de19e1314acd2e8f))

## [0.2.0](https://github.com/adanalife/playout/compare/playout-v0.1.0...playout-v0.2.0) (2026-07-14)


### Features

- cdk8s deploy authoring (playout-youtube, stage + prod) ([#4](https://github.com/adanalife/playout/issues/4)) ([ea70041](https://github.com/adanalife/playout/commit/ea7004138fdc9410669ae9386b5ab70b0a7aa9ee))
- container image and release workflows ([#3](https://github.com/adanalife/playout/issues/3)) ([121b085](https://github.com/adanalife/playout/commit/121b0856d38a0f94394920e148970d0c8f9b7c66))
- gapless playlist pipeline walking skeleton ([#1](https://github.com/adanalife/playout/issues/1)) ([cba93c1](https://github.com/adanalife/playout/commit/cba93c100605216fcbc3c4900ab524743f3cebd6))


### Bug Fixes

- **cdk8s:** raise playout memory limit to 4Gi ([#6](https://github.com/adanalife/playout/issues/6)) ([7ff8bde](https://github.com/adanalife/playout/commit/7ff8bdefa2b8cf995d61d2fcb16746fbf53c2842))
- mediamtx Hub tags carry no v prefix ([#5](https://github.com/adanalife/playout/issues/5)) ([3abbded](https://github.com/adanalife/playout/commit/3abbdedc8a6b7727e198579bfd6ab8942c3e746a))
