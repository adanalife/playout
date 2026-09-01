# Changelog

<!-- towncrier release notes start -->

## [v0.19.0] — 2026-09-01

### Added

- Serve every wire name under the `playout` token alongside the legacy `vlc` one: commands on `tripbot.<env>.playout.*`, the `TRIPBOT_PLAYOUT_LASTPLAYED` resume cache (read `playout`-first, written to both), and `/playout/current`. First wave of the coordinated `vlc` → `playout` rename; the legacy names stay until tripbot and the console have flipped. ([#155](https://github.com/adanalife/playout/pull/155))

### Fixed

- Recover unnumbered changelog fragments (from a merge racing `changelog-number.yml`) at collate time instead of letting them publish with no PR link. ([#152](https://github.com/adanalife/playout/pull/152))

### Misc

- Re-synced `contract.json` from tripbot, which now owns the per-platform `gateway-<platform>` Service names and the gateway HTTP port. Additive vocabulary only — playout reads neither key, so this keeps the daily drift gate green. ([#147](https://github.com/adanalife/playout/pull/147))

## [v0.18.2] — 2026-08-29

### Fixed

- `playout_publish_gap_seconds` now stops its clock when the first keyframe hits the wire instead of at branch attach. The attach step it timed before is ~1ms of a ~2s handoff gap; the ~1.6s IDR wait it skipped is most of the on-air blackout. The excluded remainder (acquire-poll latency, reader reconnect) is named in the metric description. ([#143](https://github.com/adanalife/playout/pull/143))
- Log a warning when a NATS command payload doesn't decode, instead of dropping the command in silence with the `playout_commands` counter still recording it as received. ([#148](https://github.com/adanalife/playout/pull/148))

### Misc

- Correct `publish.rs`'s module doc: OBS reconnects across a deploy handoff rather than keeping its session. ([#145](https://github.com/adanalife/playout/pull/145))
- Behavior coverage for a corpus directory that is empty at boot and fills in later (a fresh PVC, or the node-local corpus repopulating after a storage swap): the empty-`VIDEO_DIR` exit is the designed deployment-fault signal, and the crash-loop now has a test proving it converges on its own once media appears — resume state written while the directory was still empty is honored, and no manual `play.random` is needed. ([#146](https://github.com/adanalife/playout/pull/146))
- Pin the undecodable-command-payload fallbacks in the behavior suite: `skip` moves one clip, `play.file` leaves state alone. ([#148](https://github.com/adanalife/playout/pull/148))
- Swap the `hadolint-docker` pre-commit hook for the plain `hadolint` binary and add a guard that fails the config if a `language: docker_image` hook is ever reintroduced. ([#149](https://github.com/adanalife/playout/pull/149))

## [v0.18.1] — 2026-08-21

### Fixed

- Publish the stream to MediaMTX over RTSP-interleaved TCP instead of UDP. `rtspclientsink` offered UDP first, and that hop dropped datagrams whenever the node was busy — MediaMTX discarded every frame a lost packet landed in, so viewers saw decoding artifacts until the next keyframe while the playhead, the frame-gap counter and OBS's skip counters all stayed clean. ([#139](https://github.com/adanalife/playout/pull/139))

## [v0.18.0] — 2026-08-19

### Added

- New `playout_publish_gap_seconds` metric: the duration of the most recent publish gap (deploy handoff or error recovery), so blackout length is measurable from Grafana instead of eyeballed from the stream. ([#137](https://github.com/adanalife/playout/pull/137))

## [v0.17.1] — 2026-08-19

### Fixed

- The publish branch no longer sheds frames during a healthy RTSP session. Its leaky queue's default 1s cap sat below rtspclientsink's ~2s steady-state occupancy, dropping roughly half of all frames and leaving every reader (OBS included) decoding constant reference errors — visible as heavy artifacts on the stream. ([#135](https://github.com/adanalife/playout/pull/135))

## [v0.17.0] — 2026-08-19

### Changed

- Deploys now hand the MediaMTX path from the old pod to the new one in under a second, instead of a ~5–8s on-air gap. The RTSP publish is a detachable branch on the output tee: a pod that can't have the path (relay parked, another publisher holding it, a rejected/kicked publish) runs map-only but *ready*, polling until the path frees and attaching the publish in-process — it never kicks a live publisher, and never exits just to reconfigure. The Deployment rolls new-then-old (`RollingUpdate`, `maxUnavailable: 0`), so a broken image or a pod that never goes ready leaves the old pod streaming instead of taking the stream down. ([#132](https://github.com/adanalife/playout/pull/132))

### Security

- `h2` moves to 0.4.16, clearing [RUSTSEC-2026-0258](https://rustsec.org/advisories/RUSTSEC-2026-0258.html) — the crate queued empty HTTP/2 DATA frames without a limit, so a stream nobody drained could grow memory unbounded or panic on a length overflow. Low severity and reached only through `hyper`, not from anything playout calls directly. Found by the advisory scan this release adds, on its first run. ([#13](https://github.com/adanalife/playout/pull/13))

### CI / Tooling

- The advisory scan can publish its findings as a check run. It was missing `checks: write`, so the create-check call 403'd, the action reported that it "seems to be executed from the forked repository" — it isn't; that is just what a permission 403 looks like from inside it — and printed the advisory report into the job log instead, well below the line that says the job failed. ([#13](https://github.com/adanalife/playout/pull/13))
- The weekly super-linter sweep passes again: codespell allowlists `unparseable`, and the workflow has the `statuses: write` permission its per-linter commit statuses need. ([#130](https://github.com/adanalife/playout/pull/130))
- The changelog-fragment numbering workflow now fails loudly when it cannot diff against the base commit, instead of reporting success having numbered nothing. ([#131](https://github.com/adanalife/playout/pull/131))
- Validate synthed `cdk8s/dist/` manifests against the cluster's k8s API schemas with kubeconform. ([#133](https://github.com/adanalife/playout/pull/133))

### Misc

- The NATS Service name and port in `NATS_URL` come from `contract.json` instead of being restated here. Rendered manifests are unchanged. ([#127](https://github.com/adanalife/playout/pull/127))

## [v0.16.0] — 2026-08-08

### Changed

- cdk8s now reads playout's Service name and HTTP port from `contract.json`, synced from tripbot via `task contract:sync`, instead of restating them by hand. A CI gate asserts the synced copy matches tripbot main. ([#123](https://github.com/adanalife/playout/pull/123))
- cdk8s now reads the per-platform MediaMTX relay's Service name and RTSP port from `contract.json` instead of building them by hand. Rendered manifests are unchanged. ([#126](https://github.com/adanalife/playout/pull/126))

### Fixed

- `playout_output_frame_gaps_total`'s exported description says DTS, matching what the probe has actually keyed off since 0.11.3. The doc comment was corrected earlier, but the description is a string literal that ships in the binary and reaches Grafana's metric browser, where it still claimed PTS. ([#118](https://github.com/adanalife/playout/pull/118))
- Set `runAsNonRoot` on the PreSync image gate pod, so it conforms to the `restricted` PodSecurity profile the pinned namespaces run. ([#122](https://github.com/adanalife/playout/pull/122))

### CI / Tooling

- Cache the GStreamer apt install and the pinned MediaMTX/NATS release tarballs in the test job. The ~160-package install was the job's least predictable step — 28s on a good day, 83s on a bad one, and once the full 30-minute timeout with zero tests run — and a warm cache now leaves the job with no network fetches at all. ([#106](https://github.com/adanalife/playout/pull/106))
- The release-please workflow can be triggered manually, so a release branch left on an old base by chore-only merges can be rebased off current `main` without waiting for the next releasable commit. ([#125](https://github.com/adanalife/playout/pull/125))

### Misc

- Re-sync `contract.json` from tripbot main, which now carries the per-platform MediaMTX service names and the RTSP port. ([#124](https://github.com/adanalife/playout/pull/124))

## [v0.15.3] — 2026-07-30

### Fixed

- Tag Sentry events with the deploy-env id (`prod-1` / `stage-1`) instead of the NATS subject env (`production` / `staging`), so playout issues filter alongside the rest of the fleet under the same environment. The `DEPLOYMENT_ENVIRONMENT` lookup shared with the OTLP label moved into one helper. ([#101](https://github.com/adanalife/playout/pull/101))
- Only report to Sentry from prod. Stage runs the same binary against parked platforms and routinely-absent upstreams, so its errors described the environment rather than a defect while spending the shared event budget — 143 of them from stage `rtspclientsink` alone. Stage errors still reach Loki and the dashboards. ([#103](https://github.com/adanalife/playout/pull/103))
- Cut boot-to-ready from 31s to 11.6s when NATS is unreachable. The stream-ensure and the resume read each spent a guaranteed 10s JetStream timeout on the boot path rediscovering what the connect window had already settled, delaying first frame by 20s on every restart while NATS was down or slow to come up alongside playout. The stream is now declared by a task that waits for a connection first, so it still gets created the moment NATS appears. ([#105](https://github.com/adanalife/playout/pull/105))

### CI / Tooling

- Close three gaps in the test suite: the test job now fails when the behavior harness's external tools (`mediamtx`, `nats-server`, `gst-launch-1.0`) are missing, instead of silently skipping every integration test and reporting green; map-only mode (the console's chat-map path, where the MediaMTX relay is parked) is covered end to end; and the RTSP watchdog's decision loop — failure threshold, reset on recovery, cold-boot initial delay — is covered on tokio's virtual clock. ([#104](https://github.com/adanalife/playout/pull/104))
- Close the rest of the test-suite audit: cover the bounded-recovery give-up (an entirely unplayable corpus must crash-loop, not spin) and the no-control-plane boot, sweep `seek_walk`'s input space for landings outside a clip, cover the OTLP header parser, and replace two assertions that could not fail. Also fixes `changelog-number` dying when one PR carries two fragments of the same type. ([#104](https://github.com/adanalife/playout/pull/104))

## [v0.15.2] — 2026-07-28

### Removed

- The unused bare `/health` alias. Liveness/readiness probes use `/health/live` and `/health/ready`; tripbot reads `/vlc/current`. ([#96](https://github.com/adanalife/playout/pull/96))

### Fixed

- Size playout's memory limit to the encode mode: 1Gi for `passthrough` (prod and stage both run it — five prod instances hold 96-185Mi steady, 420Mi worst observed over five days), 4Gi kept for the decode-and-re-encode modes. Frees 3Gi of per-instance headroom on the minipc, 15Gi across the prod fleet. ([#99](https://github.com/adanalife/playout/pull/99))

### CI / Tooling

- Collapse the fast per-PR gates (conventional title, changelog fragment, platforms.json contract, cdk8s dist sync) into a single `gates` job in `pr-gates.yml`. Actions bills per job rounded up to the minute, so four short checks cost four minutes; as steps of one job they cost one. ([#93](https://github.com/adanalife/playout/pull/93))
- Trimmed the CI surface: the post-merge `cdk8s-synth` workflow is gone (the `pr-gates` synth step already covers every change into main), `platforms-contract` runs on its daily schedule only, and the base-image mirror installs a pinned `crane` release binary instead of compiling it from source on every run. ([#95](https://github.com/adanalife/playout/pull/95))

### Misc

- NATS commands are counted once where they arrive instead of separately in `dispatch` and the seek fast-path — same `playout_commands_total` series, one increment site. ([#96](https://github.com/adanalife/playout/pull/96))
- Dropped the unreachable `SUPPORTED_PLATFORMS` subset guard from the cdk8s config — every env sets `platforms` from that same tuple, and `platforms-contract` is what actually catches gateway-side drift. ([#97](https://github.com/adanalife/playout/pull/97))

## [v0.15.1] — 2026-07-23

### Fixed

- Choose the output sink from MediaMTX relay reachability at startup: publish over RTSP when the relay is up, else run a fakesink so the pipeline still plays and the console map keeps advancing off the NATS playhead. Fixes the crash-loop when a platform is in chat-map mode (relay parked), and a lightweight monitor restarts to reconfigure when the relay comes or goes. ([#91](https://github.com/adanalife/playout/pull/91))

### CI / Tooling

- Harden shared CI workflows: scope token permissions, pin super-linter and setup-uv. ([#90](https://github.com/adanalife/playout/pull/90))

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
