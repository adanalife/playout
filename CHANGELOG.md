# Changelog

## [0.5.1](https://github.com/adanalife/playout/compare/v0.5.0...v0.5.1) (2026-07-15)


### Bug Fixes

* request concat pads in spawn order, not preroll order ([#22](https://github.com/adanalife/playout/issues/22)) ([d2c4ceb](https://github.com/adanalife/playout/commit/d2c4cebaae6efcb8ecb29fbc35e49dd2766e6e87))

## [0.5.0](https://github.com/adanalife/playout/compare/v0.4.0...v0.5.0) (2026-07-15)


### Features

* add /version endpoint ([#18](https://github.com/adanalife/playout/issues/18)) ([b5332dc](https://github.com/adanalife/playout/commit/b5332dc4303ea08baec3f58ea8f270603e3133f0))
* graceful shutdown on SIGTERM ([#20](https://github.com/adanalife/playout/issues/20)) ([619f33e](https://github.com/adanalife/playout/commit/619f33e8fa2e877b703d3717ccec39ac2d401567))
* split /health into /health/live and /health/ready ([#16](https://github.com/adanalife/playout/issues/16)) ([4c0b4bb](https://github.com/adanalife/playout/commit/4c0b4bb5477b96112b2dfe4daa2c70a27823f33b))
* structured logging via tracing ([#15](https://github.com/adanalife/playout/issues/15)) ([d83cd9c](https://github.com/adanalife/playout/commit/d83cd9c0063fd67c5784dce5c56d152493810ce3))


### Bug Fixes

* seek resume offset before linking the clip into concat ([#17](https://github.com/adanalife/playout/issues/17)) ([de4eaf7](https://github.com/adanalife/playout/commit/de4eaf74f25e0dd0b1fbc044e7f79b67a409d775))
* subscribe per-platform leafed NATS command subjects ([#21](https://github.com/adanalife/playout/issues/21)) ([242110e](https://github.com/adanalife/playout/commit/242110e11968cc8bf8e188f010600f44145e829a))

## [0.4.0](https://github.com/adanalife/playout/compare/v0.3.0...v0.4.0) (2026-07-15)


### Features

* **cdk8s:** stage playout on VAAPI encode with the iGPU claim ([#12](https://github.com/adanalife/playout/issues/12)) ([359299f](https://github.com/adanalife/playout/commit/359299fb03207b5e6ebed52fed54ba75a275a8be))
* **control-plane:** vlc-server-compatible NATS commands, /vlc/current, and lastplayed resume ([#10](https://github.com/adanalife/playout/issues/10)) ([c99486b](https://github.com/adanalife/playout/commit/c99486b908020c62e762c0d3d528afe72eb9ca87))

## [0.3.0](https://github.com/adanalife/playout/compare/v0.2.0...v0.3.0) (2026-07-14)


### Features

* **playout:** Enable VAAPI encoding and add pipeline queues ([#9](https://github.com/adanalife/playout/issues/9)) ([02b9a6e](https://github.com/adanalife/playout/commit/02b9a6ea9a9e85e9f3c29a9c7fb4434496ca0e51))


### Bug Fixes

* **ci:** drop component prefix from release tags ([#7](https://github.com/adanalife/playout/issues/7)) ([2276382](https://github.com/adanalife/playout/commit/22763825b8c81ef91b5b20e4de19e1314acd2e8f))

## [0.2.0](https://github.com/adanalife/playout/compare/playout-v0.1.0...playout-v0.2.0) (2026-07-14)


### Features

* cdk8s deploy authoring (playout-youtube, stage + prod) ([#4](https://github.com/adanalife/playout/issues/4)) ([ea70041](https://github.com/adanalife/playout/commit/ea7004138fdc9410669ae9386b5ab70b0a7aa9ee))
* container image and release workflows ([#3](https://github.com/adanalife/playout/issues/3)) ([121b085](https://github.com/adanalife/playout/commit/121b0856d38a0f94394920e148970d0c8f9b7c66))
* gapless playlist pipeline walking skeleton ([#1](https://github.com/adanalife/playout/issues/1)) ([cba93c1](https://github.com/adanalife/playout/commit/cba93c100605216fcbc3c4900ab524743f3cebd6))


### Bug Fixes

* **cdk8s:** raise playout memory limit to 4Gi ([#6](https://github.com/adanalife/playout/issues/6)) ([7ff8bde](https://github.com/adanalife/playout/commit/7ff8bdefa2b8cf995d61d2fcb16746fbf53c2842))
* mediamtx Hub tags carry no v prefix ([#5](https://github.com/adanalife/playout/issues/5)) ([3abbded](https://github.com/adanalife/playout/commit/3abbdedc8a6b7727e198579bfd6ab8942c3e746a))
