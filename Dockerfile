# playout image: gapless dashcam playout publishing RTSP to MediaMTX.
#
# amd64 only — the minipc (stage-1 + prod-1) is amd64 and nothing schedules
# playout onto the Pi 5. Add an arm64 leg if that changes.
#
# Bases come from the GHCR mirrors (see mirror-images.yml), never Docker Hub:
# Hub's pull rate limit repeatedly broke fleet CI (manifest GETs count against
# the quota even on full layer-cache hits).
FROM ghcr.io/adanalife/mirror/rust:1.97-trixie AS build

# DL3008: deliberately unpinned — Debian apt versions age out of the repo, so
# a pinned build breaks on every point release; the base tag pins the distro.
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build identity stamped into the binary (served at /version). release.yml
# passes real values; the defaults mark unstamped images as dev builds.
ARG VERSION=dev
ARG SHA=unknown
RUN VERSION=$VERSION SHA=$SHA BUILT_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
    cargo build --release

# Runtime: trixie matches the build stage's glibc and ships GStreamer 1.26.
FROM ghcr.io/adanalife/mirror/debian:trixie-slim

# x264enc lives in -ugly, rtspclientsink in -rtsp, software H.264 decode in
# -libav, the VA plugin (vah264enc + hw decode) in -bad; the Intel driver
# needs non-free enabled.
# hadolint ignore=DL3008
RUN sed -i 's/Components: main/Components: main non-free non-free-firmware/' \
    /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-rtsp \
    gstreamer1.0-tools \
    intel-media-va-driver-non-free \
    libva2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/playout /usr/local/bin/playout

# Fail the build early if the load-bearing elements are missing from the
# plugin packages above (they have moved between packages across releases).
RUN gst-inspect-1.0 --exists uridecodebin3 \
    && gst-inspect-1.0 --exists concat \
    && gst-inspect-1.0 --exists x264enc \
    && gst-inspect-1.0 --exists rtspclientsink

ENTRYPOINT ["playout"]
