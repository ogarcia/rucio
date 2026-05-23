# syntax=docker/dockerfile:1.7
#
# Produces two independent runtime images:
#
#   ruciod  →  tag "master" / "0.1.0" / "latest"
#              Only the daemon binary.  Use this for production nodes,
#              bootstrap servers and VPS deployments.
#
#   full    →  tag "master-full" / "0.1.0-full" / "latest-full"
#              Both ruciod (daemon) and rucio (CLI).  Use this for
#              development, debugging, or inspecting a running node.
#
# Local build (compiles from source):
#   podman build --target ruciod -t rucio:dev .
#   podman build --target full   -t rucio:dev-full .
#
# CI build (binaries pre-compiled and placed in dist/):
#   podman build --target ruciod --build-arg BUILDER=prebuilt .
#   podman build --target full   --build-arg BUILDER=prebuilt .
#
# Environment variables (runtime):
#   RUCIOD_CONFIG   Path to the daemon config file — optional, defaults to
#                   $HOME/.config/rucio/config.toml (/var/lib/rucio/.config/…)

# ── Stage 1a: compile from source (default local path) ──────────────────────

FROM rust:1-alpine3.23 AS builder

# musl-dev: C headers needed by ring (pulled in by rustls via reqwest).
# Everything else in the dependency tree is pure Rust.
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY . .

RUN cargo build --release --locked && \
    cp target/release/ruciod /usr/bin/ruciod && \
    cp target/release/rucio  /usr/bin/rucio  && \
    strip /usr/bin/ruciod /usr/bin/rucio

# ── Stage 1b: pre-built binaries injected from CI (dist/ in build context) ──

FROM scratch AS prebuilt
COPY dist/ruciod /usr/bin/ruciod
COPY dist/rucio  /usr/bin/rucio

# ── Stage 1c: indirection — points to 'builder' or 'prebuilt' via build arg ─

ARG BUILDER=builder
FROM ${BUILDER} AS bins

# ── Stage 2: runtime – daemon only (tag: master / vX.Y.Z / latest) ──────────

FROM alpine:3.23 AS ruciod

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/ruciod /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

EXPOSE 4321/tcp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 3: runtime – full (tag: master-full / 0.1.0-full / latest-full) ──

FROM alpine:3.23 AS full

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/ruciod /usr/bin/ruciod
COPY --from=bins /usr/bin/rucio  /usr/bin/rucio

USER rucio
WORKDIR /var/lib/rucio

EXPOSE 4321/tcp
EXPOSE 7070/tcp

ENTRYPOINT ["/usr/bin/ruciod"]
