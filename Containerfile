# syntax=docker/dockerfile:1.25
#
# Produces three runtime images:
#
#   complete   →  tag "master" / "0.1.0" / "latest"
#                 The full client: fat `rucio` binary (daemon + CLI) with the
#                 embedded web panel and eMule support. The web UI is served at
#                 http://<host>:3003/ alongside the REST API, and you can exec
#                 in to run `rucio` CLI commands. This is the default image.
#
#   headless   →  tag "master-headless" / "0.1.0-headless" / "latest-headless"
#                 Daemon only (`ruciod`), no web panel, no CLI. Smallest
#                 footprint, for servers/VPS controlled via the API.
#
#   cli        →  tag "master-cli" / "0.1.0-cli" / "latest-cli"
#                 Standalone `rucio-cli` client only (no daemon, no libp2p) —
#                 a tiny image to drive a remote daemon over its REST API.
#                 Point it at the daemon with RUCIO_API.
#
#   bootstrap  →  tag "master-bootstrap" / "0.1.0-bootstrap" / "latest-bootstrap"
#                 rucio-bootstrap compiled with --features indexer: a stable DHT
#                 entry point plus the passive DHT indexer (REST search API).
#                 The indexer runs by default; disable it with --no-index.
#
# Local build (compiles from source):
#   podman build --target complete   -t rucio:dev            .
#   podman build --target headless   -t rucio:dev-headless   .
#   podman build --target cli        -t rucio:dev-cli        .
#   podman build --target bootstrap  -t rucio-bootstrap:dev  .
#
# CI build (binaries pre-compiled and placed in dist/<arch>/, arch = amd64|arm64
# to match Docker's automatic TARGETARCH; buildx picks the right subdir per
# platform, so the same context yields a multi-arch manifest):
#   podman build --target complete   --build-arg BUILDER=prebuilt .
#   podman build --target headless   --build-arg BUILDER=prebuilt .
#   podman build --target cli        --build-arg BUILDER=prebuilt .
#   podman build --target bootstrap  --build-arg BUILDER=prebuilt .
#
# Environment variables (runtime):
#   RUCIOD_CONFIG              Path to the daemon config file — optional, defaults to
#                              $HOME/.config/rucio/config.toml (/var/lib/rucio/.config/…)
#   RUCIO_API                  Daemon API URL used by the rucio CLI
#                              (default: http://127.0.0.1:3003)
#   RUCIOD_KAD_PORT            Kad2 UDP port for eMule network (default: 4672).
#                              Must be mapped on the host: -p 4672:4672/udp
#   RUCIOD_UPNP                Enable UPnP port mapping (default: false in the
#                              container; set to true only with --network=host)
#   RUCIO_BOOTSTRAP_CONFIG     Path to the bootstrap config file — optional, defaults to
#                              /var/lib/rucio/.config/rucio-bootstrap/config.toml
#   RUCIO_BOOTSTRAP_API_LISTEN Indexer REST API bind address (default: 0.0.0.0:3003)
#   RUCIO_BOOTSTRAP_LOG        Log filter for rucio-bootstrap (default: info)

# ── Stage 1a: compile from source (default local path) ──────────────────────

# ARG declared before the first FROM so it is available as a global
# build argument. Default is 'builder' (local compile); CI passes
# --build-arg BUILDER=prebuilt to skip compilation and use dist/.
ARG BUILDER=builder

# Base-image and build-tool versions. Defaults mirror versions.env (the single
# source of truth); CI overrides them with --build-arg from that file. Declared
# before the first FROM so they apply to every FROM below. Override locally with
# e.g. --build-arg TRUNK_VERSION=x.y.z.
ARG ALPINE_VERSION=3.24
ARG RUST_VERSION=1
ARG TRUNK_VERSION=0.21.14

FROM docker.io/rust:${RUST_VERSION}-alpine${ALPINE_VERSION} AS builder

ARG TRUNK_VERSION

# musl-dev: C headers needed by ring (pulled in by rustls via reqwest).
# wget: needed to download the trunk binary.
RUN apk add --no-cache musl-dev wget && \
    rustup target add wasm32-unknown-unknown && \
    wget -qO- "https://github.com/trunk-rs/trunk/releases/download/v${TRUNK_VERSION}/trunk-$(uname -m)-unknown-linux-musl.tar.gz" \
        | tar xz -C /usr/local/bin/

WORKDIR /app
COPY . .

# Build the Leptos frontend to WASM.  dist/ is produced here and consumed
# by the web-ui build below via rust-embed.  --public-url ./ emits relative
# asset paths so the served <base href> can relocate the app under a subpath.
RUN cd rucio-web && trunk build --release --public-url ./

# First pass (no web-ui): the headless daemon and the bootstrap binary.
# Second pass recompiles only the fat `rucio` with the embedded web panel.
# The first pass builds the workspace default-members, which already includes
# the standalone rucio-cli — just copy it out alongside the daemon binaries.
RUN cargo build --release --locked --features emule-compat,indexer && \
    cp target/release/ruciod          /usr/bin/ruciod          && \
    cp target/release/rucio-bootstrap /usr/bin/rucio-bootstrap && \
    cp target/release/rucio-cli       /usr/bin/rucio-cli       && \
    strip /usr/bin/ruciod /usr/bin/rucio-bootstrap /usr/bin/rucio-cli && \
    # complete: fat binary with eMule + embedded web panel (incremental)
    cargo build --release --locked -p rucio --features emule-compat,web-ui && \
    cp target/release/rucio /usr/bin/rucio && \
    strip /usr/bin/rucio

# ── Stage 1b: pre-built binaries injected from CI (dist/ in build context) ───

# TARGETARCH (amd64/arm64) is set automatically by buildx per target platform;
# it must be re-declared here to be usable in COPY. The dist/<arch>/ layout is
# produced by the container.yml build matrix.
FROM scratch AS prebuilt
ARG TARGETARCH
COPY --chmod=0755 dist/${TARGETARCH}/ruciod          /usr/bin/ruciod
COPY --chmod=0755 dist/${TARGETARCH}/rucio           /usr/bin/rucio
COPY --chmod=0755 dist/${TARGETARCH}/rucio-cli       /usr/bin/rucio-cli
COPY --chmod=0755 dist/${TARGETARCH}/rucio-bootstrap /usr/bin/rucio-bootstrap

# ── Stage 1c: indirection — points to 'builder' or 'prebuilt' via build arg ──

FROM ${BUILDER} AS bins

# ── Stage 2: runtime – complete (tag: master / 0.1.0 / latest) ───────────────

FROM docker.io/alpine:${ALPINE_VERSION} AS complete

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

# Fat binary: invoked as `ruciod` it starts the daemon; invoked as `rucio` it
# runs the CLI. Both names are available via the symlink.
COPY --from=bins /usr/bin/rucio /usr/bin/rucio
RUN ln -s /usr/bin/rucio /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIOD_API_LISTEN=0.0.0.0:3003
# UPnP rarely works from inside a container (the daemon can't see the LAN
# gateway), and trying just adds startup noise. Off by default here; set
# RUCIOD_UPNP=true if you run with --network=host on a UPnP-capable router.
ENV RUCIOD_UPNP=false

EXPOSE 4321/tcp
# REST API and web control panel — http://<host>:3003/
EXPOSE 3003/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 3: runtime – headless daemon (tag: …-headless) ─────────────────────

FROM docker.io/alpine:${ALPINE_VERSION} AS headless

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/ruciod /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIOD_API_LISTEN=0.0.0.0:3003
# UPnP rarely works from inside a container (the daemon can't see the LAN
# gateway), and trying just adds startup noise. Off by default here; set
# RUCIOD_UPNP=true if you run with --network=host on a UPnP-capable router.
ENV RUCIOD_UPNP=false

EXPOSE 4321/tcp
EXPOSE 3003/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 4: runtime – standalone CLI client (tag: …-cli) ────────────────────

FROM docker.io/alpine:${ALPINE_VERSION} AS cli

RUN apk add --no-cache ca-certificates

# Installed as `rucio` so usage matches the docs: `docker run …:latest-cli …`.
COPY --from=bins /usr/bin/rucio-cli /usr/bin/rucio

# Target a remote daemon by overriding this (or pass --api on each command).
ENV RUCIO_API=http://127.0.0.1:3003

ENTRYPOINT ["/usr/bin/rucio"]

# ── Stage 5: runtime – bootstrap node (tag: …-bootstrap) ─────────────────────

FROM docker.io/alpine:${ALPINE_VERSION} AS bootstrap

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/rucio-bootstrap /usr/bin/rucio-bootstrap

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIO_BOOTSTRAP_API_LISTEN=0.0.0.0:3003

# DHT port (primary Kademlia identity).
EXPOSE 4321/tcp
# Indexer REST API (runs by default; disable with --no-index).
EXPOSE 3003/tcp

ENTRYPOINT ["/usr/bin/rucio-bootstrap"]
