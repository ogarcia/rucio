# syntax=docker/dockerfile:1.7
#
# Produces four independent runtime images:
#
#   ruciod     →  tag "master" / "0.1.0" / "latest"
#                 Standalone daemon.  No web panel.  Minimal footprint.
#                 Use this for production nodes, bootstrap servers and VPS
#                 deployments where the web UI is not needed.
#
#   web        →  tag "master-web" / "0.1.0-web" / "latest-web"
#                 Daemon with embedded web control panel (ruciod + web-ui).
#                 The panel is served at http://<host>:3003/ in addition to
#                 the REST API.  Use this for single-host deployments where a
#                 browser UI is convenient.
#
#   full       →  tag "master-full" / "0.1.0-full" / "latest-full"
#                 Fat binary (rucio) + ruciod symlink + web panel.  Includes
#                 the rucio CLI so you can exec into the container and run
#                 commands.  Use this for development and debugging.
#
#   bootstrap  →  tag "master-bootstrap" / "0.1.0-bootstrap" / "latest-bootstrap"
#                 rucio-bootstrap compiled with --features indexer.
#                 Stable DHT entry point + optional passive DHT indexer with
#                 REST search API.  Indexer is off by default; enable with
#                 indexer.enabled = true in the config or --index flag.
#
# Local build (compiles from source):
#   podman build --target ruciod     -t rucio:dev            .
#   podman build --target web        -t rucio:dev-web        .
#   podman build --target full       -t rucio:dev-full       .
#   podman build --target bootstrap  -t rucio-bootstrap:dev  .
#
# CI build (binaries pre-compiled and placed in dist/):
#   podman build --target ruciod     --build-arg BUILDER=prebuilt .
#   podman build --target web        --build-arg BUILDER=prebuilt .
#   podman build --target full       --build-arg BUILDER=prebuilt .
#   podman build --target bootstrap  --build-arg BUILDER=prebuilt .
#
# Environment variables (runtime):
#   RUCIOD_CONFIG              Path to the daemon config file — optional, defaults to
#                              $HOME/.config/rucio/config.toml (/var/lib/rucio/.config/…)
#   RUCIO_API                  Daemon API URL used by the rucio CLI
#                              (default: http://127.0.0.1:3003)
#   RUCIOD_KAD_PORT            Kad2 UDP port for eMule network (default: 4672).
#                              Must be mapped on the host: -p 4672:4672/udp
#   RUCIO_BOOTSTRAP_CONFIG     Path to the bootstrap config file — optional, defaults to
#                              /var/lib/rucio/.config/rucio-bootstrap/config.toml
#   RUCIO_BOOTSTRAP_API_LISTEN Indexer REST API bind address (default: 0.0.0.0:3003)
#   RUCIO_BOOTSTRAP_LOG        Log filter for rucio-bootstrap (default: info)

# ── Stage 1a: compile from source (default local path) ──────────────────────

# ARG declared before the first FROM so it is available as a global
# build argument. Default is 'builder' (local compile); CI passes
# --build-arg BUILDER=prebuilt to skip compilation and use dist/.
ARG BUILDER=builder

# trunk version used to build the Leptos WASM frontend.
# Override at build time if you need a different version:
#   podman build --build-arg TRUNK_VERSION=x.y.z ...
ARG TRUNK_VERSION=0.21.14

FROM docker.io/rust:1-alpine3.23 AS builder

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
# in the cargo build step below via the web-ui feature / rust-embed.
RUN cd rucio-web && trunk build --release

# Build all binaries without the web panel.  rucio-daemon and rucio are also
# compiled again with --features web-ui afterwards; rucio-bootstrap is only
# needed here.
RUN cargo build --release --locked --features emule-compat,indexer && \
    cp target/release/ruciod          /usr/bin/ruciod          && \
    cp target/release/rucio           /usr/bin/rucio           && \
    cp target/release/rucio-bootstrap /usr/bin/rucio-bootstrap && \
    strip /usr/bin/ruciod /usr/bin/rucio /usr/bin/rucio-bootstrap && \
    # web-ui variants — only rucio-daemon and rucio recompile (incremental)
    cargo build --release --locked -p rucio-daemon -p rucio \
        --features emule-compat,web-ui && \
    cp target/release/ruciod /usr/bin/ruciod-web && \
    cp target/release/rucio  /usr/bin/rucio-fat-web && \
    strip /usr/bin/ruciod-web /usr/bin/rucio-fat-web

# ── Stage 1b: pre-built binaries injected from CI (dist/ in build context) ───

FROM scratch AS prebuilt
COPY --chmod=0755 dist/ruciod          /usr/bin/ruciod
COPY --chmod=0755 dist/ruciod-web      /usr/bin/ruciod-web
COPY --chmod=0755 dist/rucio           /usr/bin/rucio
COPY --chmod=0755 dist/rucio-fat-web   /usr/bin/rucio-fat-web
COPY --chmod=0755 dist/rucio-bootstrap /usr/bin/rucio-bootstrap

# ── Stage 1c: indirection — points to 'builder' or 'prebuilt' via build arg ──

FROM ${BUILDER} AS bins

# ── Stage 2: runtime – daemon only (tag: master / 0.1.0 / latest) ────────────

FROM docker.io/alpine:3.23 AS ruciod

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/ruciod /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIOD_API_LISTEN=0.0.0.0:3003

EXPOSE 4321/tcp
EXPOSE 3003/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 3: runtime – daemon + web panel (tag: master-web / 0.1.0-web) ──────

FROM docker.io/alpine:3.23 AS web

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/ruciod-web /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIOD_API_LISTEN=0.0.0.0:3003

EXPOSE 4321/tcp
# REST API and web control panel — http://<host>:3003/
EXPOSE 3003/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 4: runtime – full (tag: master-full / 0.1.0-full / latest-full) ────

FROM docker.io/alpine:3.23 AS full

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/rucio-fat-web /usr/bin/rucio
RUN ln -s /usr/bin/rucio /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIOD_API_LISTEN=0.0.0.0:3003

EXPOSE 4321/tcp
# REST API and web control panel — http://<host>:3003/
EXPOSE 3003/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 5: runtime – bootstrap node (tag: master-bootstrap / …-bootstrap) ──

FROM docker.io/alpine:3.23 AS bootstrap

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
# Indexer REST API (active when indexer.enabled = true in the config).
# Map with: -p 3003:3003/tcp
EXPOSE 3003/tcp

ENTRYPOINT ["/usr/bin/rucio-bootstrap"]
