# syntax=docker/dockerfile:1.7
#
# Produces three independent runtime images:
#
#   ruciod     →  tag "master" / "0.1.0" / "latest"
#                 Standalone daemon binary only.  Use this for production nodes,
#                 bootstrap servers and VPS deployments.
#
#   full       →  tag "master-full" / "0.1.0-full" / "latest-full"
#                 Fat binary (rucio) + ruciod symlink.  Use this for
#                 development, debugging, or inspecting a running node.
#
#   bootstrap  →  tag "master-bootstrap" / "0.1.0-bootstrap" / "latest-bootstrap"
#                 rucio-bootstrap compiled with --features indexer.
#                 Stable DHT entry point + optional passive DHT indexer with
#                 REST search API.  Indexer is off by default; enable with
#                 indexer.enabled = true in the config or --index flag.
#
# Local build (compiles from source):
#   podman build --target ruciod     -t rucio:dev            .
#   podman build --target full       -t rucio:dev-full       .
#   podman build --target bootstrap  -t rucio-bootstrap:dev  .
#
# CI build (binaries pre-compiled and placed in dist/):
#   podman build --target ruciod     --build-arg BUILDER=prebuilt .
#   podman build --target full       --build-arg BUILDER=prebuilt .
#   podman build --target bootstrap  --build-arg BUILDER=prebuilt .
#
# Environment variables (runtime):
#   RUCIOD_CONFIG              Path to the daemon config file — optional, defaults to
#                              $HOME/.config/rucio/config.toml (/var/lib/rucio/.config/…)
#   RUCIO_API                  Daemon API URL used by the rucio CLI
#                              (default: http://127.0.0.1:7070)
#   RUCIOD_KAD_PORT            Kad2 UDP port for eMule network (default: 4672).
#                              Must be mapped on the host: -p 4672:4672/udp
#   RUCIO_BOOTSTRAP_CONFIG     Path to the bootstrap config file — optional, defaults to
#                              /var/lib/rucio/.config/rucio-bootstrap/config.toml
#   RUCIO_BOOTSTRAP_API_LISTEN Indexer REST API bind address (default: 127.0.0.1:8090)
#   RUCIO_BOOTSTRAP_LOG        Log filter for rucio-bootstrap (default: info)

# ── Stage 1a: compile from source (default local path) ──────────────────────

# ARG declared before the first FROM so it is available as a global
# build argument. Default is 'builder' (local compile); CI passes
# --build-arg BUILDER=prebuilt to skip compilation and use dist/.
ARG BUILDER=builder

FROM docker.io/rust:1-alpine3.23 AS builder

# musl-dev: C headers needed by ring (pulled in by rustls via reqwest).
# Everything else in the dependency tree is pure Rust.
RUN apk add --no-cache musl-dev

WORKDIR /app
COPY . .

RUN cargo build --release --locked --features emule-compat,indexer && \
    cp target/release/ruciod          /usr/bin/ruciod          && \
    cp target/release/rucio           /usr/bin/rucio           && \
    cp target/release/rucio-bootstrap /usr/bin/rucio-bootstrap && \
    strip /usr/bin/ruciod /usr/bin/rucio /usr/bin/rucio-bootstrap

# ── Stage 1b: pre-built binaries injected from CI (dist/ in build context) ───

FROM scratch AS prebuilt
COPY --chmod=0755 dist/ruciod          /usr/bin/ruciod
COPY --chmod=0755 dist/rucio           /usr/bin/rucio
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

EXPOSE 4321/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 3: runtime – full (tag: master-full / 0.1.0-full / latest-full) ────

FROM docker.io/alpine:3.23 AS full

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/rucio /usr/bin/rucio
RUN ln -s /usr/bin/rucio /usr/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

EXPOSE 4321/tcp
EXPOSE 7070/tcp
# Kad2 UDP port for eMule network (emule-compat builds).
# Map with: -p 4672:4672/udp
EXPOSE 4672/udp

ENTRYPOINT ["/usr/bin/ruciod"]

# ── Stage 4: runtime – bootstrap node (tag: master-bootstrap / …-bootstrap) ──

FROM docker.io/alpine:3.23 AS bootstrap

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 -h /var/lib/rucio rucio && \
    mkdir -p /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=bins /usr/bin/rucio-bootstrap /usr/bin/rucio-bootstrap

USER rucio
WORKDIR /var/lib/rucio

# DHT port (primary Kademlia identity).
EXPOSE 4321/tcp
# Indexer REST API (active when indexer.enabled = true in the config).
# Map with: -p 8090:8090/tcp
EXPOSE 8090/tcp

ENTRYPOINT ["/usr/bin/rucio-bootstrap"]
