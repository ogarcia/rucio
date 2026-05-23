# syntax=docker/dockerfile:1.7
#
# Two-stage build producing two independent runtime images:
#
#   ruciod-only  →  tag "master" / "vX.Y.Z" / "latest"
#                   Only the daemon binary.  Use this for production
#                   nodes, bootstrap servers and VPS deployments.
#
#   full         →  tag "master-full" / "vX.Y.Z-full" / "latest-full"
#                   Both ruciod (daemon) and rucio (CLI).  Use this for
#                   development, debugging, or when you want to inspect
#                   a running node from inside the container.
#
# Build notes:
#   - Both images compile against musl so the result is a fully static
#     binary with no glibc runtime dependency.
#   - SQLite is compiled in via the bundled feature of sqlx; no system
#     libsqlite3 needed at runtime.
#   - TLS is handled by rustls (pure Rust); ca-certificates is included
#     in the runtime image only as a courtesy for future HTTPS calls
#     and can be dropped if not needed.
#   - The `rucio` user runs with a fixed UID/GID (10001) so that
#     bind-mounted volumes keep consistent ownership across rebuilds.
#
# Environment variables (runtime):
#   RUCIOD_CONFIG   Path to the daemon config file
#                   (default: $XDG_CONFIG_HOME/rucio/config.toml)
#   RUCIO_API       Daemon API URL used by the rucio CLI
#                   (default: http://127.0.0.1:7070)

# ── Stage 1: compile ────────────────────────────────────────────────────────

FROM rust:1-alpine3.23 AS builder

# musl-dev: C headers needed by ring (pulled in by rustls via reqwest).
# Everything else in the dependency tree is pure Rust.
RUN apk add --no-cache musl-dev

WORKDIR /app

# Copy workspace manifests first so dependency layers are cached
# independently of source changes.
COPY Cargo.toml Cargo.lock ./
COPY rucio-core/Cargo.toml   rucio-core/Cargo.toml
COPY rucio-daemon/Cargo.toml rucio-daemon/Cargo.toml
COPY rucio-cli/Cargo.toml    rucio-cli/Cargo.toml
COPY rucio/Cargo.toml        rucio/Cargo.toml

# Stub out every lib/bin so `cargo build` can resolve and cache all
# dependencies without needing the real source.
RUN mkdir -p rucio-core/src rucio-daemon/src rucio-cli/src rucio/src && \
    echo 'pub fn _stub() {}' > rucio-core/src/lib.rs && \
    echo 'fn main() {}' > rucio-daemon/src/main.rs && \
    echo 'pub fn _stub() {}' > rucio-daemon/src/lib.rs && \
    echo 'fn main() {}' > rucio-cli/src/main.rs && \
    echo 'pub fn _stub() {}' > rucio-cli/src/lib.rs && \
    echo 'fn main() {}' > rucio/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked 2>/dev/null || true

# Now copy the real source and do the final build.
COPY rucio-core/src   rucio-core/src
COPY rucio-daemon/src rucio-daemon/src
COPY rucio-cli/src    rucio-cli/src
COPY rucio/src        rucio/src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/ruciod /usr/local/bin/ruciod && \
    cp target/release/rucio  /usr/local/bin/rucio  && \
    strip /usr/local/bin/ruciod /usr/local/bin/rucio

# ── Stage 2: runtime – daemon only (tag: master / vX.Y.Z / latest) ──────────

FROM alpine:3.23 AS ruciod

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 rucio && \
    mkdir -p /etc/rucio /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio /etc/rucio

COPY --from=builder /usr/local/bin/ruciod /usr/local/bin/ruciod

USER rucio
WORKDIR /var/lib/rucio

# RUCIOD_CONFIG lets operators inject a config file via a bind-mount
# or a ConfigMap without rebuilding the image.
ENV RUCIOD_CONFIG=/etc/rucio/config.toml

EXPOSE 4321/tcp
EXPOSE 7070/tcp

ENTRYPOINT ["/usr/local/bin/ruciod"]

# ── Stage 3: runtime – full (tag: master-full / vX.Y.Z-full / latest-full) ──

FROM alpine:3.23 AS full

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser  -S -G rucio -u 10001 rucio && \
    mkdir -p /etc/rucio /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio /etc/rucio

COPY --from=builder /usr/local/bin/ruciod /usr/local/bin/ruciod
COPY --from=builder /usr/local/bin/rucio  /usr/local/bin/rucio

USER rucio
WORKDIR /var/lib/rucio

ENV RUCIOD_CONFIG=/etc/rucio/config.toml \
    RUCIO_API=http://127.0.0.1:7070

EXPOSE 4321/tcp
EXPOSE 7070/tcp

ENTRYPOINT ["/usr/local/bin/ruciod"]
