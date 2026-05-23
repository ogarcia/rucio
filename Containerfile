# syntax=docker/dockerfile:1.7
#
# Two-stage build. The builder compiles against musl in alpine; the
# runtime ships just the static binary on top of a minimal alpine.
# The result is a ~15-20 MB image with no glibc or OpenSSL runtime
# dependency — every C bit (libsqlite3, etc.) is bundled and every
# TLS bit (rustls) is pure Rust.
#
# Pinning notes:
#   * Alpine is pinned to 3.23 in both stages so a new base image
#     never silently changes the runtime.
#   * Rust is left floating within the 1.x channel, mirroring the
#     `dtolnay/rust-toolchain@stable` posture of the CI workflow.
#     Switch to `rust:1.<N>-alpine3.23` if bit-exact reproducibility
#     of the toolchain is required.

FROM rust:1-alpine3.23 AS builder

# musl-dev provides the C toolchain headers ring (the rustls-tls
# backend pulled in by reqwest) needs during its compile path;
# everything else in the dependency tree is pure Rust.
RUN apk add --no-cache musl-dev

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp target/release/rucio /usr/bin/rucio && \
    strip /usr/bin/rucio

# ----------------------------------------------------------------------

FROM alpine:3.23

# ca-certificates: HTTPS ¿needed?. The rucio user is
# created with a fixed UID/GID so that bind-mounted volumes keep the
# same ownership across rebuilds and across base-image bumps that would
# otherwise reshuffle the default system-user counter.
RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 rucio && \
    adduser -S -G rucio -u 10001 rucio && \
    mkdir -p /etc/rucio /var/lib/rucio && \
    chown -R rucio:rucio /var/lib/rucio

COPY --from=builder /usr/bin/rucio /usr/bin/rucio
COPY --chmod=0755 container/entrypoint.sh /usr/bin/rucio-entrypoint

USER rucio
WORKDIR /var/lib/rucio

ENV rucio_CONFIG=/etc/rucio/rucio.toml \
    MNEMO_SYNC_INTERVAL_SECS=3600

ENTRYPOINT ["/usr/bin/ruciod"]
