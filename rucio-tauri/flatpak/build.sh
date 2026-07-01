#!/usr/bin/env bash
# Build the release binary the Flatpak manifest packages.
#
# Mirrors the CI desktop build: first `trunk build` produces the web UI that the
# embedded daemon serves (rust-embed reads rucio-web/dist), then the Tauri shell
# is compiled. The manifest (me.ogarcia.rucio.yml) then picks up the resulting
# target/release/rucio-tauri binary — no `cargo tauri` / .deb step needed.
#
# Prerequisites: a Rust toolchain, the wasm32-unknown-unknown target, `trunk`,
# and the WebKitGTK development libraries (to link the Tauri shell). See
# README.md.
set -euo pipefail

# Repo root, regardless of where this is invoked from.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo ">> Building the web UI (trunk)"
( cd rucio-web && trunk build --release --public-url ./ )

echo ">> Building the Rucio desktop shell (release)"
cargo build --release -p rucio-tauri

echo ">> Done: target/release/rucio-tauri"
echo "   Now: flatpak-builder --user --install --force-clean build-dir rucio-tauri/flatpak/me.ogarcia.rucio.yml"
