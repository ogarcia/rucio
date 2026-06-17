#!/usr/bin/env bash
# test-e2e.sh — End-to-end tests for Rucio (BLAKE3 verified streaming).
#
# Spins up isolated daemons on loopback and exercises the native (libp2p)
# transfer path end to end. Scenarios:
#
#   1. Basic transfer — node A shares a file, node B searches for it, downloads
#      it, and the content is verified. Also asserts the root hash IS the file's
#      canonical BLAKE3 (the bao tree root).
#   2. Partial sharing — a node serves verified chunks straight from its `.part`
#      (via the bao `.part.obao` slice path) while it is still downloading.
#   3. Resumption — a download interrupted (SIGKILL) mid-flight resumes from its
#      `.part` + `.part.obao` with its verified chunks intact (no restart from
#      zero). Driving the rest to completion needs DHT provider rediscovery,
#      which a single-host loopback swarm can't form reliably — see the note in
#      the scenario; that leg is covered by manual / real-network testing.
#
# Usage:
#   bash tests/e2e/test-e2e.sh        (from workspace root)
#   bash test-e2e.sh                   (from tests/e2e/)
#
# Requirements: curl, jq, b3sum

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUCIOD="$WORKSPACE_ROOT/target/debug/ruciod"

CHUNK=$((4 * 1024 * 1024))      # 4 MiB transfer chunk
FILE_MIB=20                     # test file size — spans 5 chunks
# Download throttle (KB/s) for the nodes we need to catch mid-transfer. Tuned so
# a single chunk's rate-limit wait (~2s for 4 MiB at 2 MB/s) stays well under the
# manifest/chunk request timeout — the throttle is awaited on the engine loop, so
# too low a rate makes a busy downloader briefly unresponsive to other peers.
SLOW_KBPS=2000

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✓${NC} $*"; }
fail() { echo -e "${RED}✗${NC} $*"; exit 1; }
info() { echo -e "${YELLOW}→${NC} $*"; }
section() { echo -e "\n${YELLOW}══ $* ══${NC}"; }

# ---------------------------------------------------------------------------
# 0. Pre-checks + build
# ---------------------------------------------------------------------------
command -v curl  >/dev/null || fail "curl is not installed"
command -v jq    >/dev/null || fail "jq is not installed"
command -v b3sum >/dev/null || fail "b3sum is not installed (BLAKE3 root-hash check)"

info "Building binaries..."
cargo build --manifest-path "$WORKSPACE_ROOT/Cargo.toml" -p rucio-daemon -p rucio-cli --quiet
[[ -f "$RUCIOD" ]] || fail "ruciod not found at $RUCIOD"

TEST_DIR=$(mktemp -d /tmp/rucio-XXXXXX)
info "Test workspace: $TEST_DIR"

declare -a ALL_PIDS=()

cleanup() {
    info "Stopping daemons..."
    for pid in "${ALL_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
    echo ""
    info "Logs and artefacts are in: $TEST_DIR"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# write_config <name> <api_port> <p2p_port> [dl_limit_kbps] [bootstrap_multiaddr]
write_config() {
    local name=$1 api=$2 p2p=$3 dl=${4:-0} boot=${5:-}
    local base="$TEST_DIR/$name"
    local boot_line="bootstrap_peers     = []"
    [[ -n "$boot" ]] && boot_line="bootstrap_peers     = [\"$boot\"]"
    mkdir -p "$base/data" "$base/downloads" "$base/temp" "$base/share"
    cat > "$base/config.toml" <<EOF
[node]
identity_path = "$base/identity.key"
listen_addrs  = ["/ip4/0.0.0.0/tcp/$p2p"]

[api]
listen = "127.0.0.1:$api"

[storage]
download_dir  = "$base/downloads"
temp_dir      = "$base/temp"
database_path = "$base/data/rucio.db"

[network]
$boot_line
exclusive_bootstrap = true
download_limit_kbps = $dl
EOF
    printf -v "${name//-/_}_API" '%s' "http://127.0.0.1:$api"
}

# launch <name> [log_suffix] — (re)launch the daemon from its existing config.
launch() {
    local name=$1 suffix=${2:-}
    local base="$TEST_DIR/$name"
    "$RUCIOD" --config "$base/config.toml" > "$base/daemon${suffix}.log" 2>&1 &
    local pid=$!
    ALL_PIDS+=("$pid")
    printf -v "${name//-/_}_PID" '%s' "$pid"
}

# wait_api <api_url> <name>
wait_api() {
    local url=$1 name=$2
    for _ in $(seq 1 40); do
        curl -sf "$url/api/v1/status" >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    fail "$name did not start in time (check $TEST_DIR/$name/daemon.log)"
}

# peer_id <api_url>
peer_id() { curl -sf "$1/api/v1/status" | jq -r '.peer_id'; }

# wait_peers <api_url> <name> — wait until at least one peer is discovered.
wait_peers() {
    local url=$1 name=$2
    for _ in $(seq 1 30); do
        [[ "$(curl -sf "$url/api/v1/peers" | jq '.peers | length')" -ge 1 ]] && return 0
        sleep 0.5
    done
    fail "$name discovered no peers via mDNS"
}

# start_download <api_url> <magnet> <providers_json>
start_download() {
    local code
    code=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$1/api/v1/downloads" \
        -H 'Content-Type: application/json' \
        -d "{\"magnet\": \"$2\", \"providers\": $3}")
    [[ "$code" == "202" ]] || fail "Download request rejected (HTTP $code)"
}

# dl_state / dl_bytes <api_url>; dl_id <api_url>; pieces_done <api_url> <id>
dl_state() { curl -sf "$1/api/v1/downloads" | jq -r '.downloads[0].state // "unknown"'; }
dl_bytes() { curl -sf "$1/api/v1/downloads" | jq -r '.downloads[0].bytes_done // 0'; }
dl_id()    { curl -sf "$1/api/v1/downloads" | jq -r '.downloads[0].id // empty'; }
pieces_done() { curl -sf "$1/api/v1/downloads/$2" | jq -r '.pieces_done // 0'; }

# wait_partial <api_url> <name> — block until at least one chunk is *verified*
# (pieces_done >= 1) but the download is not yet complete, so the node holds
# slice-able partial content. Gating on verified pieces (not bytes_done, which
# counts bytes received on the wire before verification) guarantees a resumed
# download has real progress to keep.
wait_partial() {
    local url=$1 name=$2 id st done
    for _ in $(seq 1 40); do id=$(dl_id "$url"); [[ -n "$id" ]] && break; sleep 0.25; done
    [[ -n "$id" ]] || fail "$name: no download row appeared"
    for _ in $(seq 1 120); do
        st=$(dl_state "$url"); done=$(pieces_done "$url" "$id")
        [[ "$st" == "Completed" ]] && fail "$name completed before we could catch it partial (throttle too high?)"
        [[ "$st" == "Failed" ]]    && fail "$name download failed"
        [[ "$done" -ge 1 ]] && return 0
        sleep 0.5
    done
    fail "$name never reached a partial state (>= 1 verified chunk)"
}

# wait_complete <api_url> <name>
wait_complete() {
    local url=$1 name=$2 st
    for _ in $(seq 1 240); do
        st=$(dl_state "$url")
        [[ "$st" == "Completed" ]] && return 0
        [[ "$st" == "Failed" ]] && fail "$name download failed (check $TEST_DIR/$name)"
        sleep 0.5
    done
    fail "$name download did not complete (last state: $(dl_state "$url"))"
}

# verify_download <name> — find the downloaded file and check its SHA-256.
verify_download() {
    local name=$1 f got
    f=$(find "$TEST_DIR/$name/downloads" -type f | head -n1)
    [[ -f "$f" ]] || fail "$name: downloaded file not found"
    got=$(sha256sum "$f" | cut -d' ' -f1)
    [[ "$got" == "$ORIGINAL_SHA256" ]] || fail "$name: SHA-256 mismatch — file corrupted!"
}

# served_chunks <api_url> — session chunks_served counter.
served_chunks() { curl -sf "$1/api/v1/metrics" | jq -r '.session.chunks_served // 0'; }

# ---------------------------------------------------------------------------
# Test file (lives in node-a's share dir; the share API registers directories)
# ---------------------------------------------------------------------------
write_config node-a 17070 14321
TEST_FILE="$TEST_DIR/node-a/share/test-file.bin"
info "Creating ${FILE_MIB} MiB test file..."
dd if=/dev/urandom of="$TEST_FILE" bs=1M count="$FILE_MIB" 2>/dev/null
ORIGINAL_SHA256=$(sha256sum "$TEST_FILE" | cut -d' ' -f1)
ORIGINAL_BLAKE3=$(b3sum --no-names "$TEST_FILE")
SIZE=$((FILE_MIB * 1024 * 1024))
info "SHA-256: $ORIGINAL_SHA256"
info "BLAKE3:  $ORIGINAL_BLAKE3"

# ===========================================================================
# Scenario 1 — basic transfer A → B
# ===========================================================================
section "Scenario 1: basic transfer (A shares, B downloads)"

# A is the root seeder. Every other node is bootstrapped off a known peer
# (B,C,E off A; D off C) rather than left to mDNS: an explicit connection forms
# the Gossipsub mesh and request-response links deterministically, which a
# freshly-started single-host swarm otherwise does flakily.
info "Starting node A (:17070)..."
launch node-a; wait_api "$node_a_API" node-a
PEER_A=$(peer_id "$node_a_API")
A_ADDR="/ip4/127.0.0.1/tcp/14321/p2p/$PEER_A"
info "Node A PeerId: $PEER_A"

info "Starting node B (:17071, bootstrapped off A)..."
write_config node-b 17071 14322 0 "$A_ADDR"
launch node-b; wait_api "$node_b_API" node-b
ok "Both nodes up"

info "Sharing test directory on node A..."
QUEUED=$(curl -sf -X POST "$node_a_API/api/v1/shares" \
    -H 'Content-Type: application/json' \
    -d "{\"path\": \"$TEST_DIR/node-a/share\"}" | jq -r '.queued')
[[ "$QUEUED" -ge 1 ]] || fail "Share request did not queue any files"

for _ in $(seq 1 20); do
    [[ "$(curl -sf "$node_a_API/api/v1/shares/files" | jq '.shares | length')" -ge 1 ]] && break
    sleep 0.5
done
SHARES=$(curl -sf "$node_a_API/api/v1/shares/files")
[[ "$(echo "$SHARES" | jq '.shares | length')" -ge 1 ]] || fail "File not indexed on node A"
ROOT_HASH=$(echo "$SHARES" | jq -r '.shares[0].root_hash')
ok "File indexed on node A (root $ROOT_HASH)"
[[ "$ROOT_HASH" == "$ORIGINAL_BLAKE3" ]] || \
    fail "Root hash != file BLAKE3 ($ROOT_HASH vs $ORIGINAL_BLAKE3)"
ok "Root hash equals the file's canonical BLAKE3"

# A clean magnet (no embedded provider) — providers are passed explicitly so
# each scenario controls exactly which peers a downloader is told about.
MAGNET="rucio:$ROOT_HASH?name=test-file.bin&size=$SIZE"

info "Waiting for mDNS discovery between A and B..."
wait_peers "$node_b_API" node-b
ok "Node B discovered the swarm"

info "Searching for 'test' on the Rucio network from node B..."
RESULT_COUNT=0; RESULTS="{}"
for attempt in $(seq 1 10); do
    QID=$(curl -sf -X POST "$node_b_API/api/v1/searches" \
        -H 'Content-Type: application/json' \
        -d '{"keywords": ["test"], "network": "rucio"}' | jq -r '.id')
    for _ in $(seq 1 6); do
        RESULTS=$(curl -sf "$node_b_API/api/v1/searches/$QID")
        RESULT_COUNT=$(echo "$RESULTS" | jq '.results | length')
        [[ "$RESULT_COUNT" -ge 1 ]] && break
        sleep 0.5
    done
    [[ "$RESULT_COUNT" -ge 1 ]] && break
    sleep 1
done
[[ "$RESULT_COUNT" -ge 1 ]] || fail "No search results on node B"
ok "Search returned $RESULT_COUNT result(s)"
SEARCH_LINK=$(echo "$RESULTS" | jq -r --arg h "$ROOT_HASH" \
    '.results[] | select(.download_link | startswith("rucio:" + $h)) | .download_link' | head -n1)
[[ -n "$SEARCH_LINK" ]] || fail "Search results did not include our shared file"
ok "Search found our file by its root hash"

info "Downloading on node B (provider A)..."
start_download "$node_b_API" "$MAGNET" "[\"$PEER_A\"]"
ok "Download queued"
wait_complete "$node_b_API" node-b
verify_download node-b
ok "Node B download completed and SHA-256 verified"

# ===========================================================================
# Scenario 2 — partial sharing (C serves from its .part while downloading)
# ===========================================================================
section "Scenario 2: partial sharing (D pulls from a still-downloading C)"

# C downloads from A, throttled, so it stays partial long enough to serve.
write_config node-c 17072 14323 "$SLOW_KBPS" "$A_ADDR"
launch node-c; wait_api "$node_c_API" node-c
wait_peers "$node_c_API" node-c
PEER_C=$(peer_id "$node_c_API")
C_ADDR="/ip4/127.0.0.1/tcp/14323/p2p/$PEER_C"
info "Node C PeerId: $PEER_C (throttled ${SLOW_KBPS} KB/s)"

info "Starting throttled download on node C (provider A)..."
start_download "$node_c_API" "$MAGNET" "[\"$PEER_A\"]"
wait_partial "$node_c_API" node-c
ok "Node C holds verified partial content (>= 1 chunk, still downloading)"

# D is told ONLY about C (provider) and bootstrapped off C, so it reliably has
# C's address to fetch the manifest (served straight from C's in-progress
# download row) and pull the chunks C already holds from its .part — exercising
# the partial-serve path. Once C finishes it hands D the rest.
write_config node-d 17073 14324 0 "$C_ADDR"
launch node-d; wait_api "$node_d_API" node-d
wait_peers "$node_d_API" node-d
info "Starting download on node D (provider C only)..."
start_download "$node_d_API" "$MAGNET" "[\"$PEER_C\"]"
wait_complete "$node_d_API" node-d
verify_download node-d
ok "Node D download completed and SHA-256 verified"

C_SERVED=$(served_chunks "$node_c_API")
[[ "$C_SERVED" -ge 1 ]] || \
    fail "Node C served no chunks — partial sharing path did not run"
ok "Node C served $C_SERVED chunk(s) while still downloading (partial sharing)"

# ===========================================================================
# Scenario 3 — resumption (interrupt mid-download, restart, resume from .part)
# ===========================================================================
section "Scenario 3: resumption (SIGKILL mid-download, restart, resume from disk)"

# This asserts the migration's deterministic guarantee: an interrupted download
# resumes from its persisted `.part` + `.part.obao`, with its already-verified
# chunks intact (it does NOT re-download from zero). Driving the *remaining*
# chunks to completion afterwards needs provider rediscovery via the Kademlia
# DHT, which does not form reliably between LowId nodes on a single loopback
# host — that leg is covered by manual / real-network testing, not asserted here.
write_config node-e 17074 14325 "$SLOW_KBPS" "$A_ADDR"
launch node-e; wait_api "$node_e_API" node-e
wait_peers "$node_e_API" node-e
info "Starting throttled download on node E (provider A)..."
start_download "$node_e_API" "$MAGNET" "[\"$PEER_A\"]"
wait_partial "$node_e_API" node-e
ok "Node E reached partial state (>= 1 verified chunk on disk)"

info "Killing node E (SIGKILL) mid-download..."
kill -9 "$node_e_PID" 2>/dev/null || true
wait "$node_e_PID" 2>/dev/null || true
PART_OBAO=$(find "$TEST_DIR/node-e/temp" -name '*.part.obao' | head -n1)
[[ -f "$PART_OBAO" ]] || fail "No .part.obao sidecar on disk after interrupt"
[[ -s "$PART_OBAO" ]] || fail ".part.obao sidecar is empty"
ok "Partial outboard sidecar persisted: $(basename "$PART_OBAO")"

info "Restarting node E..."
launch node-e .restart
wait_api "$node_e_API" node-e
sleep 2  # let resume_interrupted run
# Strip ANSI colour codes the tracing logger embeds, so `done=N` matches.
RESUME_LINE=$(grep "Download resumed" "$TEST_DIR/node-e/daemon.restart.log" \
    | head -n1 | sed 's/\x1b\[[0-9;]*m//g')
[[ -n "$RESUME_LINE" ]] || \
    fail "Node E did not resume the interrupted download (expected a resume log line)"
# The resume log reports done=N/total — N must be >= 1 (verified chunks kept).
echo "$RESUME_LINE" | grep -qE 'done=[1-9]' || \
    fail "Node E resumed with zero verified chunks — it restarted from scratch"
ok "Node E resumed from its .part with verified chunks intact (no restart from zero)"

# Progress is preserved across the restart: the API still reports the bytes that
# were already verified (>= one full chunk), not a reset to zero.
RESUMED_BYTES=$(dl_bytes "$node_e_API")
[[ "$RESUMED_BYTES" -gt 0 ]] || \
    fail "Resumed download lost its progress (bytes_done reset to 0)"
ok "Resumed download preserved its progress ($RESUMED_BYTES bytes on the books)"

# ===========================================================================
echo ""
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo -e "${GREEN}  All scenarios passed — end-to-end OK    ${NC}"
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo ""
info "Artefacts kept at: $TEST_DIR"
