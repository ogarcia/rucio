#!/usr/bin/env bash
# test-e2e.sh — End-to-end test for Rucio
#
# Starts two daemon instances (A and B), shares a file on A,
# searches from B, downloads it, and verifies the SHA-256 hash.
#
# Usage:
#   bash tests/e2e/test-e2e.sh        (from workspace root)
#   bash test-e2e.sh                   (from tests/e2e/)
#
# Requirements:
#   - curl and jq installed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RUCIOD="$WORKSPACE_ROOT/target/debug/ruciod"
API_A="http://127.0.0.1:17070"
API_B="http://127.0.0.1:17071"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

ok()   { echo -e "${GREEN}✓${NC} $*"; }
fail() { echo -e "${RED}✗${NC} $*"; exit 1; }
info() { echo -e "${YELLOW}→${NC} $*"; }

# ---------------------------------------------------------------------------
# 0. Pre-checks
# ---------------------------------------------------------------------------
command -v curl >/dev/null || fail "curl is not installed"
command -v jq   >/dev/null || fail "jq is not installed"

info "Building binaries..."
cargo build --manifest-path "$WORKSPACE_ROOT/Cargo.toml" -p rucio-daemon -p rucio-cli --quiet
[[ -f "$RUCIOD" ]] || fail "ruciod not found at $RUCIOD"

# ---------------------------------------------------------------------------
# 1. Create isolated temp workspace
# ---------------------------------------------------------------------------
TEST_DIR=$(mktemp -d /tmp/rucio-XXXXXX)
info "Test workspace: $TEST_DIR"

mkdir -p \
    "$TEST_DIR/node-a/data" "$TEST_DIR/node-a/downloads" \
    "$TEST_DIR/node-b/data" "$TEST_DIR/node-b/downloads"

# Write config for node A
cat > "$TEST_DIR/node-a/config.toml" <<EOF
[node]
identity_path = "$TEST_DIR/node-a/identity.key"
listen_addrs  = ["/ip4/0.0.0.0/tcp/14321"]

[api]
listen = "127.0.0.1:17070"

[storage]
download_dir  = "$TEST_DIR/node-a/downloads"
database_path = "$TEST_DIR/node-a/data/rucio.db"

[network]
bootstrap_peers = []
EOF

# Write config for node B
cat > "$TEST_DIR/node-b/config.toml" <<EOF
[node]
identity_path = "$TEST_DIR/node-b/identity.key"
listen_addrs  = ["/ip4/0.0.0.0/tcp/14322"]

[api]
listen = "127.0.0.1:17071"

[storage]
download_dir  = "$TEST_DIR/node-b/downloads"
database_path = "$TEST_DIR/node-b/data/rucio.db"

[network]
bootstrap_peers = []
EOF

# Create a 2 MiB random test file
TEST_FILE="$TEST_DIR/test-file.bin"
info "Creating 2 MiB test file..."
dd if=/dev/urandom of="$TEST_FILE" bs=1M count=2 2>/dev/null
ORIGINAL_SHA256=$(sha256sum "$TEST_FILE" | cut -d' ' -f1)
info "SHA-256: $ORIGINAL_SHA256"

# ---------------------------------------------------------------------------
# Cleanup on exit — always show where logs are
# ---------------------------------------------------------------------------
cleanup() {
    info "Stopping daemons..."
    kill "${PID_A:-}" "${PID_B:-}" 2>/dev/null || true
    wait 2>/dev/null || true
    echo ""
    info "Logs and artefacts are in: $TEST_DIR"
    info "  Node A log: $TEST_DIR/node-a/daemon.log"
    info "  Node B log: $TEST_DIR/node-b/daemon.log"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 2. Start both daemons
# ---------------------------------------------------------------------------
info "Starting node A (API :17070, P2P :14321)..."
"$RUCIOD" --config "$TEST_DIR/node-a/config.toml" \
    > "$TEST_DIR/node-a/daemon.log" 2>&1 &
PID_A=$!

info "Starting node B (API :17071, P2P :14322)..."
"$RUCIOD" --config "$TEST_DIR/node-b/config.toml" \
    > "$TEST_DIR/node-b/daemon.log" 2>&1 &
PID_B=$!

# ---------------------------------------------------------------------------
# 3. Wait for both APIs to be ready
# ---------------------------------------------------------------------------
info "Waiting for daemons to start..."
for i in $(seq 1 40); do
    A_OK=$(curl -sf "$API_A/api/v1/status" >/dev/null 2>&1 && echo yes || echo no)
    B_OK=$(curl -sf "$API_B/api/v1/status" >/dev/null 2>&1 && echo yes || echo no)
    [[ "$A_OK" == "yes" && "$B_OK" == "yes" ]] && break
    sleep 0.5
done
curl -sf "$API_A/api/v1/status" >/dev/null || fail "Node A did not start in time (check $TEST_DIR/node-a/daemon.log)"
curl -sf "$API_B/api/v1/status" >/dev/null || fail "Node B did not start in time (check $TEST_DIR/node-b/daemon.log)"
ok "Both nodes up"

PEER_A=$(curl -sf "$API_A/api/v1/status" | jq -r '.peer_id')
PEER_B=$(curl -sf "$API_B/api/v1/status" | jq -r '.peer_id')
info "Node A PeerId: $PEER_A"
info "Node B PeerId: $PEER_B"

# ---------------------------------------------------------------------------
# 4. Share the test file on node A
# ---------------------------------------------------------------------------
info "Sharing test file on node A..."
SHARE_RESP=$(curl -sf -X POST "$API_A/api/v1/shares" \
    -H 'Content-Type: application/json' \
    -d "{\"path\": \"$TEST_FILE\"}")
QUEUED=$(echo "$SHARE_RESP" | jq -r '.queued')
[[ "$QUEUED" -ge 1 ]] || fail "Share request did not queue any files"
ok "File queued for indexing"

# Wait for background indexing to complete
for i in $(seq 1 20); do
    COUNT=$(curl -sf "$API_A/api/v1/shares" | jq '.shares | length')
    [[ "$COUNT" -ge 1 ]] && break
    sleep 0.5
done
SHARES=$(curl -sf "$API_A/api/v1/shares")
[[ "$(echo "$SHARES" | jq '.shares | length')" -ge 1 ]] || \
    fail "File was not indexed on node A (check $TEST_DIR/node-a/daemon.log)"
ok "File indexed on node A"
ROOT_HASH=$(echo "$SHARES" | jq -r '.shares[0].root_hash')
info "Root hash: $ROOT_HASH"

# ---------------------------------------------------------------------------
# 5. Wait for mDNS peer discovery
# ---------------------------------------------------------------------------
info "Waiting for mDNS peer discovery (up to 15s)..."
for i in $(seq 1 30); do
    PEERS=$(curl -sf "$API_B/api/v1/peers" | jq '.peers | length')
    [[ "$PEERS" -ge 1 ]] && break
    sleep 0.5
done
PEERS=$(curl -sf "$API_B/api/v1/peers" | jq '.peers | length')
[[ "$PEERS" -ge 1 ]] || fail "Node B did not discover node A via mDNS"
ok "Node B discovered node A ($PEERS peer(s))"

# ---------------------------------------------------------------------------
# 6. Search from node B
# ---------------------------------------------------------------------------
info "Searching for 'test-file' from node B..."
QUERY_ID=$(curl -sf -X POST "$API_B/api/v1/search" \
    -H 'Content-Type: application/json' \
    -d '{"keywords": ["test-file"]}' | jq -r '.query_id')
info "Query ID: $QUERY_ID"

RESULTS="{}"
for i in $(seq 1 30); do
    RESULTS=$(curl -sf "$API_B/api/v1/search/$QUERY_ID")
    [[ "$(echo "$RESULTS" | jq '.results | length')" -ge 1 ]] && break
    sleep 1
done
RESULT_COUNT=$(echo "$RESULTS" | jq '.results | length')
[[ "$RESULT_COUNT" -ge 1 ]] || fail "No search results found on node B"
ok "Found $RESULT_COUNT result(s)"

MAGNET=$(echo "$RESULTS"  | jq -r '.results[0].magnet')
PROVIDER=$(echo "$RESULTS" | jq -r '.results[0].provider')
info "Magnet:   $MAGNET"
info "Provider: $PROVIDER"

# ---------------------------------------------------------------------------
# 7. Download from node B
# ---------------------------------------------------------------------------
info "Starting download on node B..."
HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
    -X POST "$API_B/api/v1/downloads" \
    -H 'Content-Type: application/json' \
    -d "{\"magnet\": \"$MAGNET\", \"provider\": \"$PROVIDER\"}")
[[ "$HTTP_CODE" == "202" ]] || fail "Download request rejected (HTTP $HTTP_CODE)"
ok "Download queued (HTTP 202)"

# ---------------------------------------------------------------------------
# 8. Wait for completion
# ---------------------------------------------------------------------------
info "Waiting for download to complete (up to 60s)..."
STATUS="unknown"
for i in $(seq 1 120); do
    DOWNLOADS=$(curl -sf "$API_B/api/v1/downloads")
    STATUS=$(echo "$DOWNLOADS" | jq -r '.downloads[0].state // "unknown"')
    [[ "$STATUS" == "Completed" || "$STATUS" == "Failed" ]] && break
    sleep 0.5
done
[[ "$STATUS" == "Completed" ]] || \
    fail "Download did not complete — final state: $STATUS (check $TEST_DIR/node-b/daemon.log)"
ok "Download completed"

# ---------------------------------------------------------------------------
# 9. Verify integrity
# ---------------------------------------------------------------------------
DEST_FILE=$(find "$TEST_DIR/node-b/downloads" -type f | head -1)
[[ -f "$DEST_FILE" ]] || fail "Downloaded file not found under $TEST_DIR/node-b/downloads"
info "Downloaded file: $DEST_FILE"

DOWNLOADED_SHA256=$(sha256sum "$DEST_FILE" | cut -d' ' -f1)
info "Original   SHA-256: $ORIGINAL_SHA256"
info "Downloaded SHA-256: $DOWNLOADED_SHA256"
[[ "$ORIGINAL_SHA256" == "$DOWNLOADED_SHA256" ]] || fail "SHA-256 mismatch — file is corrupted!"
ok "SHA-256 matches — file integrity verified"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo -e "${GREEN}  All checks passed — end-to-end test OK  ${NC}"
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo ""
info "Artefacts kept at: $TEST_DIR"
