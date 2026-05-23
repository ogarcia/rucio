#!/usr/bin/env bash
# test-e2e.sh — End-to-end test for Rucio
#
# Starts two daemon instances (A and B), shares a file on A,
# searches from B, downloads it, and verifies the BLAKE3 hash.
#
# Usage:
#   bash test-e2e.sh
#
# Requirements:
#   - cargo build must have been run (binaries in target/debug/)
#   - curl and jq installed

set -euo pipefail

REPO="$(cd "$(dirname "$0")" && pwd)"
RUCIOD="$REPO/target/debug/ruciod"
RUCIO="$REPO/target/debug/rucio"
TEST_DIR="/tmp/rucio-test"
TEST_FILE="$TEST_DIR/test-file.bin"
API_A="http://127.0.0.1:17070"
API_B="http://127.0.0.1:17071"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

ok()   { echo -e "${GREEN}✓${NC} $*"; }
fail() { echo -e "${RED}✗${NC} $*"; exit 1; }
info() { echo -e "${YELLOW}→${NC} $*"; }

# ---------------------------------------------------------------------------
# 0. Pre-checks
# ---------------------------------------------------------------------------
info "Building binaries..."
cargo build -p rucio-daemon -p rucio-cli --quiet 2>/dev/null || cargo build -p rucio-daemon -p rucio-cli

[[ -f "$RUCIOD" ]] || fail "ruciod not found at $RUCIOD"
[[ -f "$RUCIO"  ]] || fail "rucio not found at $RUCIO"
command -v curl >/dev/null || fail "curl not installed"
command -v jq   >/dev/null || fail "jq not installed"

# Create test file if missing
if [[ ! -f "$TEST_FILE" ]]; then
    info "Creating 2 MiB test file..."
    dd if=/dev/urandom of="$TEST_FILE" bs=1M count=2 2>/dev/null
fi

ORIGINAL_SHA256=$(sha256sum "$TEST_FILE" | cut -d' ' -f1)
info "Test file SHA-256: $ORIGINAL_SHA256"

# Clean state from previous runs
rm -f "$TEST_DIR/node-a/data/rucio.db" \
      "$TEST_DIR/node-b/data/rucio.db" \
      "$TEST_DIR/node-a/identity.key" \
      "$TEST_DIR/node-b/identity.key"
mkdir -p "$TEST_DIR/node-a/data" "$TEST_DIR/node-a/downloads" \
         "$TEST_DIR/node-b/data" "$TEST_DIR/node-b/downloads"

# ---------------------------------------------------------------------------
# 1. Start node A
# ---------------------------------------------------------------------------
info "Starting node A (API: $API_A, P2P: :14321)..."
"$RUCIOD" --config "$TEST_DIR/node-a/config.toml" \
    > "$TEST_DIR/node-a/daemon.log" 2>&1 &
PID_A=$!
trap 'info "Stopping daemons..."; kill $PID_A $PID_B 2>/dev/null; wait 2>/dev/null' EXIT

# ---------------------------------------------------------------------------
# 2. Start node B
# ---------------------------------------------------------------------------
info "Starting node B (API: $API_B, P2P: :14322)..."
"$RUCIOD" --config "$TEST_DIR/node-b/config.toml" \
    > "$TEST_DIR/node-b/daemon.log" 2>&1 &
PID_B=$!

# ---------------------------------------------------------------------------
# 3. Wait for both APIs to be ready
# ---------------------------------------------------------------------------
info "Waiting for daemons to start..."
for i in $(seq 1 30); do
    A_OK=$(curl -sf "$API_A/api/v1/status" >/dev/null 2>&1 && echo yes || echo no)
    B_OK=$(curl -sf "$API_B/api/v1/status" >/dev/null 2>&1 && echo yes || echo no)
    [[ "$A_OK" == "yes" && "$B_OK" == "yes" ]] && break
    sleep 0.5
done
curl -sf "$API_A/api/v1/status" >/dev/null || fail "Node A did not start in time"
curl -sf "$API_B/api/v1/status" >/dev/null || fail "Node B did not start in time"
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
echo "$SHARE_RESP" | jq .
QUEUED=$(echo "$SHARE_RESP" | jq -r '.queued')
[[ "$QUEUED" -ge 1 ]] || fail "Share request did not queue any files"
ok "File queued for indexing"

# Wait for indexing to complete (background task)
info "Waiting for indexing..."
for i in $(seq 1 20); do
    SHARES=$(curl -sf "$API_A/api/v1/shares" | jq '.shares | length')
    [[ "$SHARES" -ge 1 ]] && break
    sleep 0.5
done
SHARES=$(curl -sf "$API_A/api/v1/shares")
echo "$SHARES" | jq .
SHARE_COUNT=$(echo "$SHARES" | jq '.shares | length')
[[ "$SHARE_COUNT" -ge 1 ]] || fail "File was not indexed on node A"
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
SEARCH_RESP=$(curl -sf -X POST "$API_B/api/v1/search" \
    -H 'Content-Type: application/json' \
    -d '{"keywords": ["test-file"]}')
QUERY_ID=$(echo "$SEARCH_RESP" | jq -r '.query_id')
info "Query ID: $QUERY_ID"

# Poll for results
RESULT_COUNT=0
for i in $(seq 1 30); do
    RESULTS=$(curl -sf "$API_B/api/v1/search/$QUERY_ID")
    RESULT_COUNT=$(echo "$RESULTS" | jq '.results | length')
    [[ "$RESULT_COUNT" -ge 1 ]] && break
    sleep 1
done
echo "$RESULTS" | jq .
[[ "$RESULT_COUNT" -ge 1 ]] || fail "No search results found on node B"
ok "Found $RESULT_COUNT result(s)"

MAGNET=$(echo "$RESULTS" | jq -r '.results[0].magnet')
PROVIDER=$(echo "$RESULTS" | jq -r '.results[0].provider')
info "Magnet: $MAGNET"
info "Provider: $PROVIDER"

# ---------------------------------------------------------------------------
# 7. Download from node B
# ---------------------------------------------------------------------------
info "Starting download on node B..."
DL_RESP=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "$API_B/api/v1/downloads" \
    -H 'Content-Type: application/json' \
    -d "{\"magnet\": \"$MAGNET\", \"provider\": \"$PROVIDER\"}")
[[ "$DL_RESP" == "202" ]] || fail "Download request rejected (HTTP $DL_RESP)"
ok "Download queued (HTTP 202)"

# ---------------------------------------------------------------------------
# 8. Wait for completion
# ---------------------------------------------------------------------------
info "Waiting for download to complete (up to 60s)..."
STATUS="unknown"
for i in $(seq 1 120); do
    DOWNLOADS=$(curl -sf "$API_B/api/v1/downloads")
    STATUS=$(echo "$DOWNLOADS" | jq -r '.downloads[0].state // "unknown"')
    info "  Status: $STATUS (${i}/120)"
    [[ "$STATUS" == "Completed" || "$STATUS" == "Failed" ]] && break
    sleep 0.5
done
echo "$DOWNLOADS" | jq .
[[ "$STATUS" == "Completed" ]] || fail "Download did not complete (final state: $STATUS)"
ok "Download completed"

# ---------------------------------------------------------------------------
# 9. Verify integrity
# ---------------------------------------------------------------------------
DEST_FILE=$(find "$TEST_DIR/node-b/downloads" -type f | head -1)
[[ -f "$DEST_FILE" ]] || fail "Downloaded file not found in $TEST_DIR/node-b/downloads"
info "Downloaded file: $DEST_FILE"

DOWNLOADED_SHA256=$(sha256sum "$DEST_FILE" | cut -d' ' -f1)
info "Original  SHA-256: $ORIGINAL_SHA256"
info "Downloaded SHA-256: $DOWNLOADED_SHA256"
[[ "$ORIGINAL_SHA256" == "$DOWNLOADED_SHA256" ]] || fail "SHA-256 mismatch — file corrupted!"
ok "SHA-256 matches — file integrity verified"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo -e "${GREEN}══════════════════════════════════════════${NC}"
echo -e "${GREEN}  All checks passed — end-to-end test OK  ${NC}"
echo -e "${GREEN}══════════════════════════════════════════${NC}"
