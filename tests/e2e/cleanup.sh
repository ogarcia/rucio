#!/usr/bin/env bash
# cleanup.sh — Kill stray Rucio test daemons and remove /tmp workspaces.
#
# Run this after a failed or interrupted test-e2e.sh run to free ports
# and disk space before trying again.
#
# Usage:
#   bash tests/e2e/cleanup.sh          (from workspace root)
#   bash cleanup.sh                     (from tests/e2e/)

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

ok()   { echo -e "${GREEN}✓${NC} $*"; }
info() { echo -e "${YELLOW}→${NC} $*"; }
warn() { echo -e "${RED}!${NC} $*"; }

# ---------------------------------------------------------------------------
# 1. Kill stray ruciod processes
# ---------------------------------------------------------------------------
info "Looking for stray ruciod processes..."
PIDS=$(pgrep -x ruciod 2>/dev/null || true)
if [[ -n "$PIDS" ]]; then
    echo "$PIDS" | xargs kill 2>/dev/null || true
    sleep 0.5
    # SIGKILL any survivors
    SURVIVORS=$(pgrep -x ruciod 2>/dev/null || true)
    if [[ -n "$SURVIVORS" ]]; then
        echo "$SURVIVORS" | xargs kill -9 2>/dev/null || true
    fi
    ok "Killed ruciod process(es): $(echo "$PIDS" | tr '\n' ' ')"
else
    ok "No stray ruciod processes found"
fi

# ---------------------------------------------------------------------------
# 2. Remove /tmp/rucio-* workspaces
# ---------------------------------------------------------------------------
info "Removing /tmp/rucio-* test workspaces..."
DIRS=$(find /tmp -maxdepth 1 -name 'rucio-*' -type d 2>/dev/null || true)
if [[ -n "$DIRS" ]]; then
    COUNT=$(echo "$DIRS" | wc -l)
    echo "$DIRS" | xargs rm -rf
    ok "Removed $COUNT workspace(s)"
else
    ok "No test workspaces found in /tmp"
fi

echo ""
ok "Cleanup complete"
