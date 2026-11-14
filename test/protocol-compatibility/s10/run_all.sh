#!/usr/bin/env bash
# S10-D: run every evidence script in this directory, in order.
#
#   run_evidence.sh      -- D.1: raw STUN, aiortc signaling + srflx, aiortc TURN
#                           (one node: WSS + STUN/TURN, no subscription kernel)
#   run_no_persist.sh    -- D.2: runtime "no persistence" proof (needs a node with
#                           the storage *and* subscription kernel live, so it
#                           boots its own)
#
# Both scripts own their node (`port => 0` everywhere) and kill it via a trap, so
# running them back to back never leaves a node behind and never touches the
# fixed 8080/8083/1883/8081/8082/8084 ports.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

EREP="${S10_EVIDENCE:-/tmp/opencode/s10-evidence}"

echo "=== S10-D D.1: real third-party client evidence ==="
./run_evidence.sh

echo
echo "=== S10-D D.2: runtime no-persistence proof ==="
./run_no_persist.sh

echo
echo "=== S10-D evidence complete: copy the fresh JSONL artefacts in ==="
# The scripts write their raw output under $S10_EVIDENCE (or $S10_DATA for D.1);
# the committed copies live next to this script. These four are what README
# quotes.
for f in stun_evidence.jsonl webrtc_signaling.jsonl turn_evidence.jsonl \
         no_persistence.jsonl; do
    for dir in "$EREP" "${S10_DATA:-/tmp/opencode/s10-evidence}"; do
        if [ -f "$dir/$f" ]; then
            cp "$dir/$f" "$HERE/$f"
            echo "  updated $f"
            break
        fi
    done
done
