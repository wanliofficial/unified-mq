#!/usr/bin/env bash
# S10-D D.1: run the full real-third-party-client evidence suite against one
# evidence node, and drop the raw artefacts under this directory.
#
# The suite is three scripts, all driven against the SAME node:
#   * stun_evidence.py     -- raw UDP Binding, hex + hand-decode (no library)
#   * webrtc_evidence.py   -- two aiortc peers: offer/answer + data channel +
#                             STUN srflx, through our WSS signaling relay
#   * turn_evidence.py     -- a real aioice client allocates a TURN relay with
#                             the alice credential and is refused with a wrong one
#
# It is NOT part of `rebar3 ct`. It needs the aiortc wheel set on PYTHONPATH
# (see README) and a running Erlang node, which this script compiles, boots,
# waits for, drives, and tears down itself. Nothing here touches the fixed
# 8080/8083/1883/8081/8082/8084 ports or any CT relay range; the node binds an
# ephemeral WSS port, an ephemeral STUN/TURN port, and a narrow relay pool.
set -euo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

# aiortc + aioice + av + websockets live outside the repo (installed to a
# scratch dir). Override S10_LIBS to point elsewhere.
LIBS_PY="${S10_LIBS:-/tmp/opencode/s10-libs}"
EREP="${S10_DATA:-/tmp/opencode/s10-evidence}"
ERL_EBIN="${S10_EBIN:-/tmp/opencode/s10-ebin}"

NODELOG="$EREP/node.out"
mkdir -p "$EREP"

echo "[s10] compiling evidence node..."
LIBS=$(for d in ../../../_build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done)
mkdir -p "$ERL_EBIN"
rm -f "$ERL_EBIN"/s10_node.beam "$ERL_EBIN"/s10_run.beam
# shellcheck disable=SC2086
erlc -o "$ERL_EBIN" $LIBS -I src -I ../../../apps/broker_core/include src/*.erl

echo "[s10] booting evidence node..."
rm -rf "$EREP/data"
erl -noshell $LIBS -pa "$ERL_EBIN" -s s10_run main > "$NODELOG" 2>&1 &
NODE_PID=$!
trap 'kill $NODE_PID 2>/dev/null || true' EXIT

READY=""
for _ in $(seq 1 100); do
    if READY=$(grep -o 'S10-NODE-READY.*' "$NODELOG" 2>/dev/null | head -1); then
        break
    fi
    sleep 0.1
done
if [ -z "$READY" ]; then
    echo "[s10] node never printed S10-NODE-READY; log:" >&2
    cat "$NODELOG" >&2
    exit 1
fi
echo "[s10] $READY"

# Parse the two ports and the advertised ICE host out of the readiness line.
WSS_PORT=$(printf '%s' "$READY" | sed -n 's/.*wss=\([0-9]*\).*/\1/p')
STUN_PORT=$(printf '%s' "$READY" | sed -n 's/.*stun=\([0-9]*\).*/\1/p')
ICE_HOST=$(printf '%s' "$READY" | sed -n 's/.*ice=\([^ ]*\).*/\1/p')
echo "[s10] wss=$WSS_PORT stun=$STUN_PORT ice=$ICE_HOST"

export PYTHONPATH="$LIBS_PY"

echo "[s10] [1/3] raw STUN binding..."
/usr/bin/python3 stun_evidence.py \
    --ice-host "$ICE_HOST" --stun-port "$STUN_PORT" \
    --out "$EREP/stun_evidence.jsonl"

echo "[s10] [2/3] aiortc signaling + STUN srflx..."
/usr/bin/python3 webrtc_evidence.py \
    --host 127.0.0.1 --stun-host "$ICE_HOST" \
    --wss-port "$WSS_PORT" --stun-port "$STUN_PORT" \
    --out "$EREP/webrtc_signaling.jsonl"

echo "[s10] [3/3] aiortc TURN allocate..."
/usr/bin/python3 turn_evidence.py \
    --ice-host "$ICE_HOST" --turn-port "$STUN_PORT" \
    --out "$EREP/turn_evidence.jsonl"

echo "[s10] done. Artefacts in $EREP:"
ls -1 "$EREP"/*.jsonl
