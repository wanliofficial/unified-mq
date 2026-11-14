#!/usr/bin/env bash
# S10-D D.6 capacity bench: drive the production WebRTC signaling relay and the
# STUN/TURN relay path through ONE evidence node, and drop the raw JSON matrix
# under bench/results/.
#
# It is NOT part of `rebar3 ct'. It boots the same evidence node the D.1/D.2
# evidence uses (`s10_node': WSS on an ephemeral loopback port, STUN/TURN on an
# ephemeral UDP port, narrow relay pool, max_connections => 64), waits for it, runs:
#   * the signaling matrix (connections x rooms x rate, sequential + concurrent)
#     via webrtc_bench.py (the exact D.1 wire path);
#   * the ICE allocation-rate + relay-throughput bench via ice_bench.erl.
#
# Nothing here touches the fixed 8080/8083/1883/8081/8082/8084 ports or any CT
# relay range; everything binds an ephemeral port on loopback.
set -euo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
S10ROOT="$(cd "$HERE/.." && pwd)"
RESULTS="$HERE/results"
mkdir -p "$RESULTS"

# aiortc + aioice + av + websockets live outside the repo (installed to a scratch
# dir). Override S10_LIBS to point elsewhere.
LIBS_PY="${S10_LIBS:-/tmp/opencode/s10-libs}"
EREP="${BENCH_DATA:-/tmp/opencode/s10-bench}"
ERL_EBIN="$EREP/ebin"
NODELOG="$EREP/node.out"

LIBS=$(for d in "$S10ROOT"/../../../_build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done)

APPS="$S10ROOT/../../../apps"

echo "[bench] compiling evidence node + ICE bench..."
mkdir -p "$ERL_EBIN"
rm -f "$ERL_EBIN"/s10_node.beam "$ERL_EBIN"/s10_run.beam \
      "$ERL_EBIN"/ice_bench.beam "$ERL_EBIN"/s10_bench_run.beam \
      "$ERL_EBIN"/ice_test_client.beam
# shellcheck disable=SC2086
erlc -o "$ERL_EBIN" $LIBS -I "$S10ROOT/src" -I "$APPS/broker_core/include" \
    "$S10ROOT/src/s10_node.erl" "$S10ROOT/src/s10_run.erl"
# shellcheck disable=SC2086
erlc -o "$ERL_EBIN" $LIBS -I "$APPS/broker_ice/test" \
    "$APPS/broker_ice/test/ice_test_client.erl" \
    "$HERE/ice_bench.erl" "$HERE/s10_bench_run.erl"

echo "[bench] booting evidence node..."
rm -rf "$EREP/data"
S10_DATA="$EREP/data" S10_CERTDIR="$EREP/certs" \
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
    echo "[bench] node never printed S10-NODE-READY; log:" >&2
    cat "$NODELOG" >&2
    exit 1
fi
WSS_PORT=$(printf '%s' "$READY" | sed -n 's/.*wss=\([0-9]*\).*/\1/p')
STUN_PORT=$(printf '%s' "$READY" | sed -n 's/.*stun=\([0-9]*\).*/\1/p')
echo "[bench] $READY"

export PYTHONPATH="$LIBS_PY"

# Resumable: skip any case whose JSON already exists, so a re-run after an OOM
# (the box is often under external memory pressure) only redoes the missing
# pieces instead of starting the whole matrix over.
run_sig() {
  local tag="$1"; shift
  if [ -s "$RESULTS/webrtc_${tag}.json" ]; then
    echo "  -> $tag (cached, skip)"
    return 0
  fi
  echo "  -> $tag"
  # Cooperative memory guard: if the box is starved (external builds/VMs), wait
  # for headroom instead of charging the OOM killer. Needs >=800MB available.
  # LC_ALL=C forces the English "Mem:" line label (the box locale is zh_CN).
  for _ in $(seq 1 60); do
    local avail
    avail=$(LC_ALL=C free -m | awk '/^Mem:/{print $7}')
    [ "${avail:-0}" -ge 800 ] && break
    echo "     waiting for memory (avail=${avail}MB)..."
    sleep 5
  done
  /usr/bin/python3 "$HERE/webrtc_bench.py" \
    --port "$WSS_PORT" "$@" --out "$RESULTS/webrtc_${tag}.json"
}

echo "[bench] [1/2] signaling matrix (connections x rooms x rate, seq + conc)..."
# Connections scaling at a moderate per-pair rate.
for ROOMS in 1 4 16 32; do
  for MODE in sequential concurrent; do
    run_sig "conn-r${ROOMS}-${MODE}" --rooms "$ROOMS" --rate 50 --total 100 --mode "$MODE"
  done
done
# Per-pair rate sweep at a fixed connection count (32 connections = 16 rooms).
for RATE in 10 50 100 200 500 0; do
  run_sig "rate-r${RATE}" --rooms 16 --rate "$RATE" --total 200 --mode concurrent
done

echo "[bench] [2/2] ICE allocation rate + relay throughput..."
BENCH_STUN_PORT="$STUN_PORT" BENCH_ALLOC_SEQ=50 BENCH_ALLOC_CONC=12 \
  BENCH_RELAY_MSGS=5000 BENCH_PAYLOAD=256 \
  erl -noshell $LIBS -pa "$ERL_EBIN" -s s10_bench_run main \
    > "$RESULTS/ice_bench.json" 2> "$RESULTS/ice_bench.err" || {
      echo "[bench] ICE bench failed; log:" >&2
      cat "$RESULTS/ice_bench.err" >&2
      exit 1
    }

echo "[bench] done. Raw JSON in $RESULTS:"
ls -1 "$RESULTS"/*.json
