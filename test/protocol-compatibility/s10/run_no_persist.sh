#!/usr/bin/env bash
# S10-D D.2: the runtime "no persistence" proof.
#
# One Erlang VM boots the real kernel (WAL storage + live broker_subscription
# with its cursor tick) and both production listeners (WSS signaling, STUN/TURN),
# then the driver (`s10_no_persist_run`) does:
#
#   snapshot -> full 1:1 aiortc call through the relay -> snapshot -> diff
#   + two negative controls (one append must move the diff; a real subscription
#     must make a `.cursors' file appear)
#
# It is NOT part of `rebar3 ct`: the webrtc CT suites do not boot the storage and
# subscription kernel, and this proof is about a *live* kernel writer never
# firing. Ports are ephemeral (`port => 0`); nothing touches 8080/8083/1883/
# 8081/8082/8084.
#
# Note the two separate directories: S10_DATA is the node's WAL/session volume
# (wiped at boot), S10_EVIDENCE is where the JSONL artefacts land (kept).
set -euo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

LIBS_PY="${S10_LIBS:-/tmp/opencode/s10-libs}"
DATA_DIR="${S10_DATA:-/tmp/opencode/s10-d2-data}"
EREP="${S10_EVIDENCE:-/tmp/opencode/s10-evidence}"
ERL_EBIN="${S10_EBIN:-/tmp/opencode/s10-ebin}"
OUT="$EREP/no_persistence.jsonl"

mkdir -p "$EREP" "$ERL_EBIN"

echo "[s10-d2] compiling evidence modules..."
LIBS=$(for d in ../../../_build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done)
rm -f "$ERL_EBIN"/s10_node.beam "$ERL_EBIN"/s10_run.beam \
      "$ERL_EBIN"/s10_no_persist.beam "$ERL_EBIN"/s10_no_persist_run.beam
# shellcheck disable=SC2086
erlc -o "$ERL_EBIN" $LIBS -I src -I ../../../apps/broker_core/include src/*.erl

echo "[s10-d2] booting node + driving the 1:1 call (one VM, ephemeral ports)..."
S10_DATA="$DATA_DIR" S10_DIR="$HERE" S10_LIBS="$LIBS_PY" \
S10_NOPERSIST_OUT="$OUT" \
erl -noshell $LIBS -pa "$ERL_EBIN" -s s10_no_persist_run main 2>&1 | tee "$EREP/d2-node.out"

echo
echo "[s10-d2] evidence: $OUT"
