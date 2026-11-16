#!/usr/bin/env bash
# S11-C capacity bench: boot a real broker node in-process, drive the real
# CoAP-over-DTLS listener with a real PSK client, and drop the raw JSON matrix
# under bench/results/.
#
# NOT part of `rebar3 ct'. Nothing binds a fixed port -- every listener uses
# `port => 0' and everything stays on loopback.
set -euo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
RESULTS="$HERE/results"
mkdir -p "$RESULTS"

echo "[bench] building the project..."
( cd "$REPO" && rebar3 compile >/dev/null )

echo "[bench] compiling bench module..."
erlc -o "$HERE" -I "$REPO/apps/broker_core/include" "$HERE/dtls_bench.erl"

echo "[bench] running dtls_bench..."
cd "$REPO"
# shellcheck disable=SC2046
erl -noshell -pa "$HERE" $(for d in _build/default/lib/*/ebin; do printf -- "-pa %s " "$d"; done) \
    -eval 'dtls_bench:main()' \
    2>&1 | tee "$RESULTS/run.log"

echo "[bench] results: $RESULTS"
