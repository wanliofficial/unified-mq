#!/usr/bin/env bash
#
# Run the MQTT-over-QUIC compatibility suite against a real broker.
#
# Client:  emqtt 1.16.1 (third-party, EMQX) with quicer 0.4.12
# Broker:  this repository, built with `rebar3 as quic' and listening on an
#          ephemeral UDP port
#
# Usage:
#   ./run.sh                     # all cases
#   COMPAT_CASES=connect,qos1 ./run.sh
#
# The first run compiles quicer (and through it msquic + quictls); that is a
# ~10-15 minute C build. Later runs reuse both `_build/` trees.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../../.." && pwd)
otp_bin=${OTP_BIN:-/home/wanli/Development/otp-28.3.1/bin}
export PATH="$otp_bin:$PATH"

harness_ebin="$here/_build/harness"
# The client project sets `base_dir' to `_build/quic_client' at the repo root:
# its build tree must not live under `test/', which `rebar3 ct' compiles whole.
client_build="$repo/_build/quic_client"
client_ebin="$client_build/default/lib/emqtt/ebin"
quicer_ebin="$repo/_build/quic/lib/quicer/ebin"
tls_dir="$here/data/tls"
log="$here/_build/broker.log"

step() { printf '\n>>> %s\n' "$*"; }

step "broker: rebar3 as quic compile"
(cd "$repo" && rebar3 as quic compile >/dev/null)

step "client: emqtt + quicer"
(cd "$here/client" && rebar3 compile >/dev/null)

step "harness modules"
mkdir -p "$harness_ebin"
erlc -o "$harness_ebin" \
     "$here/mqtt_quic_broker_boot.erl" \
     "$here/mqtt_quic_compat.erl"

step "TLS certificate (QUIC has no cleartext mode)"
mkdir -p "$tls_dir"
if [ ! -f "$tls_dir/cert.pem" ] || [ ! -f "$tls_dir/key.pem" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
            -subj /CN=localhost \
            -addext subjectAltName=DNS:localhost,IP:127.0.0.1 \
            -addext extendedKeyUsage=serverAuth \
            -keyout "$tls_dir/key.pem" -out "$tls_dir/cert.pem" 2>/dev/null
fi

step "starting the broker"
mkdir -p "$here/_build"
: >"$log"
erl -noshell \
    -pa "$repo"/_build/quic/lib/*/ebin \
    -pa "$harness_ebin" \
    -s mqtt_quic_broker_boot run >"$log" 2>&1 &
broker_pid=$!
cleanup() { kill "$broker_pid" 2>/dev/null || true; wait "$broker_pid" 2>/dev/null || true; }
trap cleanup EXIT

port=""
for _ in $(seq 1 100); do
    port=$(sed -n 's/^PORT \([0-9][0-9]*\)$/\1/p' "$log" | head -1 || true)
    [ -n "$port" ] && break
    if ! kill -0 "$broker_pid" 2>/dev/null; then
        echo "broker exited during boot:" >&2
        sed -n '1,80p' "$log" >&2
        exit 1
    fi
    sleep 0.2
done
if [ -z "$port" ]; then
    echo "no PORT line in $log after 20s:" >&2
    sed -n '1,80p' "$log" >&2
    exit 1
fi
echo "    listening on UDP $port"

step "running the cases"
set +e
BROKER_PORT="$port" \
COMPAT_CASES="${COMPAT_CASES:-all}" \
erl -noshell \
    -pa "$harness_ebin" \
    -pa "$client_ebin" \
    -pa "$quicer_ebin" \
    -eval 'mqtt_quic_compat:main([]).'
status=$?
set -e

if [ "$status" -ne 0 ]; then
    step "broker log (tail)"
    tail -40 "$log" || true
fi
exit "$status"
