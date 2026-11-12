#!/usr/bin/env bash
# S10-0 spike: the same signaling code over ranch's ssl transport (wss).
#
# Answers "does S10-A need a second code path for TLS?" -- it should not, and
# this script proves it against a self-signed cert on an ephemeral port.
set -euo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd "$(dirname "$0")"

CERTDIR="$PWD/certs"
mkdir -p "$CERTDIR"
if [ ! -f "$CERTDIR/spike-cert.pem" ]; then
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$CERTDIR/spike-key.pem" -out "$CERTDIR/spike-cert.pem" \
        -days 1 -subj "/CN=127.0.0.1" >/dev/null 2>&1
fi

PORTFILE="$PWD/tls-port.txt"
EVENTS="$PWD/tls-events.out"
NODELOG="$PWD/tls-node.out"
CLIENTLOG="$PWD/tls-client.out"
rm -f "$PORTFILE" "$EVENTS" "$NODELOG" "$CLIENTLOG"

# the beam files the scripts load live in _build/default, which a plain
# `rebar3 eunit` does not produce (that only fills _build/test)
rebar3 compile >/dev/null

erl -noshell -pa _build/default/lib/*/ebin -pa _build/default/checkouts/*/ebin \
    -run spike_run_tls main "$PORTFILE" "$EVENTS" "$CERTDIR" 12 > "$NODELOG" 2>&1 &
NODE_PID=$!
trap 'kill $NODE_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
    [ -s "$PORTFILE" ] && break
    sleep 0.1
done
if [ ! -s "$PORTFILE" ]; then
    echo "wss listener never published a port; node log:" >&2
    cat "$NODELOG" >&2
    exit 1
fi
PORT=$(cat "$PORTFILE")
echo "wss listener on 127.0.0.1:$PORT (ephemeral, port => 0)"

PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 \
    spike_wss_client.py "$PORT" "$CLIENTLOG" "$CERTDIR"
echo "--- client results ---"
cat "$CLIENTLOG"

for _ in $(seq 1 200); do
    [ -s "$EVENTS" ] && break
    sleep 0.2
done
echo "--- server events ---"
cat "$EVENTS" 2>/dev/null || echo "(no trace file)"
