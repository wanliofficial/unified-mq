#!/usr/bin/env bash
# S10-0 spike runner: start the Erlang listener on an ephemeral port, drive it
# with the Python client, then collect the event trace.
#
# Port 0 everywhere: no fixed port is touched (8080/8083/1883/8081/8082 stay
# exactly as they were).
set -euo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd "$(dirname "$0")"

PORTFILE="$PWD/port.txt"
EVENTS="$PWD/events.out"
NODELOG="$PWD/node.out"
CLIENTLOG="$PWD/client.out"

rm -f "$PORTFILE" "$EVENTS" "$NODELOG" "$CLIENTLOG"

# the beam files the scripts load live in _build/default, which a plain
# `rebar3 eunit` does not produce (that only fills _build/test)
rebar3 compile >/dev/null

erl -noshell -pa _build/default/lib/*/ebin -pa _build/default/checkouts/*/ebin \
    -run spike_run main "$PORTFILE" "$EVENTS" 12 > "$NODELOG" 2>&1 &
NODE_PID=$!
trap 'kill $NODE_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 100); do
    [ -s "$PORTFILE" ] && break
    sleep 0.1
done
if [ ! -s "$PORTFILE" ]; then
    echo "listener never published a port; node log:" >&2
    cat "$NODELOG" >&2
    exit 1
fi
PORT=$(cat "$PORTFILE")
echo "listener on 127.0.0.1:$PORT (ephemeral, port => 0)"

PYTHONPATH=/tmp/opencode/s10-libs /usr/bin/python3 spike_client.py "$PORT" "$CLIENTLOG"
echo "--- client results ---"
cat "$CLIENTLOG"

# the collector writes its trace when its 12 s window closes; wait for it
for _ in $(seq 1 200); do
    [ -s "$EVENTS" ] && break
    sleep 0.2
done
echo "--- server events ---"
cat "$EVENTS" 2>/dev/null || echo "(no trace file)"
