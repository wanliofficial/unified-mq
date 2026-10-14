#!/usr/bin/env bash
# S0.2 - form K independent single-node ra groups and sweep K to measure
# per-group machinery overhead (processes / ETS / memory) and write throughput.
#
# Single BEAM, single node. This isolates the cost of *having* N raft groups
# from replication cost (S0.1 already pinned ~677 us/write replicated).
set -uo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
SPIKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Absolute data dir so the Erlang side (via SPIKE_DATA_DIR) and the -ra flag
# resolve to the exact same path.
export SPIKE_DATA_DIR="$SPIKE_DIR/.data"
mkdir -p "$SPIKE_DATA_DIR"
rm -rf "$SPIKE_DATA_DIR"/*
PA=$(ls -d "$SPIKE_DIR"/_build/default/lib/*/ebin | tr '\n' ' ')

erl -pa $PA -sname s0_2 -setcookie s0_2cookie \
    -ra data_dir "\"$SPIKE_DATA_DIR/ra\"" \
    -kernel logger_level warning \
    -sasl sasl_error_logger false \
    -noshell \
    -eval "s0_2_bench:main()." \
    2>&1 | grep -vE '^ *[0-9]+>|alarm|=>|^\s*\[?\{?[a-z_]+,|SUPERVISOR REPORT|supervisor:|errorContext|offender|reason:|pid,undefined|#Ref|\.data/ra/'
