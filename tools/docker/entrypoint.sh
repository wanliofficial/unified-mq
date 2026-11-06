#!/bin/sh
# T14 broker entrypoint.
#
# Partition creation is deliberately out of band (design §6/H8, ADR-011
# §11.2): boot only recovers, so a fresh data dir must be prepared by an
# explicit `create_partition/1' call before the node is allowed to start.  The
# call is idempotent: on a recovered data dir every partition answers
# `{error, {already_exists, P}}' and the node proceeds to the real recovery.
set -eu

REL=${REL:-/opt/unified_mq}
CONFIG="$REL/releases/0.1.0/sys.config"
DATA=${S1_DATA_DIR:-/var/lib/unified-mq}

mkdir -p "$DATA/log"

# A clean failure is evidence: no fallback to an empty partition, no retry that
# hides a corrupt data directory.
if ! erl -noshell -boot no_dot_erlang -config "$CONFIG" -eval '
    application:load(broker_queue),
    {ok, _} = application:ensure_all_started(broker_storage),
    lists:foreach(
      fun(P) ->
          case broker_queue:create_partition(P) of
              ok ->
                  io:format("s1 bootstrap: created partition ~p~n", [P]);
              {error, {already_exists, P}} ->
                  ok;
              {error, Reason} ->
                  io:format(standard_error,
                            "s1 bootstrap: partition ~p failed: ~p~n", [P, Reason]),
                  halt(1)
          end
      end, broker_queue:partition_ids()),
    halt(0).'; then
    echo "s1 bootstrap failed; refusing to start" >&2
    exit 1
fi

cd "$REL"
# relx's launcher requires exactly one -name/-sname (config/vm.args.src).  epmd
# lives in this container's network namespace, so this does not touch the host.
epmd -daemon || true
exec "$REL/bin/unified_mq" foreground
