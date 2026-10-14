#!/usr/bin/env bash
# S0.1 - boot a 3-node ra cluster and run the failure scenarios.
#
# Nodes are separate OS processes with Erlang distribution, sharing only
# loopback. That is the minimum fidelity needed to answer "does ra survive a
# node dying" (README 11.3).
#
# The scenario driver runs INSIDE a cluster node (it needs local ra state),
# so it cannot kill its own node: the kill would tear down the rpc link
# before a result could be returned. The script therefore drives the phases
# and performs the kills itself.
set -uo pipefail

export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
SPIKE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PA=$(ls -d "$SPIKE_DIR"/_build/default/lib/*/ebin | tr '\n' ' ')
# Erlang distribution rejects literal IPs ("Hostname 127.0.0.1 is illegal").
HOST="$(hostname)"
COOKIE="$(cat "$HOME/.erlang.cookie")"
DATA_ROOT="${SPIKE_DATA_DIR:-/tmp/ra_spike}"

# on_node <suffix> <Module> <Function> [ArgsTerm] [Timeout]
# Calls into a cluster node and prints the raw Erlang term.
# ~0p keeps the term on one line so the RESULT marker stays greppable.
on_node() {
  local suffix="$1" mod="$2" fun="$3" args="${4:-[]}" timeout="${5:-30000}"
  erl -sname "ra_drv_$suffix" -setcookie "$COOKIE" -pa $PA -noshell \
      -eval "Target = list_to_atom(\"ra_${suffix}@$HOST\"),
             io:format(\"RESULT ~0p~n\",
                 [rpc:call(Target, $mod, $fun, $args, $timeout)]),
             halt(0)." 2>&1 \
    | sed -n 's/^RESULT //p' \
    | tr '\n' ' '
}

# The caller passes a bare node suffix (1/2/3); the BEAM node is ra_n<suffix>.
alive() {
  [[ -n "$(node_pids "$1")" ]]
}

# node_pids <suffix> -> OS pids of the BEAM node named ra_n<suffix>
#
# Read /proc/<pid>/cmdline, NOT `ps ... | grep`. The shell running this script
# has the whole script in its own argv, so ANY ps/grep pattern matches this
# shell as well -- which made alive/1 report dead nodes as running and
# kill_node/1 silently do nothing. /proc cmdline has no such self-reference.
node_pids() {
  local pid args
  for pid in $(ls /proc 2>/dev/null | grep -E '^[0-9]+$'); do
    # Processes exit between ls and read, so a missing cmdline is normal.
    # Test readability first to keep stderr clean (the redirect error below
    # would otherwise leak "/proc/<pid>/cmdline: No such file or directory").
    [ -r "/proc/$pid/cmdline" ] || continue
    args=$(tr '\0' ' ' < "/proc/$pid/cmdline") || continue
    [ -n "$args" ] || continue
    case "$args" in
      *beam.smp*" -sname ra_n$1 "*)
        echo "$pid"
        ;;
    esac
  done
}

kill_node() {
  local suffix="$1"
  local pids
  pids=$(node_pids "$suffix")
  [ -z "$pids" ] && return 0
  # SIGKILL, not a graceful stop: we are testing crash recovery.
  echo "$pids" | while read -r p; do kill -9 "$p" 2>/dev/null; done
  sleep 3
}

cleanup() {
  # Kill by OS pid taken from "pid,args"; a ps/grep pattern would also match
  # this shell, whose argv contains the same "-sname ra_n1" string.
  for i in 1 2 3; do kill_node "$i"; done
  sleep 1
}

start_node() {
  local i="$1"
  rm -rf "$DATA_ROOT/n$i"
  mkdir -p "$DATA_ROOT/n$i"
  # ra reads its data dir from a -ra command line flag. Setting
  # application:set_env(ra, data_dir, ...) in the node's own code happens too
  # late: ra_env:data_dir/0 is consulted while the system starts, before any
  # of our code runs, so the directory silently stays at the cwd.
  erl -sname "ra_n$i" -setcookie "$COOKIE" -pa $PA -ra data_dir "\"$DATA_ROOT/n$i\"" -noshell \
      -eval "s0_1_cluster:node_main([\"$DATA_ROOT/n$i\",\"n$i\"]), halt(0)." \
      > "$DATA_ROOT/n$i.log" 2>&1 < /dev/null &
  disown 2>/dev/null || true
}

wait_up() {
  for i in 1 2 3; do
    for _ in $(seq 1 40); do
      [ -f "$DATA_ROOT/n$i/up" ] && break
      sleep 1
    done
    [ -f "$DATA_ROOT/n$i/up" ] \
      || { echo "FATAL: n$i down"; cat "$DATA_ROOT/n$i.log"; exit 1; }
  done
}

form_group() {
  # ONE call forms the whole group: ra:start_or_restart_cluster/5 dispatches
  # each server to its own node and bootstraps membership, which is what lets
  # an election happen. Calling it once per node does not work.
  echo -n "   form: "
  on_node n1 s0_1_cluster start_local '[meta]'
  echo
}

wait_leader() {
  # A fresh group needs a few election ticks before it accepts writes.
  for _ in $(seq 1 30); do
    if on_node n1 s0_1_driver members '[]' 20000 | grep -q '{ok'; then
      return 0
    fi
    sleep 2
  done
  echo "   WARN: members never reported a leader"
  return 1
}

# surviving <killed-suffix> -> first live node that is not the killed one
# surviving <killed-suffix> -> a live node other than the killed one.
# Returns the "nX" form used for node names.
surviving() {
  local i
  for i in 1 2 3; do
    if [ "$i" != "$1" ] && alive "$i"; then echo "n$i"; return 0; fi
  done
  # Only reachable if every other node is already gone.
  echo ""
}

# Extract the n-suffix (1/2/3) from a returned {meta,'ra_nX@host'} tuple.
node_suffix_of() { grep -oE 'ra_n[123]@' <<<"$1" | head -1 | sed 's/^ra_n//; s/@//'; }

run_scenarios() {
  echo "== phase 1: healthy =="
  HEALTHY=$(on_node n1 s0_1_driver healthy '[]' 30000)
  echo "   $HEALTHY"

  echo "== phase 2: replicated write cost (500 writes) =="
  COST=$(on_node n1 s0_1_driver cost '[500]' 120000)
  echo "   $COST"

  echo "== phase 3: kill the leader =="
  PLAN=$(on_node n1 s0_1_driver plan_leader '[]' 20000)
  echo "   plan: $PLAN"
  KILLED=$(node_suffix_of "$PLAN")
  echo "   killing $KILLED"
  kill_node "$KILLED"
  sleep 8
  SURV=$(surviving "$KILLED")
  echo "   surviving: $SURV"
  AFTER_LEADER=$(on_node "$SURV" s0_1_driver after_kill \
    "[{meta,list_to_atom(\"ra_n${KILLED}@$HOST\")}]" 40000)
  echo "   $AFTER_LEADER"

  echo "== phase 4: kill a follower =="
  # Phase 3 already killed one member and ra may still list it. Pick a victim
  # that is genuinely alive right now and is not the leader, so this phase
  # cannot silently re-kill a corpse or the node we query from.
  CUR_LEADER=$(on_node "$SURV" s0_1_driver members '[]' 20000 \
               | grep -oE 'ra_n[123]@[^}]+' | tail -1 | cut -d@ -f1 | sed 's/^ra_n//')
  KILLED2=""
  for cand in 1 2 3; do
    if [ "$cand" != "${KILLED}" ] && [ "$cand" != "${CUR_LEADER:-}" ] && alive "$cand"; then
      KILLED2="$cand"; break
    fi
  done
  if [ -z "$KILLED2" ]; then
    AFTER_FOLLOWER="skipped: no live follower to kill"
  else
    echo "   killing live follower $KILLED2 (leader is ${CUR_LEADER:-?})"
    kill_node "$KILLED2"
    sleep 8
    SURV2=$(surviving "$KILLED2")
    echo "   surviving: $SURV2"
    AFTER_FOLLOWER=$(on_node "$SURV2" s0_1_driver after_kill \
      "[{meta,list_to_atom(\"ra_n${KILLED2}@$HOST\")}]" 40000)
  fi
  echo "   $AFTER_FOLLOWER"

  echo "== phase 5: no majority (kill every other node) =="
  # SURV2 is "n2", so strip the n before comparing against the bare suffix.
  SURV2_SUFFIX="${SURV2#n}"
  for i in 1 2 3; do
    if [ "$i" != "$SURV2_SUFFIX" ]; then
      echo "   killing $i"
      kill_node "$i"
    fi
  done
  sleep 10
  echo "   surviving: $SURV2 (alone; majority lost)"
  NO_MAJ=$(on_node "$SURV2" s0_1_driver after_kill '[all_others]' 40000)
  echo "   $NO_MAJ"

  cat > "$DATA_ROOT/RESULTS.txt" <<EOF
=== S0.1 ra 3-node cluster results ===
1 healthy             : $HEALTHY
2 write cost          : $COST
3 after leader kill   : $AFTER_LEADER
4 after follower kill : $AFTER_FOLLOWER
5 no majority         : $NO_MAJ
EOF
  echo
  echo "results written to $DATA_ROOT/RESULTS.txt"
}

case "${1:-all}" in
  down) cleanup; echo "stopped" ;;
  # Bring the cluster up and leave it running so it can be poked at
  # interactively (./run_cluster.sh up, then query, then ./run_cluster.sh down)
  up)
    cleanup; mkdir -p "$DATA_ROOT"
    (cd "$SPIKE_DIR" && rebar3 compile) || exit 1
    for i in 1 2 3; do start_node "$i"; done
    wait_up
    form_group
    wait_leader || true
    echo "cluster up; data in $DATA_ROOT"
    ;;
  all)
    cleanup; mkdir -p "$DATA_ROOT"
    (cd "$SPIKE_DIR" && rebar3 compile) || exit 1
    echo "--- starting nodes"
    for i in 1 2 3; do start_node "$i"; done
    wait_up
    echo "--- forming raft group"
    form_group
    wait_leader || true
    echo "--- scenarios"
    run_scenarios
    ;;
  *) echo "usage: $0 {up|all|down}" >&2; exit 2 ;;
esac