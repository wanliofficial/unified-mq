#!/usr/bin/env python3
"""S6 Gate: GameDay fault drill (roadmap §8.3 item 3, README §11.3).

Four failure classes, each measured on the **real release** in the same
container shape as the T14 crash harness (host ext4 bind mount, OTP 28.3.1
image).  Every scenario reports a wall-clock recovery time and the invariant it
broke, so the report in `docs/benchmarks/` is evidence, not a claim.

  crash            kill -9 with a durable QoS1 message in flight -> restart,
                   measure seconds to the first CONNACK, then prove the message
                   came back and the subscription set survived.
  slow_consumer    a subscriber that never acks; the broker must stay bounded
                   and keep accepting publishes (reuses the S2 Gate 2 driver).
  disk_readonly    the data directory turns read-only mid-run; the broker must
                   not silently ACK (no PUBACK for a write it did not persist)
                   and must serve again once the disk is writable.
  partition        3-node Metadata Raft group: kill the Leader and measure the
                   time until a majority commits again (reuses the S4 dist
                   suite; this scenario drives it and records the result).

Usage:

    export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
    python3 tools/bench/s6_gameday.py --skip-build
    python3 tools/bench/s6_gameday.py --scenario crash --skip-build

The `partition` scenario shells out to `rebar3 ct` (peer nodes), so it needs the
build tree; the others only need the `t14` release and Docker.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "docker"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import mq_harness as H          # noqa: E402
import s2_bench as B            # noqa: E402
import s2_gate2 as G2           # noqa: E402

OUT_DIR = H.ROOT / "docs" / "benchmarks"
CONTAINER = H.CONTAINER


# ---------------------------------------------------------------------------
# crash
# ---------------------------------------------------------------------------

def scenario_crash():
    """kill -9 a node that acknowledged a QoS1 publish, then restart it.

    The subscriber is offline when the publish is acked, so exactly one durable
    message is waiting for it; this is R1a's shape (a connected subscriber would
    have already acked it, and correctly expects no redelivery).
    """
    B.stop_and_wipe()
    B.render_config(32, 4)
    H.start_broker("default")

    cid = f"gameday-crash-{os.getpid()}"
    filter_ = "gameday/crash/#"
    H.offline_subscriber(cid, filter_, 1)

    pub = H.Client(f"gameday-crash-pub-{os.getpid()}")
    pub.connect()
    payload = b"gameday-durable"
    pub.publish("gameday/crash/0", payload, qos=1)
    pub.close()

    t0 = time.monotonic()
    H.kill_broker("default")
    H.start_broker("default")           # waits for the first CONNACK
    recovery_s = round(time.monotonic() - t0, 3)

    sub, session_present = H.reconnect(cid)
    msg = sub.next_message(8.0)
    sub.close()

    result = {"scenario": "crash",
              "recovery_s": recovery_s,
              "session_present": session_present,
              "delivered_after_restart": bool(msg is not None),
              "payload_ok": bool(msg is not None and msg.payload == payload)}
    H.stop_broker("default")
    result["pass"] = bool(result["session_present"]
                          and result["delivered_after_restart"]
                          and result["payload_ok"])
    return result


# ---------------------------------------------------------------------------
# slow consumer
# ---------------------------------------------------------------------------

def scenario_slow_consumer():
    """Delegate to the S2 Gate 2 driver: one subscriber that never acks."""
    row = G2.scenario_slow_consumer()
    row = dict(row)
    row["scenario"] = "slow_consumer"
    return row


# ---------------------------------------------------------------------------
# disk read-only
# ---------------------------------------------------------------------------

def scenario_disk_readonly():
    """Mount the data directory read-only and prove the broker does not serve a
    write it cannot persist.

    A `chmod` is not a disk failure: an already-open WAL fd keeps writing.  A
    real read-only bind mount is EROFS for every process, which is what this
    drill uses.  Two outcomes both count as a pass, because both are "loud":

      * the node refuses to boot (the entrypoint's `mkdir`/bootstrap or the WAL
        open fails), or
      * it boots but a QoS1 publish is never ACKed.

    Either way there is no silent ACK for a write the disk could not take.  Then
    the volume is restored writable and the node must serve again.
    """
    B.stop_and_wipe()
    B.render_config(32, 4)
    H.start_broker("default")

    # A durable message before the disk goes read-only, so recovery after the
    # restore can be checked against real data.
    cid = f"gameday-disk-sub-{os.getpid()}"
    H.offline_subscriber(cid, "gameday/disk/#", 1)
    pub = H.Client(f"gameday-disk-pub-{os.getpid()}")
    pub.connect()
    pub.publish("gameday/disk/before", b"before", qos=1)
    pub.close()
    H.stop_broker("default")

    # Recreate the container with the data volume read-only.
    H.run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)
    ro_args = H.docker_args("default")
    ro_args = [f"{H.DATA}:{H.DATA_IN_CONTAINER}:ro"
               if a == f"{H.DATA}:{H.DATA_IN_CONTAINER}" else a
               for a in ro_args]
    H.run(["docker", "run", "-d", "--name", CONTAINER, *ro_args,
           "-p", f"127.0.0.1:{H.PORT}:1883", "--stop-timeout", "10",
           "--entrypoint", "/bin/sh", H.IMAGE, "/opt/s1/entrypoint.sh"],
          timeout=120)

    booted = False
    try:
        H.wait_port(open_timeout=15.0)
        booted = True
    except Exception:                   # noqa: BLE001 - refusing to boot is a pass
        booted = False

    ack_withheld = True
    error = None
    if booted:
        try:
            p = H.Client(f"gameday-disk-ro-pub-{os.getpid()}")
            p.connect()
            p.publish("gameday/disk/fail", b"after", qos=1, wait=True, timeout=6.0)
            ack_withheld = False       # a PUBACK arrived: silent degradation
            p.force_close()
        except Exception as exc:        # noqa: BLE001 - the timeout is the signal
            error = type(exc).__name__

    # Restore a writable volume and measure the time to serve again.
    H.run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)
    t0 = time.monotonic()
    H.start_broker("default")
    recovery_s = round(time.monotonic() - t0, 3)

    sub, session_present = H.reconnect(cid)
    msg = sub.next_message(8.0)
    sub.close()
    H.stop_broker("default")

    result = {"scenario": "disk_readonly",
              "refused_to_boot": not booted,
              "ack_withheld": ack_withheld,
              "first_error": error,
              "recovery_s": recovery_s,
              "session_present_after_restore": session_present,
              "durable_message_survived": bool(msg is not None
                                               and msg.payload == b"before")}
    result["pass"] = bool((not booted or ack_withheld)
                          and result["durable_message_survived"])
    return result


# ---------------------------------------------------------------------------
# network partition (delegated to the 3-node CT suite)
# ---------------------------------------------------------------------------

def scenario_partition():
    """Drive the S4 3-node Raft suite: Leader kill and majority loss.

    `peer` nodes live in the CT VM, so this is a `rebar3 ct` invocation rather
    than a Docker drill.  The scenario's recovery time is the suite's own
    failover measurement; here we record pass/fail and the suite duration.
    """
    env = os.environ.copy()
    otp_bin = "/home/wanli/Development/otp-28.3.1/bin"
    env["PATH"] = f"{otp_bin}:{env.get('PATH', '')}"
    # A previous suite leaves a mode-0400 `.erlang.cookie' under
    # `_build/test/extras'; `rebar3 ct' re-copies that tree and fails to
    # overwrite it.  The full-CT rule is to clear extras first, so do the same.
    shutil.rmtree(H.ROOT / "_build" / "test" / "extras", ignore_errors=True)
    t0 = time.monotonic()
    proc = subprocess.run(
        ["rebar3", "ct", "--suite",
         "apps/broker_cluster/test/broker_cluster_dist_SUITE"],
        cwd=str(H.ROOT), env=env, capture_output=True, text=True, timeout=1800)
    elapsed = round(time.monotonic() - t0, 1)
    out = proc.stdout + proc.stderr
    passed = proc.returncode == 0 and "All " in out and "failed" not in out.lower()
    result = {"scenario": "partition",
              "suite": "broker_cluster_dist_SUITE",
              "elapsed_s": elapsed,
              "exit_code": proc.returncode,
              "summary": _last_all_line(out)}
    result["pass"] = bool(passed)
    return result


def _last_all_line(text):
    for line in reversed(text.splitlines()):
        if line.strip().startswith("All ") or "tests passed" in line:
            return line.strip()
    return ""


# ---------------------------------------------------------------------------

SCENARIOS = {"crash": scenario_crash,
             "slow_consumer": scenario_slow_consumer,
             "disk_readonly": scenario_disk_readonly,
             "partition": scenario_partition}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", choices=sorted(SCENARIOS), default=None)
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    if not args.skip_build:
        H.ensure_release()

    names = [args.scenario] if args.scenario else list(SCENARIOS)
    results = []
    for name in names:
        print(f"== scenario {name}", flush=True)
        try:
            row = SCENARIOS[name]()
        except Exception as exc:        # noqa: BLE001 - record, do not abort the drill
            row = {"scenario": name, "pass": False, "error": repr(exc)}
        print(json.dumps(row, ensure_ascii=False), flush=True)
        results.append(row)

    if not args.scenario:
        B.stop_and_wipe()

    started = datetime.now(timezone.utc)
    report = {"generated_at": started.isoformat(),
              "host": B.host_facts(),
              "scenarios": results,
              "command": " ".join(sys.argv)}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = Path(args.out) if args.out else (
        OUT_DIR / f"s6-gameday-{started.strftime('%Y%m%d-%H%M%S')}.json")
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"== wrote {path}")
    if not all(row.get("pass") for row in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
