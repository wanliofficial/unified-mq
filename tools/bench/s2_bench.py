#!/usr/bin/env python3
"""S2-I benchmark driver: the four-dimension matrix (design §4.9).

The matrix is message size x WAL batch x partitions x subscriptions.  Every
cell runs against the **real release** in the same container shape as the T14
crash harness (host ext4 bind mount, OTP 28.3.1 image), so the numbers in
`docs/benchmarks/` are reproducible with the command printed at the end.

What is measured, and why:

  * **throughput (msg/s)** -- from the first QoS1 PUBLISH to the last PUBACK.
    QoS1 answers only after the WAL append (and the fsync when the tier asks
    for it), so this is the durable path, not a socket microbenchmark.
  * **latency p50/p99/p999 (ms)** -- every payload carries a publish timestamp;
    the subscriber timestamps arrival in its own callback thread.  The delta is
    end-to-end broker time on a real TCP connection.
  * **recovery time (s)** -- a warm ``kill -9`` followed by a restart, measured
    wall-clock to the first CONNACK (the listener is the last boot step).

Two matrices:

  quick (default)  13 cells: one sweep per dimension, everything else fixed.
  full             108 cells: the design's cartesian product.  1 MB x 256 batch
                   is capped in message count so a cell cannot write tens of GB.

Usage:

    export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
    python3 tools/bench/s2_bench.py                 # quick, writes JSON
    python3 tools/bench/s2_bench.py --full --skip-build
    python3 tools/bench/s2_bench.py --only 1KB/32/4/1 --skip-build
"""

import argparse
import json
import os
import re
import struct
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "docker"))

import mq_harness as H  # noqa: E402

ROOT = H.ROOT
OUT_DIR = ROOT / "docs" / "benchmarks"
TOPIC_PREFIX = "bench"
TOPIC_POOL = 64          # distinct destinations, so partitions are actually hit
PIPELINE = 200           # in-flight publishes before waiting for the acks
# One publisher connection serialises its appends (the conn FSM waits for the
# writer before it reads the next PUBLISH), so a single-connection cell measures
# fsync latency, not group commit.  The durable-throughput cells therefore use
# 8 concurrent publishers -- the same "append only enters the partition writer"
# path, with enough producers for the batch window to actually fill.  The
# connection-scaling curve (1/4/8/16) is reported separately by
# `--publisher-sweep`.
PUBLISHERS = 8

SIZES = [64, 1024, 65536, 1048576]
BATCHES = [1, 32, 256]
PARTITIONS = [1, 4, 16]
SUBSCRIPTIONS = [1, 10, 100]

# Per-cell message count: bounded bytes, not bounded time.  A 1 MB cell writes
# 100 MB to the log; a 64 B cell can afford far more samples.
def message_count(size, subs):
    base = {64: 4000, 1024: 4000, 65536: 400, 1048576: 100}[size]
    if subs >= 100:
        return 200
    if subs >= 10:
        return 500
    return base


class BenchClient(H.Client):
    """A harness client that timestamps arrivals in the paho callback thread.

    Stamping at collection time instead would measure the Python queue, not the
    broker.  The publisher side is the stock client: `on_publish' is its PUBACK.
    """

    def _on_message(self, _client, _userdata, msg):
        self._messages.put((time.time_ns(), msg))

    def drain(self, n, timeout=10.0):
        out = []
        deadline = time.monotonic() + timeout
        while len(out) < n:
            left = deadline - time.monotonic()
            if left <= 0:
                break
            try:
                out.append(self._messages.get(timeout=min(left, 0.5)))
            except Exception:
                pass
        return out

    def drain_quiet(self, quiet=0.5, total=120.0):
        """Collect until the socket has been quiet for `quiet' seconds.

        Used between the throughput and latency phases: the latency number is
        only meaningful once the throughput backlog has drained, and a fixed
        count would time out on the fan-out cells.
        """
        out = []
        start = time.monotonic()
        last = start
        while time.monotonic() - start < total:
            try:
                item = self._messages.get(timeout=0.05)
            except Exception:
                if time.monotonic() - last >= quiet:
                    break
            else:
                out.append(item)
                last = time.monotonic()
        return out


def render_config(batch, partitions):
    """Render the shipped sys.config with the cell's knobs, asserting each one.

    Same one-source-of-truth rule as the T14 harness: a knob that disappeared
    from config/sys.config.src must fail here rather than silently run the
    previous cell's value.
    """
    text = H.substitute_data_dirs(
        (ROOT / "config" / "sys.config.src").read_text())

    def sub_once(pattern, repl, count=1):
        nonlocal text
        text, n = re.subn(pattern, repl, text, count=count)
        if n != count:
            raise RuntimeError(f"sys.config render: {pattern!r} matched {n}, "
                               f"expected {count}")

    sub_once(r'"log/s1\.log"', '"%s/log/s1.log"' % H.DATA_IN_CONTAINER)
    # Two independent partition knobs (router and queue) must agree.
    sub_once(r'\{partitions,\s*\d+\}', "{partitions, %d}" % partitions, count=2)
    # The WAL policy is a map: `batch_size => 32', not a tuple.
    sub_once(r'batch_size\s*=>\s*\d+', "batch_size => %d" % batch)
    sub_once(r'max_batch_size\s*=>\s*\d+',
             "max_batch_size => %d" % max(256, batch))
    H.RUN.mkdir(parents=True, exist_ok=True)
    (H.RUN / "sys.config").write_text(text)
    (H.RUN / "vm.args").write_text((ROOT / "config" / "vm.args.src").read_text())


def stop_and_wipe():
    H.run(["docker", "rm", "-f", H.CONTAINER], check=False, timeout=60)
    H.clear_data()


def fresh(batch, partitions):
    stop_and_wipe()
    render_config(batch, partitions)
    H.start_broker("default")


def payload_for(size, seq):
    header = struct.pack("!QQ", time.time_ns(), seq)
    if size < len(header):
        raise ValueError("payload size below the 16-byte header")
    return header + b"x" * (size - len(header))


def run_cell(size, batch, partitions, subs, publishers=PUBLISHERS):
    n = message_count(size, subs)
    fresh(batch, partitions)
    subscribers = [BenchClient(f"bench-sub-{i}-{os.getpid()}")
                   for i in range(subs)]
    pubs = [H.Client(f"bench-pub-{i}-{os.getpid()}")
            for i in range(max(1, publishers))]
    try:
        for c in subscribers:
            c.connect()
            c.subscribe(f"{TOPIC_PREFIX}/#", qos=1)
        for p in pubs:
            p.connect()
        topics = [f"{TOPIC_PREFIX}/{i}" for i in range(min(TOPIC_POOL, n))]

        # Warm one message through the whole path so the first sample does not
        # pay for connection setup / lazy allocations.
        pubs[0].publish(topics[0], payload_for(size, 0), qos=1)
        for c in subscribers:
            c.drain(1, timeout=5.0)

        t0 = time.monotonic()
        for start in range(0, n, PIPELINE):
            pending = {p: [] for p in pubs}
            for seq in range(start, min(start + PIPELINE, n)):
                p = pubs[seq % len(pubs)]
                pending[p].append(
                    p.publish(topics[seq % len(topics)],
                              payload_for(size, seq), qos=1, wait=False))
            for p, mids in pending.items():
                if mids:
                    p.wait_pubacks(mids, timeout=180.0)
        elapsed = time.monotonic() - t0
        # Drain the throughput backlog before the latency phase: a queued
        # backlog would otherwise make queueing delay look like broker latency.
        # Waiting for exactly the messages published is deterministic (this
        # subscriber receives every one of them); a quiet-window heuristic can
        # stop in a scheduling gap and poison the latency sample.
        subscribers[0].drain(n, timeout=300.0)

        # Latency phase: one publisher, spaced so each sample finds an empty
        # pipeline.  p50/p99/p999 here are end-to-end broker latency.  The
        # sample count is bounded by bytes: 1 MB x 300 would be 300 MB.
        lat_n = {64: 300, 1024: 300, 65536: 100, 1048576: 20}[size]
        if subs >= 100:
            lat_n = max(20, lat_n // 3)
        spacing = 0.005 if size <= 65536 else 0.05
        mids = []
        for seq in range(lat_n):
            mids.append(pubs[0].publish(topics[seq % len(topics)],
                                        payload_for(size, seq + n),
                                        qos=1, wait=False))
            time.sleep(spacing)
        pubs[0].wait_pubacks(mids, timeout=300.0)
        samples = subscribers[0].drain(lat_n, timeout=60.0)
        latencies = []
        for arrived_ns, msg in samples:
            try:
                sent_ns, _seq = struct.unpack("!QQ", msg.payload[:16])
            except struct.error:
                continue
            latencies.append(max(0.0, (arrived_ns - sent_ns) / 1e6))
        latencies.sort()

        return {
            "size_bytes": size,
            "batch": batch,
            "partitions": partitions,
            "subscriptions": subs,
            "publishers": len(pubs),
            "messages": n,
            "elapsed_s": round(elapsed, 3),
            "throughput_msg_s": round(n / elapsed, 1) if elapsed > 0 else None,
            "latency_ms": quantiles(latencies),
            "delivered": len(samples),
        }
    finally:
        for c in subscribers:
            c.close()
        for p in pubs:
            try:
                p.close()
            except Exception:
                pass


def quantiles(sorted_values):
    if not sorted_values:
        return {"p50": None, "p99": None, "p999": None, "max": None}

    def pick(p):
        idx = min(len(sorted_values) - 1,
                  max(0, int(round(p * (len(sorted_values) - 1)))))
        return round(sorted_values[idx], 2)

    return {"p50": pick(0.50), "p99": pick(0.99), "p999": pick(0.999),
            "max": round(sorted_values[-1], 2)}


def measure_recovery(size=1024, batch=32, partitions=4, subs=1):
    """Warm `kill -9` + restart: wall clock from start to the first CONNACK."""
    cell = run_cell(size, batch, partitions, subs)
    # The data directory now holds a real log; kill without a goodbye.
    H.kill_broker("default")
    t0 = time.monotonic()
    H.start_broker("default")          # start_broker waits for a CONNACK
    seconds = time.monotonic() - t0
    H.stop_broker("default")
    return {**cell, "recovery_s": round(seconds, 3)}


def quick_matrix():
    cells = []
    cells += [(s, 32, 4, 1) for s in SIZES]
    cells += [(1024, b, 4, 1) for b in BATCHES]
    cells += [(1024, 32, p, 1) for p in PARTITIONS]
    cells += [(1024, 32, 4, n) for n in SUBSCRIPTIONS]
    seen, unique = set(), []
    for cell in cells:
        if cell not in seen:
            seen.add(cell)
            unique.append(cell)
    return unique


def full_matrix():
    return [(s, b, p, n) for s in SIZES for b in BATCHES
            for p in PARTITIONS for n in SUBSCRIPTIONS]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--full", action="store_true",
                    help="the full 4x3x3x3 matrix (long; ~108 cells)")
    ap.add_argument("--only", default=None,
                    help="a single cell as SIZE/BATCH/PARTITIONS/SUBS")
    ap.add_argument("--skip-build", action="store_true")
    ap.add_argument("--no-recovery", action="store_true")
    ap.add_argument("--publisher-sweep", action="store_true",
                    help="1/4/8/16 publisher connections at 1KB/32/4/1")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    if not args.skip_build:
        H.ensure_release()

    if args.publisher_sweep:
        cells = []
        for pubs in (1, 4, 8, 16):
            print(f"== publisher sweep: {pubs} connections", flush=True)
            row = run_cell(1024, 32, 4, 1, publishers=pubs)
            row.update({"size_bytes": 1024, "batch": 32, "partitions": 4,
                        "subscriptions": 1})
            cells.append(row)
            print(f"   {row['throughput_msg_s']} msg/s "
                  f"p50={row['latency_ms']['p50']}ms", flush=True)
        stop_and_wipe()
        write_report(cells, None, args, "publisher-sweep")
        print_markdown(cells, None)
        return

    if args.only:
        size, batch, parts, subs = (int(x) for x in args.only.split("/"))
        cells = [(size, batch, parts, subs)]
    else:
        cells = full_matrix() if args.full else quick_matrix()

    results = []
    started = datetime.now(timezone.utc)
    for i, cell in enumerate(cells, 1):
        size, batch, parts, subs = cell
        print(f"== [{i}/{len(cells)}] size={size}B batch={batch} "
              f"partitions={parts} subs={subs}", flush=True)
        try:
            row = run_cell(*cell)
        except Exception as exc:                       # noqa: BLE001
            print(f"   FAILED: {exc}", flush=True)
            row = {"size_bytes": size, "batch": batch, "partitions": parts,
                   "subscriptions": subs, "error": str(exc)}
        row.update({"size_bytes": size, "batch": batch,
                    "partitions": parts, "subscriptions": subs})
        results.append(row)
        print(f"   {row.get('throughput_msg_s')} msg/s, "
              f"p50={row.get('latency_ms', {}).get('p50')}ms, "
              f"p99={row.get('latency_ms', {}).get('p99')}ms", flush=True)

    recovery = None
    if not args.no_recovery and not args.only:
        print("== recovery (kill -9 + restart)", flush=True)
        try:
            recovery = measure_recovery()
            print(f"   {recovery['recovery_s']} s to the first CONNACK",
                  flush=True)
        except Exception as exc:                       # noqa: BLE001
            print(f"   FAILED: {exc}", flush=True)
            recovery = {"error": str(exc)}
    stop_and_wipe()
    write_report(results, recovery, args,
                 "full" if args.full else
                 ("single" if args.only else "quick"))
    print_markdown(results, recovery)


def write_report(cells, recovery, args, matrix):
    started = datetime.now(timezone.utc)
    report = {
        "generated_at": started.isoformat(),
        "host": host_facts(),
        "matrix": matrix,
        "cells": cells,
        "recovery": recovery,
        "command": " ".join(sys.argv),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = started.strftime("%Y%m%d-%H%M%S")
    path = Path(args.out) if args.out else OUT_DIR / f"s2-bench-{stamp}.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(f"== wrote {path}", flush=True)
    return path


def host_facts():
    import subprocess
    def sh(cmd):
        try:
            return subprocess.run(cmd, shell=True, capture_output=True,
                                  text=True, timeout=10).stdout.strip()
        except Exception:
            return "unknown"
    return {
        "cpu": sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").strip(),
        "cores": os.cpu_count(),
        "memory": sh("grep MemTotal /proc/meminfo"),
        "kernel": sh("uname -r"),
        "filesystem": sh(f"df -T {ROOT} | tail -1"),
        "otp": sh("erl -noshell -eval 'io:format(\"~s/~s\","
                  "[erlang:system_info(otp_release),"
                  "erlang:system_info(version)]),halt().'"),
    }


def print_markdown(cells, recovery):
    print("\n| size | batch | parts | subs | msg/s | p50 ms | p99 ms | p999 ms |")
    print("|---|---|---|---|---|---|---|---|")
    for r in cells:
        lat = r.get("latency_ms") or {}
        print(f"| {r.get('size_bytes')} | {r.get('batch')} | "
              f"{r.get('partitions')} | {r.get('subscriptions')} | "
              f"{r.get('throughput_msg_s')} | {lat.get('p50')} | "
              f"{lat.get('p99')} | {lat.get('p999')} |")
    if recovery:
        print(f"\nrecovery: {recovery.get('recovery_s')} s "
              f"({recovery.get('error', 'ok')})")


if __name__ == "__main__":
    main()
