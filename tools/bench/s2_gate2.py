#!/usr/bin/env python3
"""S2-I / Gate S2 item 2: the three pressure scenarios, with RSS sampled.

Gate S2 says "a slow consumer / hot partition must not cause OOM".  S2-G makes
the path *bounded* (pull-round permit, `max_pending_appends`, per-partition
rotation); this driver is the measurement that the bound holds on the real
release:

  slow-consumer     one subscriber that never acks, 20k QoS1 publishes at 1 KiB
  hot-partition     one deep backlog on a hot topic while a cold topic has to
                    stay deliverable
  connection-storm  300 simultaneous CONNECTs

Every scenario samples the container's cgroup memory (`memory.current`) while
it runs.  The assertions are deliberately loose -- "bounded, alive, and the
cold path still moves" -- because the exact number belongs in the report, not
in a threshold that would turn a slow CI host into a failure.

Usage:

    export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
    python3 tools/bench/s2_gate2.py --skip-build
    python3 tools/bench/s2_gate2.py --scenario slow_consumer --skip-build
"""

import argparse
import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "docker"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import mq_harness as H          # noqa: E402
import s2_bench as B            # noqa: E402

OUT_DIR = H.ROOT / "docs" / "benchmarks"
MB = 1024 * 1024


def mem_now():
    """The container's cgroup v2 usage, in bytes."""
    out = H.run(["docker", "exec", H.CONTAINER, "cat",
                 "/sys/fs/cgroup/memory.current"], check=False, timeout=30)
    try:
        return int(out.stdout.strip())
    except ValueError:
        # cgroup v1 fallback
        out = H.run(["docker", "exec", H.CONTAINER, "cat",
                     "/sys/fs/cgroup/memory/memory.usage_in_bytes"],
                    check=False, timeout=30)
        return int(out.stdout.strip())


class Sampler(threading.Thread):
    def __init__(self, interval=0.2):
        super().__init__(daemon=True)
        self.interval = interval
        self.samples = []
        self._done = threading.Event()

    def run(self):
        while not self._done.is_set():
            try:
                self.samples.append((time.monotonic(), mem_now()))
            except Exception:
                pass
            self._done.wait(self.interval)

    def stop(self):
        self._done.set()
        self.join(timeout=5)

    def summary(self):
        values = [b for _t, b in self.samples]
        if not values:
            return {}
        return {"baseline_mb": round(values[0] / MB, 1),
                "peak_mb": round(max(values) / MB, 1),
                "growth_mb": round((max(values) - values[0]) / MB, 1),
                "samples": len(values)}


def scenario_slow_consumer(publishers=8, total=20000):
    """A subscriber that never acks: the delivery window stays full, the pull
    round pauses, and the broker must not copy the backlog into the process."""
    B.stop_and_wipe()
    B.render_config(32, 4)
    H.start_broker("default")
    sub = B.BenchClient("gate2-slow", manual_ack=True)
    sub.connect()
    sub.subscribe("bench/#", qos=1)
    pubs = [H.Client(f"gate2-pub-{i}") for i in range(publishers)]
    for p in pubs:
        p.connect()
    sampler = Sampler()
    base = mem_now()
    sampler.start()
    t0 = time.monotonic()
    mids = []
    for i in range(total):
        p = pubs[i % publishers]
        mids.append(p.publish(f"bench/{i % 64}", B.payload_for(1024, i),
                              qos=1, wait=False))
        if i % 2000 == 0:
            # Keep the producer pipeline bounded without draining the consumer.
            p.wait_pubacks(mids[-2000:], timeout=120)
    for p in pubs:
        p.wait_pubacks([m for j, m in enumerate(mids)
                        if pubs[j % publishers] is p], timeout=180)
    elapsed = time.monotonic() - t0
    sampler.stop()
    pub_ok = True
    try:
        # The broker must still accept work; a fan-out that filled memory would
        # have stopped answering here.
        pubs[0].publish("bench/alive", b"alive", qos=1)
    except Exception:
        pub_ok = False
    result = {"scenario": "slow_consumer", "published": len(mids),
              "elapsed_s": round(elapsed, 2),
              "broker_alive": pub_ok,
              "delivered_to_slow_consumer": len(sub.drain(0, timeout=0.1)),
              **sampler.summary()}
    sub.close()
    for p in pubs:
        p.close()
    H.stop_broker("default")
    result["pass"] = bool(pub_ok and result.get("growth_mb", 999) < 256)
    return result


def scenario_hot_partition(publishers=8, total=20000):
    """A deep hot backlog must not stop another partition from advancing."""
    B.stop_and_wipe()
    B.render_config(32, 4)
    H.start_broker("default")
    hot = B.BenchClient("gate2-hot")
    hot.connect()
    hot.subscribe("hot/#", qos=1)
    cold = B.BenchClient("gate2-cold")
    cold.connect()
    cold.subscribe("cold/#", qos=1)
    # The cold topic only has to be a *different* destination; the assertion is
    # that it is delivered while the hot backlog is still deep, not which
    # partition the hash chose.
    cold_topic = "cold/0"
    pubs = [H.Client(f"gate2-hot-pub-{i}") for i in range(publishers)]
    for p in pubs:
        p.connect()
    sampler = Sampler()
    sampler.start()
    t0 = time.monotonic()
    mids = []
    for i in range(total):
        p = pubs[i % publishers]
        mids.append(p.publish("hot/x", B.payload_for(1024, i),
                              qos=1, wait=False))
        if i % 2000 == 0:
            p.wait_pubacks(mids[-2000:], timeout=120)
    for p in pubs:
        p.wait_pubacks([m for j, m in enumerate(mids)
                        if pubs[j % publishers] is p], timeout=180)
    hot_elapsed = time.monotonic() - t0
    # The hot consumer is deliberately left unacked; the cold publish is the
    # probe.  It must arrive while the hot backlog is still being delivered.
    cold_t0 = time.monotonic()
    pubs[0].publish(cold_topic, b"cold-probe", qos=1)
    got = cold.drain(1, timeout=10.0)
    cold_latency = time.monotonic() - cold_t0 if got else None
    sampler.stop()
    result = {"scenario": "hot_partition",
              "published_hot": len(mids),
              "hot_elapsed_s": round(hot_elapsed, 2),
              "cold_latency_s": round(cold_latency, 3) if cold_latency else None,
              "cold_delivered": bool(got),
              **sampler.summary()}
    hot.close()
    cold.close()
    for p in pubs:
        p.close()
    H.stop_broker("default")
    result["pass"] = bool(got and cold_latency is not None
                          and cold_latency < 1.0
                          and result.get("growth_mb", 999) < 256)
    return result


def scenario_connection_storm(count=300):
    """300 simultaneous CONNECTs: the accept pool must not be a single point of
    failure, and the connection count must not cost unbounded memory."""
    B.stop_and_wipe()
    B.render_config(32, 4)
    H.start_broker("default")
    sampler = Sampler()
    sampler.start()
    results = []
    lock = threading.Lock()

    def one(i):
        c = H.Client(f"gate2-storm-{i}")
        try:
            c.connect(timeout=15.0)
            ok = True
        except Exception:
            ok = False
        finally:
            c.close()
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=one, args=(i,)) for i in range(count)]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    elapsed = time.monotonic() - t0
    sampler.stop()
    result = {"scenario": "connection_storm", "attempted": count,
              "connected": sum(1 for ok in results if ok),
              "elapsed_s": round(elapsed, 2),
              "broker_alive": H.broker_running(),
              **sampler.summary()}
    H.stop_broker("default")
    result["pass"] = bool(result["connected"] == count
                          and result["broker_alive"]
                          and result.get("growth_mb", 999) < 256)
    return result


SCENARIOS = {"slow_consumer": scenario_slow_consumer,
             "hot_partition": scenario_hot_partition,
             "connection_storm": scenario_connection_storm}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scenario", choices=sorted(SCENARIOS), default=None)
    ap.add_argument("--skip-build", action="store_true")
    args = ap.parse_args()

    if not args.skip_build:
        H.ensure_release()

    names = [args.scenario] if args.scenario else list(SCENARIOS)
    results = [SCENARIOS[name]() for name in names]
    B.stop_and_wipe()

    started = datetime.now(timezone.utc)
    report = {"generated_at": started.isoformat(),
              "host": B.host_facts(),
              "scenarios": results,
              "command": " ".join(sys.argv)}
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / f"s2-gate2-{started.strftime('%Y%m%d-%H%M%S')}.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    for row in results:
        print(json.dumps(row, ensure_ascii=False))
    print(f"== wrote {path}")
    if not all(row.get("pass") for row in results):
        sys.exit(1)


if __name__ == "__main__":
    main()
