#!/usr/bin/env python3
"""T14: docker crash-recovery driver for the S1 MQTT slice (design §8).

The broker is a real release node in a container; this script owns the MQTT
clients on the host and injects `kill -9` through docker-compose.  It covers
R1a, R1b, R2, R3, R4, R5, R6 and R7 from §8.2, and runs the default and the
near-zero-regress cursor tiers (R3/R7 explicitly require both).

Usage:
    python3 tools/docker/s1_recovery.py                 # all scenarios, both tiers
    python3 tools/docker/s1_recovery.py --tier default  # one tier
    python3 tools/docker/s1_recovery.py --scenario r3 --tier zero --keep-data

The container/probe/client harness lives in mq_harness.py (imported below); this
module keeps only the R1a..R7 scenarios and the CLI.  The release must be
buildable with the host's OTP 28.3.1; the image `erlang:28.3.1.0` supplies the
matching ERTS (16.2), so no host binaries are copied into the container.
"""

# Constants, container lifecycle, config rendering, the one-shot Erlang probes,
# the paho client and the scenario assertion helpers all live in the shared
# harness now; `import *` re-exports them (they are all public, non-`_` names)
# so the scenarios below can keep referring to them unqualified.
from mq_harness import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# Scenarios (design §8.2)
# ---------------------------------------------------------------------------

def scenario_r1a(tier):
    """PUBACK received -> kill -9 -> the message must come back from the WAL."""
    cid = f"s1t14-r1a-{tier}-sub"
    filter_ = "t14/r1a/#"
    payload = b"r1a-durable"
    subs_bytes = offline_subscriber(cid, filter_, 1)
    pub = Client(f"s1t14-r1a-{tier}-pub")
    pub.connect()
    pub.publish("t14/r1a/0", payload, 1)
    pub.close()

    kill_broker(tier)
    wal = probe_recover(tier)
    assert_true(sum(h["high"] - 1 for h in wal["highs"].values()) == 1,
                f"R1a: expected exactly one WAL record, got {wal['highs']}")
    start_broker(tier)

    sub, session_present = reconnect(cid)
    assert_true(session_present, "R1a: session_present must be 1 after kill")
    msg = sub.next_message(8.0)
    assert_true(msg is not None and msg.payload == payload,
                f"R1a: got {msg.payload if msg else None!r}")
    sub.close()
    return {"payload": payload.decode(), "session_present": session_present,
            "subs_bytes": subs_bytes, "wal": wal["highs"]}


def scenario_r1b(tier):
    """The subscription *set* survives kill -9; no SUBSCRIBE is replayed."""
    cid = f"s1t14-r1b-{tier}-sub"
    filter_ = "t14/r1b/#"
    sub = Client(cid, clean=False)
    sub.connect()
    sub.subscribe(filter_, 1)
    subs_before = subs_path(cid).stat().st_size

    kill_broker(tier)
    sub.force_close()
    start_broker(tier)
    assert_true(subs_path(cid).exists(), "R1b: .subs disappeared across the crash")

    sub2, session_present = reconnect(cid)
    assert_true(session_present, "R1b: session_present must be 1")
    # Deliberately no SUBSCRIBE here: the restored set has to be live.
    pub = Client(f"s1t14-r1b-{tier}-pub")
    pub.connect()
    pub.publish("t14/r1b/0", b"r1b-after-crash", 1)
    pub.close()
    msg = sub2.next_message(8.0)
    assert_true(msg is not None and msg.payload == b"r1b-after-crash",
                f"R1b: restored subscription did not deliver: {msg}")
    sub2.close()
    return {"session_present": session_present, "subs_bytes": subs_before,
            "subs_after": subs_path(cid).stat().st_size}


def scenario_r2(tier):
    """Kill while QoS1 publishes are in flight: a complete prefix or nothing --
    never a torn record."""
    cid = f"s1t14-r2-{tier}-sub"
    offline_subscriber(cid, "t14/r2/#", 1)
    pub = Client(f"s1t14-r2-{tier}-pub")
    pub.connect()
    n = 200
    # The first 10 are fully PUBACKed, so they must survive.  The rest race the
    # kill: some may not reach the WAL at all, but whatever is recovered has to
    # form a gap-free prefix of the publish sequence.
    pub.publish_many([f"t14/r2/0" for _ in range(10)],
                     [f"msg-{i:04d}".encode() for i in range(10)], qos=1)
    for i in range(10, n):
        pub.publish("t14/r2/0", f"msg-{i:04d}".encode(), 1, wait=False)
    kill_broker(tier)
    pub.force_close()

    start_broker(tier)
    sub, session_present = reconnect(cid)
    assert_true(session_present, "R2: session_present must be 1")
    msgs = sub.collect(quiet=0.8, total=6.0)
    idx = payload_indices(msgs)
    assert_true(idx == sorted(idx), f"R2: out-of-order delivery {idx[:20]}")
    assert_true(idx == list(range(len(idx))),
                f"R2: recovered set is not a gap-free prefix: {idx[:20]}...")
    assert_true(len(idx) >= 10, f"R2: acked prefix lost (got {len(idx)})")
    assert_true(len(set(idx)) == len(idx), "R2: duplicates inside one delivery")
    sub.close()
    return {"acked_prefix": 10, "published": n, "recovered": len(idx)}


def scenario_r3(tier):
    """Acked-then-killed: cursor may regress by at most K; no acked record with
    offset > last flushed may be lost.  Zero tier must show no duplicates."""
    cid = f"s1t14-r3-{tier}-sub"
    filter_ = "t14/r3/#"
    topic = "t14/r3/0"
    # 50 keeps the burst below one 100 ms flush tick.  The default tier can then
    # actually lose the tick to kill -9 (duplicates allowed), while the zero tier
    # flushes every ack synchronously and must not.
    n = 50
    sub = Client(cid, clean=False, manual_ack=True)
    sub.connect()
    sub.subscribe(filter_, 1)
    pub = Client(f"s1t14-r3-{tier}-pub")
    pub.connect()
    pub.publish_many([topic] * n, [f"msg-{i:04d}".encode() for i in range(n)])
    pub.close()

    # max_inflight=64 means the broker will not send the whole batch before an
    # ack frees a slot, so R3 has to ack as the messages arrive.  That is also
    # what makes the crash window real: the last ack is only milliseconds old
    # when kill -9 lands in the default tier.
    msgs = []
    deadline = time.monotonic() + 20.0
    while len(msgs) < n and time.monotonic() < deadline:
        msg = sub.next_message(timeout=0.5)
        if msg is not None:
            msgs.append(msg)
            sub.ack(msg)  # manual ack: the test owns the instant it becomes durable
    assert_true(len(msgs) == n, f"R3: subscriber got {len(msgs)}/{n}")
    assert_true(payload_indices(msgs) == list(range(n)), "R3: delivery not in offset order")
    # Zero tier must have flushed each ack (interval 0 / regress 1).  Default
    # tier kills immediately: whatever the last 100 ms tick did not persist
    # must come back as replay, never as loss.
    time.sleep(0.3 if tier == "zero" else 0.0)

    # No DISCONNECT: a graceful detach may flush the cursor and hide the very
    # regression window this scenario exists to measure.
    kill_broker(tier)
    sub.force_close()
    cursor = probe_cursors(cid, tier)
    offsets = [c["offset"] for c in cursor["cursors"]]
    flushed = max(offsets) if offsets else 1
    assert_true(1 <= flushed <= n + 1,
                f"R3: cursor {flushed} outside [1, {n + 1}]")

    start_broker(tier)
    sub2, session_present = reconnect(cid, manual_ack=True)
    assert_true(session_present, "R3: session_present must be 1")
    replay = sub2.collect(quiet=0.6, total=8.0)
    for msg in replay:
        sub2.ack(msg)
    idx = payload_indices(replay)
    required = set(range(flushed - 1, n))  # offsets flushed..n, 0-based
    missing = sorted(required - set(idx))
    assert_true(not missing,
                f"R3: acked offsets after flushed cursor lost: {missing[:10]}")
    duplicates = len(idx) - len(set(idx))
    if tier == "zero":
        assert_true(flushed == n + 1,
                    f"R3 zero: cursor regressed to {flushed}, expected {n + 1}")
        assert_true(len(replay) == 0,
                    f"R3 zero: {len(replay)} duplicate delivery/deliveries")
    sub2.close()
    return {"flushed_cursor": flushed, "required_replay": len(required),
            "replayed": len(replay), "duplicates": duplicates,
            "session_present": session_present}


def scenario_r4(tier):
    """A torn tail is truncated; every fsynced record before it stays readable."""
    cid = f"s1t14-r4-{tier}-sub"
    n = 20
    offline_subscriber(cid, "t14/r4/#", 1)
    pub = Client(f"s1t14-r4-{tier}-pub")
    pub.connect()
    pub.publish_many(["t14/r4/0"] * n,
                     [f"msg-{i:04d}".encode() for i in range(n)])
    pub.close()

    kill_broker(tier)
    # Every partition has a segment file, most of them empty; only the one that
    # received the topic has bytes to tear.
    segments = [p for p in glob.glob(str(DATA / "wal" / "*" / "segment-*.log"))
                if Path(p).stat().st_size > 0]
    assert_true(len(segments) == 1, f"R4: expected one non-empty segment, got {segments}")
    seg = Path(segments[0])
    size_before = seg.stat().st_size
    with seg.open("ab") as fh:
        fh.write(b"\x55\x01")  # magic + version, then the header ends mid-write
    size_torn = seg.stat().st_size
    assert_true(size_torn == size_before + 2, "R4: tear was not written")

    start_broker(tier)
    size_after = seg.stat().st_size
    assert_true(size_after == size_before,
                f"R4: torn tail not truncated ({size_after} != {size_before})")

    sub, session_present = reconnect(cid)
    assert_true(session_present, "R4: session_present must be 1")
    msgs = sub.collect(quiet=0.6, total=8.0)
    idx = payload_indices(msgs)
    assert_true(idx == list(range(n)),
                f"R4: recovered records changed: {idx}")
    assert_true(len(msgs) == n, f"R4: phantom records after torn tail: {len(msgs)}")
    sub.close()
    return {"segment": seg.name, "size_before": size_before,
            "size_torn": size_torn, "size_after": size_after,
            "recovered": len(idx)}


def scenario_r5(tier):
    """QoS0 is allowed to vanish; it must never turn into a stored phantom."""
    cid = f"s1t14-r5-{tier}-sub"
    offline_subscriber(cid, "t14/r5/#", 0)
    pub = Client(f"s1t14-r5-{tier}-pub")
    pub.connect()
    for i in range(10):
        pub.publish("t14/r5/0", f"msg-{i:04d}".encode(), 0, wait=False)
    pub.close()
    kill_broker(tier)

    wal = probe_recover(tier)
    stored = sum(h["high"] - 1 for h in wal["highs"].values())
    assert_true(stored == 0, f"R5: QoS0 wrote {stored} WAL record(s)")

    start_broker(tier)
    sub, session_present = reconnect(cid)
    assert_true(session_present, "R5: session_present must be 1")
    msgs = sub.collect(quiet=0.8, total=3.0)
    assert_true(not msgs, f"R5: QoS0 phantom delivery after restart: {len(msgs)}")
    sub.close()
    return {"published": 10, "stored": stored, "delivered": len(msgs)}


def scenario_r6(tier):
    """20k stored records must replay in under 500 ms (measured recovery only,
    not VM/container startup)."""
    n = 20000
    pub = Client(f"s1t14-r6-{tier}-pub")
    pub.connect()
    topics = [f"t14/r6/{i}" for i in range(n)]
    payloads = [f"msg-{i:05d}".encode() for i in range(n)]
    pub.publish_many(topics, payloads, qos=1, timeout=300.0)
    pub.close()
    kill_broker(tier)

    wal = probe_recover(tier, timeout=300)
    ms = wal["micros"] / 1000.0
    records = sum(h["high"] - 1 for h in wal["highs"].values())
    assert_true(records == n, f"R6: stored {records}/{n} records")
    assert_true(ms < R6_BUDGET_MS,
                f"R6: replay {ms:.1f} ms >= {R6_BUDGET_MS:.0f} ms budget")

    start_broker(tier)
    assert_true(broker_running(), "R6: broker did not come back after replay")
    return {"published": n, "stored": records, "recovery_ms": round(ms, 3),
            "budget_ms": R6_BUDGET_MS, "wal": wal["highs"]}


def scenario_r7(tier):
    """Delivered-but-unacked in-flight may regress, but must be resent and must
    never be lost."""
    cid = f"s1t14-r7-{tier}-sub"
    payload = b"r7-inflight"
    sub = Client(cid, clean=False, manual_ack=True)
    sub.connect()
    sub.subscribe("t14/r7/#", 1)
    pub = Client(f"s1t14-r7-{tier}-pub")
    pub.connect()
    pub.publish("t14/r7/0", payload, 1)
    pub.close()

    first = sub.next_message(8.0)
    assert_true(first is not None and first.payload == payload,
                "R7: message was not delivered before the kill")
    # Let the 100 ms inflight snapshot happen, but leave the message unacked.
    # No DISCONNECT: the crash must happen with the in-flight entry still open.
    time.sleep(0.35)
    kill_broker(tier)
    sub.force_close()
    start_broker(tier)

    sub2, session_present = reconnect(cid, manual_ack=True)
    assert_true(session_present, "R7: session_present must be 1")
    again = sub2.next_message(8.0)
    assert_true(again is not None and again.payload == payload,
                "R7: delivered-but-unacked message was lost")
    dup = bool(again.dup)
    sub2.ack(again)
    sub2.close()
    return {"session_present": session_present, "dup": dup,
            "redelivered": True}


def scenario_r8(tier):
    """QoS 2 crash between PUBREC and PUBREL.

    The broker must not lose the message and must not wedge the client. The
    client's PUBLISH was already stored and delivered (R5's Method A), so what
    the client holds is a message it never finished acknowledging. After the
    restart the client re-sends PUBREL; the broker must answer PUBCOMP for that
    idempotently (R1) and the session's cursor must end up advanced.

    Contract measured here, per ADR-012 §12.1: **at least once, possibly
    duplicated** -- exactly-once is explicitly not promised across a crash.
    """
    cid = f"s1t14-r8-{tier}-sub"
    payload = b"r8-qos2-crash"
    topic = "t14/r8/0"
    subs_bytes = offline_subscriber(cid, "t14/r8/#", 2)

    raw = RawClient(f"s1t14-r8-{tier}-pub")
    raw.connect()
    raw.publish(topic, payload, qos=2, packet_id=7)
    kind, body = raw.recv(8.0)
    assert_true(kind == RawClient.PUBREC, f"R8: expected PUBREC, got 0x{kind:02x}")
    assert_true(packet_id(body) == 7, f"R8: PUBREC id {packet_id(body)} != 7")
    # The message was durable before PUBREC went out (ADR-018 §18.2/ADR-007),
    # so exactly one WAL record must exist at this instant. That is the promise
    # the crash below is allowed to rely on.
    wal = probe_recover(tier)
    stored = sum(h["high"] - 1 for h in wal["highs"].values())
    assert_true(stored == 1, f"R8: expected one stored record before PUBREC, got {stored}")

    # Crash with the client's handshake half done and no DISCONNECT.
    kill_broker(tier)
    raw.close()
    start_broker(tier)

    # The reconnect replays PUBREL for the id the broker no longer remembers
    # (the connection's QoS2 state died with it). R1 says: answer PUBCOMP,
    # never disconnect.
    raw2 = RawClient(f"s1t14-r8-{tier}-pub")
    raw2.connect()
    raw2.pubrel(7)
    kind, body = raw2.recv(8.0)
    assert_true(kind == RawClient.PUBCOMP,
                f"R8: expected PUBCOMP for the replayed PUBREL, got 0x{kind:02x}")
    assert_true(packet_id(body) == 7, f"R8: PUBCOMP id {packet_id(body)} != 7")
    raw2.disconnect()

    # The subscriber was offline-session'd, so the stored message must be there
    # at least once, and acking it must move the cursor.
    sub, session_present = reconnect(cid, manual_ack=True)
    assert_true(session_present, "R8: session_present must be 1 after kill")
    got = sub.collect(quiet=0.8, total=8.0)
    matched = [m for m in got if m.payload == payload]
    assert_true(matched or got, f"R8: no delivery after recovery (got {len(got)})")
    if not matched:
        raise AssertionError(f"R8: unexpected payloads after recovery: "
                             f"{[m.payload for m in got]}")
    dups = len(matched) - 1
    for m in matched:
        sub.ack(m)
    time.sleep(0.4)  # let the cursor tick land (100 ms window, both tiers)

    cursors = probe_cursors(cid, tier)
    advanced = [c for c in cursors["cursors"] if c["offset"] > 0]
    assert_true(advanced, f"R8: cursor never advanced: {cursors['cursors']}")
    sub.close()

    wal2 = probe_recover(tier)
    total = sum(h["high"] - 1 for h in wal2["highs"].values())
    assert_true(total >= 1, f"R8: the record disappeared after recovery: {wal2['highs']}")
    return {"payload": payload.decode(), "session_present": session_present,
            "subs_bytes": subs_bytes, "deliveries": len(matched),
            "duplicates": dups, "stored_records": total,
            "cursor": advanced[0]}


SCENARIO_FUNCS = {
    "r1a": scenario_r1a,
    "r1b": scenario_r1b,
    "r2": scenario_r2,
    "r3": scenario_r3,
    "r4": scenario_r4,
    "r5": scenario_r5,
    "r6": scenario_r6,
    "r7": scenario_r7,
    "r8": scenario_r8,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_one(name, tier):
    print(f"\n===== {name.upper()} tier={tier} =====", flush=True)
    started = time.monotonic()
    fresh_start(tier)
    try:
        evidence = SCENARIO_FUNCS[name](tier)
    except Exception:
        logs = broker_logs()
        print("\n---- broker logs (tail) ----\n" + logs, flush=True)
        raise
    elapsed = time.monotonic() - started
    print(f"PASS {name} tier={tier} ({elapsed:.1f}s) {json.dumps(evidence, sort_keys=True)}",
          flush=True)
    return {"scenario": name, "tier": tier, "seconds": round(elapsed, 2),
            "evidence": evidence}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tier", choices=TIERS + ["all"], default="all")
    parser.add_argument("--scenario", choices=SCENARIOS + ["all"], default="all")
    parser.add_argument("--keep-data", action="store_true",
                        help="do not delete the data dir between scenarios")
    parser.add_argument("--keep-going", action="store_true",
                        help="run the remaining scenarios after a failure")
    parser.add_argument("--skip-build", action="store_true")
    args = parser.parse_args(argv)

    tiers = TIERS if args.tier == "all" else [args.tier]
    scenarios = SCENARIOS if args.scenario == "all" else [args.scenario]

    if not args.skip_build:
        ensure_release()
    if not (RELEASE / "bin" / "unified_mq").exists():
        raise SystemExit(f"release missing: {RELEASE} (run without --skip-build)")

    original_clear = globals()["clear_data"]
    if args.keep_data:
        # Reusing a data dir is only sensible for one scenario; the caller owns
        # the cleanup then.
        globals()["clear_data"] = lambda: RUN.mkdir(parents=True, exist_ok=True)

    results, failures = [], []
    try:
        for tier in tiers:
            for name in scenarios:
                try:
                    results.append(run_one(name, tier))
                except Exception as exc:
                    failures.append({"scenario": name, "tier": tier, "error": str(exc)})
                    print(f"FAIL {name} tier={tier}: {exc}", flush=True)
                    if not args.keep_going:
                        raise
    finally:
        globals()["clear_data"] = original_clear
        run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)

    report = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "results": results, "failures": failures}
    out = RUN / f"report-{'-'.join(tiers)}-{'-'.join(scenarios)}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"\nREPORT {out}", flush=True)

    passed = len(results)
    print(f"SUMMARY passed={passed} failed={len(failures)}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
