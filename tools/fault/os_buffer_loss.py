#!/usr/bin/env python3
"""S2-B surrogate: the os_buffer power-loss window, on the CI host (design §4.2).

ADR-007's `os_buffer` tier ACKs as soon as the kernel accepts the write into its
page cache -- before any fsync -- so a process or node crash can lose exactly the
un-fsynced tail.  `kill -9` cannot show that: it leaves the page cache intact and
the kernel writes the dirty pages back, so the tail survives.  This driver models
the loss instead, deterministically and without root:

  1. append K records in `fsync` mode  -> each is group-committed with an fsync,
     so the segment's first B bytes are durable.  B is the barrier.
  2. append M records in `os_buffer` mode -> each returns an offset (the weak
     ACK) but is never fsync'd; they extend the file to B + tail but only into
     the page cache.  [B, EOF) is the known un-synced region.
  3. "power loss" = truncate the segment back to B.  The un-synced tail is gone,
     exactly as a real power failure would drop it.
  4. restart (recover) and assert: high == K+1 (the true durable end), the K
     fsync records read back gap-free, the M os_buffer offsets are absent, and
     nothing was advanced past K -- no phantom "committed".

The `fsync` prefix surviving while the `os_buffer` tail is dropped IS the fsync
control (design §4.2 "补齐未 fsync 的尾部对照"); T14 R1a already proves the
fsync side under `kill -9`.

The MQTT edge (S1) only ever appends `fsync` (QoS1) -- QoS0 is dispatched but
never stored (R5) -- so the `os_buffer` path is reachable only through the
storage API.  The injection therefore runs in a one-shot Erlang probe that owns
the single writer for the partition (ADR-011 §11.3), exactly as `kill -9` leaves
the disk for the recovery probe to read.

Usage:
    python3 tools/fault/os_buffer_loss.py --scenario tail
    python3 tools/fault/os_buffer_loss.py --scenario replicated
    python3 tools/fault/os_buffer_loss.py --scenario mqtt
    python3 tools/fault/os_buffer_loss.py            # all three

Requires the release (`rebar3 as t14 release`) and `/usr/bin/python3` (paho-mqtt
2.x for the mqtt scenario).  See docs/benchmarks/reliability/ for the reports.
"""

import argparse
import sys
import time
from pathlib import Path

# Reuse the T14 harness: container lifecycle, config rendering, one-shot Erlang
# probes, the paho client and the assertion helpers.  tools/docker is not on
# sys.path when this file is run as tools/fault/os_buffer_loss.py, so add it.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "docker"))
from mq_harness import *  # noqa: E402,F401,F403

RELIABILITY = Path(__file__).resolve().parents[2] / "docs" / "benchmarks" / "reliability"
PARTITION = 0  # the only partition the surrogate writes; 1..3 stay empty
SEGMENT = "segment-00000000000000000001.log"


# ---------------------------------------------------------------------------
# Erlang probe bodies
#
# Plain Python strings (single braces stay literal for Erlang terms); the only
# interpolation is __K__ / __M__, substituted with str.replace so an Erlang map
# #{...} or tuple {...} never has to be doubled.  `\\t' / `\\n' become Erlang's
# tab / newline escapes, matching mq_harness.probe_recover.
#
# #broker_message{} is built as a raw tuple -- field order is frozen by ADR-009
# (apps/broker_core/include/broker_message.hrl): the payload is element 6.  The
# on-disk body carries no partition_id (wal_codec stamps it from the caller), so
# the record's own partition_id slot is irrelevant here.
# ---------------------------------------------------------------------------

_INJECT = """
application:load(broker_storage),
application:load(broker_queue),
{ok, _} = application:ensure_all_started(broker_storage),
Backend = application:get_env(broker_storage, backend, broker_storage_wal),
Config = broker_queue:storage_config(),
DataDir = maps:get(data_dir, Config),
[case broker_queue:create_partition(P) of
     ok -> ok;
     {error, {already_exists, P}} -> ok;
     E -> io:format("CREATE_ERR\\t~p~n", [E])
 end || P <- broker_queue:partition_ids()],
{ok, Hd, _, _} = Backend:recover(__P__, Config),
MkMsg = fun(Payload, Rel) ->
    {broker_message, crypto:strong_rand_bytes(8), <<"tenant">>, <<"dest">>,
     __P__, Payload, raw, #{}, #{}, durable, Rel,
     erlang:system_time(millisecond), undefined, undefined}
end,
FsyncOffs = case __K__ of
    0 -> [];
    K0 ->
        {ok, Fs} = Backend:append(
                     Hd, [MkMsg(list_to_binary("fsync-" ++ integer_to_list(I)), fsync)
                          || I <- lists:seq(1, K0)], fsync),
        ok = Backend:advance_committed(Hd, K0),
        Fs
end,
SegPath = filename:join([DataDir, "wal", integer_to_list(__P__), "__SEG__"]),
B = filelib:file_size(SegPath),
{ok, TailOffs} = Backend:append(Hd, [MkMsg(list_to_binary("osbuf-" ++ integer_to_list(I)), os_buffer)
                                     || I <- lists:seq(1, __M__)], os_buffer),
%% Deliberately NO Backend:close/1: close syncs the segment, which would flush
%% the os_buffer tail to the device and destroy the very window this driver
%% measures.  halt/0 closes the fd without fsync, so the tail stays dirty in the
%% kernel page cache -- exactly what a power loss drops.  (The `tail' scenario
%% then truncates on the host; the dm-flakey scenario lets the device drop it.)
Bound = fun([]) -> 0; (L) -> hd(L) end,
BoundLast = fun([]) -> 0; (L) -> lists:last(L) end,
io:format("INJECT~nBARRIER\\t~p~nFSYNC_COUNT\\t~p~nFSYNC_FIRST\\t~p~nFSYNC_LAST\\t~p~n"
          "TAIL_COUNT\\t~p~nTAIL_FIRST\\t~p~nTAIL_LAST\\t~p~n",
          [B, length(FsyncOffs), Bound(FsyncOffs), BoundLast(FsyncOffs),
           length(TailOffs), Bound(TailOffs), BoundLast(TailOffs)]),
halt(0).
"""

_VERIFY = """
application:load(broker_storage),
application:load(broker_queue),
{ok, _} = application:ensure_all_started(broker_storage),
Backend = application:get_env(broker_storage, backend, broker_storage_wal),
Config = broker_queue:storage_config(),
lists:foreach(
  fun(P) -> case (catch Backend:recover(P, Config)) of
      {ok, _, C, H} -> io:format("WAL\\t~p\\t~p\\t~p~n", [P, C, H]);
      Other -> io:format("WALERR\\t~p\\t~p~n", [P, Other])
  end end, broker_queue:partition_ids()),
{ok, Hd0, _, _} = Backend:recover(__P__, Config),
{ok, Page, _Next} = Backend:read_from(Hd0, 1, 1000000),
lists:foreach(fun({Off, Msg}) ->
    io:format("READ\\t~p\\t~s~n", [Off, element(6, Msg)])
end, Page),
io:format("READCOUNT\\t~p~n", [length(Page)]),
halt(0).
"""

_REPLICATED = """
application:load(broker_storage),
application:load(broker_queue),
{ok, _} = application:ensure_all_started(broker_storage),
Backend = application:get_env(broker_storage, backend, broker_storage_wal),
Config = broker_queue:storage_config(),
ok = broker_queue:create_partition(__P__),
{ok, Hd, _, _} = Backend:recover(__P__, Config),
MkMsg = fun(Rel) ->
    {broker_message, crypto:strong_rand_bytes(8), <<"tenant">>, <<"dest">>,
     __P__, <<"x">>, raw, #{}, #{}, durable, Rel,
     erlang:system_time(millisecond), undefined, undefined}
end,
Repl = Backend:append(Hd, [MkMsg(replicated_commit)], replicated_commit),
Cap = Backend:capability(Hd),
{ok, [Off]} = Backend:append(Hd, [MkMsg(fsync)], fsync),
io:format("REPL\\t~p~nCAP\\t~p~nNEXT_OFF\\t~p~n", [Repl, Cap, Off]),
halt(0).
"""


def _erl(template, **subs):
    code = template
    for key, value in subs.items():
        code = code.replace(f"__{key}__", str(value))
    return code


_KV_KEYS = frozenset({
    "BARRIER", "FSYNC_COUNT", "FSYNC_FIRST", "FSYNC_LAST",
    "TAIL_COUNT", "TAIL_FIRST", "TAIL_LAST",
    "REPL", "CAP", "NEXT_OFF", "READCOUNT",
})


def _parse_kv(out):
    """Parse the INJECT/BARRIER/... tab-separated probe output into a dict."""
    kv = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) >= 2 and parts[0] in _KV_KEYS:
            kv[parts[0]] = parts[1:]
    return kv


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------

def scenario_tail(k, m, tier="default"):
    """S-B1a: fsync prefix + os_buffer tail, tail dropped at the barrier."""
    seg = DATA / "wal" / str(PARTITION) / SEGMENT
    inject = _erl(_INJECT, P=PARTITION, K=k, M=m, SEG=SEGMENT)
    out = erl_probe(inject, tier, timeout=180)
    assert_true("INJECT" in out, f"tail: injection probe did not run:\n{out}")
    kv = _parse_kv(out)
    assert_true("BARRIER" in kv and "TAIL_COUNT" in kv,
                f"tail: probe missing BARRIER/TAIL_COUNT:\n{out}")
    barrier = int(kv["BARRIER"][0])
    fsync_count = int(kv["FSYNC_COUNT"][0])
    tail_count = int(kv["TAIL_COUNT"][0])
    fsync_first, fsync_last = int(kv["FSYNC_FIRST"][0]), int(kv["FSYNC_LAST"][0])
    tail_first, tail_last = int(kv["TAIL_FIRST"][0]), int(kv["TAIL_LAST"][0])
    assert_true(fsync_count == k and (fsync_first, fsync_last) == (1, k),
                f"tail: fsync prefix is not 1..{k}: count={fsync_count} "
                f"first={fsync_first} last={fsync_last}")
    assert_true(tail_count == m and (tail_first, tail_last) == (k + 1, k + m),
                f"tail: os_buffer tail is not {k + 1}..{k + m}: count={tail_count} "
                f"first={tail_first} last={tail_last}")
    assert_true(seg.exists(), f"tail: segment missing after injection: {seg}")
    size_before = seg.stat().st_size
    assert_true(size_before > barrier,
                f"tail: segment {size_before} not past the barrier {barrier} (os_buffer tail missing)")

    # Power loss: drop the un-synced tail.  The probe has exited, so no writer
    # holds the fd (ADR-011 single-writer); this is the same host-side cut R4 uses.
    os.truncate(str(seg), barrier)
    assert_true(seg.stat().st_size == barrier,
                f"tail: truncate to {barrier} failed ({seg.stat().st_size})")

    verify = _erl(_VERIFY, P=PARTITION)
    vout = erl_probe(verify, tier, timeout=180)
    kv = _parse_kv(vout)
    wal = {int(r[1]): {"committed": int(r[2]), "high": int(r[3])}
           for r in _wal_lines(vout)}
    reads = _read_records(vout)

    # high[0] == K+1 (record_count = high-1 == K); the os_buffer tail is gone.
    assert_true(wal[PARTITION]["high"] == k + 1,
                f"tail: high[0]={wal[PARTITION]['high']} != {k + 1} (tail not dropped?)")
    # committed never passed the fsync prefix: no phantom "committed".
    assert_true(wal[PARTITION]["committed"] == k,
                f"tail: committed={wal[PARTITION]['committed']} != {k} (phantom committed?)")
    # The other partitions stay empty.
    for p in (1, 2, 3):
        assert_true(wal[p]["high"] == 1 and wal[p]["committed"] == 0,
                    f"tail: partition {p} not empty: {wal[p]}")
    # Exactly the K fsync records read back, gap-free, no os_buffer record.
    assert_true(len(reads) == k, f"tail: read {len(reads)} records, expected {k}")
    payloads = [p for _, p in reads]
    expected = [f"fsync-{i}".encode() for i in range(1, k + 1)]
    assert_true(payloads == expected,
                f"tail: recovered payloads are not the fsync prefix: {payloads[:3]}…")
    assert_true(not any(p.startswith(b"osbuf-") for p in payloads),
                "tail: an os_buffer record survived the power loss")
    return {"barrier": barrier, "size_before": size_before, "k": k, "m": m,
            "wal": {str(p): wal[p] for p in sorted(wal)}, "read": len(reads)}


def scenario_replicated(tier="default"):
    """S-B1c: replicated_commit is refused early and consumes no offset."""
    out = erl_probe(_erl(_REPLICATED, P=PARTITION), tier, timeout=120)
    kv = _parse_kv(out)
    assert_true("REPL" in kv and "CAP" in kv and "NEXT_OFF" in kv,
                f"replicated: probe output missing fields:\n{out}")
    repl = kv["REPL"][0]
    cap = kv["CAP"][0]
    next_off = int(kv["NEXT_OFF"][0])
    assert_true(repl == "{error,{unsupported_sync_mode,replicated_commit}}",
                f"replicated: wrong refusal: {repl}")
    assert_true("replicated_commit" not in cap,
                f"replicated: capability advertises replicated_commit: {cap}")
    assert_true(cap == "[os_buffer,fsync]",
                f"replicated: capability set changed: {cap}")
    # A rejected append consumes nothing: the next fsync caller gets offset 1.
    assert_true(next_off == 1,
                f"replicated: refused append consumed an offset (next={next_off})")
    return {"refusal": repl, "capability": cap, "next_offset": next_off}


def scenario_mqtt(k, m, tier="default"):
    """S-B1b: end-to-end at the MQTT edge -- PUBACK'd prefix survives, the
    os_buffer tail injected after a crash does not."""
    cid = f"s2b-osbuffer-{tier}-sub"
    filt = "t/s2b/osbuffer/#"
    # main() cleared the data dir and rendered the config; boot the broker.
    start_broker(tier)
    # Offline subscriber pins a cursor on the destination's partition.
    subs_bytes = offline_subscriber(cid, filt, 1)

    # Real QoS1 publishes -> fsync mode -> PUBACK only after the group-commit fsync.
    # One topic so every record hashes to a single partition (R2's pattern); the
    # offline subscriber's filter still matches it.
    topic = "t/s2b/osbuffer/0"
    pub = Client(f"s2b-osbuffer-{tier}-pub")
    pub.connect()
    payloads = [f"pub-{i:04d}".encode() for i in range(k)]
    pub.publish_many([topic] * k, payloads, qos=1)
    pub.close()

    # Crash (kill -9): the fsync'd prefix is already durable, the page cache holds
    # nothing unsynced yet.  Then the probe owns the writer and appends the
    # os_buffer tail to the one non-empty partition, and we cut it at the barrier.
    kill_broker(tier)
    seg = _non_empty_segment()
    assert_true(seg is not None, "mqtt: no non-empty segment after the QoS1 publishes")
    inject = _erl(_INJECT, P=seg.parent.name, K=0, M=m, SEG=seg.name)
    out = erl_probe(inject, tier, timeout=180)
    kv = _parse_kv(out)
    assert_true("BARRIER" in kv and "TAIL_COUNT" in kv,
                f"mqtt: injection probe missing BARRIER/TAIL_COUNT:\n{out}")
    barrier = int(kv["BARRIER"][0])
    assert_true(int(kv["FSYNC_COUNT"][0]) == 0,
                f"mqtt: injection unexpectedly appended a fsync prefix: {kv['FSYNC_COUNT']}")
    assert_true(int(kv["TAIL_COUNT"][0]) == m
                and int(kv["TAIL_FIRST"][0]) == k + 1
                and int(kv["TAIL_LAST"][0]) == k + m,
                f"mqtt: os_buffer tail is not {k + 1}..{k + m}: {kv}")
    size_before = seg.stat().st_size
    assert_true(size_before > barrier,
                f"mqtt: segment {size_before} not past the barrier {barrier}")
    os.truncate(str(seg), barrier)

    start_broker(tier)
    sub, session_present = reconnect(cid)
    assert_true(session_present, "mqtt: session_present must be 1 after the crash")
    msgs = sub.collect(quiet=0.8, total=8.0)
    idx = payload_indices(msgs, prefix="pub-")
    assert_true(idx == list(range(k)),
                f"mqtt: recovered deliveries are not the PUBACK'd prefix: {idx}")
    assert_true(len(msgs) == k, f"mqtt: phantom/duplicate deliveries: {len(msgs)}")
    sub.close()

    # The stored end must be the PUBACK'd prefix: high == k+1, tail absent.
    stop_broker(tier)
    vout = erl_probe(_erl(_VERIFY, P=seg.parent.name), tier, timeout=180)
    wal = {int(r[1]): {"committed": int(r[2]), "high": int(r[3])}
           for r in _wal_lines(vout)}
    part = int(seg.parent.name)
    assert_true(wal[part]["high"] == k + 1,
                f"mqtt: high[{part}]={wal[part]['high']} != {k + 1} (os_buffer tail survived?)")
    reads = _read_records(vout)
    assert_true(len(reads) == k and [p for _, p in reads] == payloads,
                f"mqtt: read back {len(reads)} records, expected the {k} published ones")
    return {"k": k, "m": m, "partition": seg.parent.name, "barrier": barrier,
            "size_before": size_before, "delivered": len(msgs),
            "wal": {str(p): wal[p] for p in sorted(wal)}, "subs_bytes": subs_bytes}


def _non_empty_segment():
    """The one segment with bytes (R4's locator), as a host Path."""
    segs = [p for p in glob.glob(str(DATA / "wal" / "*" / SEGMENT))
            if Path(p).stat().st_size > 0]
    return Path(segs[0]) if len(segs) == 1 else None


def _wal_lines(out):
    return [line.split("\t") for line in out.splitlines()
            if line.startswith("WAL\t") and len(line.split("\t")) == 4]


def _read_records(out):
    recs = []
    for line in out.splitlines():
        fields = line.split("\t")
        if line.startswith("READ\t") and len(fields) == 3:
            recs.append((int(fields[1]), fields[2].encode()))
    return recs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SCENARIOS = {
    "tail": scenario_tail,
    "replicated": scenario_replicated,
    "mqtt": scenario_mqtt,
}


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS) + ["all"], default="all")
    parser.add_argument("--tier", choices=TIERS, default="default")
    parser.add_argument("--fsync", type=int, default=16, help="K: fsync-mode records")
    parser.add_argument("--osbuffer", type=int, default=64, help="M: os_buffer-mode records")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--report", action="store_true", default=True,
                        help="write docs/benchmarks/reliability/report-*.json")
    args = parser.parse_args(argv)

    if not args.skip_build:
        ensure_release()
    if not (RELEASE / "bin" / "unified_mq").exists():
        raise SystemExit(f"release missing: {RELEASE} (run without --skip-build)")

    original_clear = globals()["clear_data"]
    if args.keep_data:
        globals()["clear_data"] = lambda: RUN.mkdir(parents=True, exist_ok=True)

    names = sorted(SCENARIOS) if args.scenario == "all" else [args.scenario]
    results, failures = [], []
    try:
        for name in names:
            print(f"\n===== S2-B {name} tier={args.tier} =====", flush=True)
            started = time.monotonic()
            # Prepare a clean data dir + rendered config, but do NOT start the
            # broker: the pure-probe scenarios (tail/replicated) drive the storage
            # API directly and own the single writer, while the mqtt scenario
            # starts the broker itself (it needs the listener for PUBACK).  The
            # stale container is removed first so `start_broker' boots a fresh
            # one against the freshly cleared data dir.
            run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)
            clear_data()
            render_config(args.tier)
            try:
                fn = SCENARIOS[name]
                if name in ("tail", "mqtt"):
                    evidence = fn(args.fsync, args.osbuffer, args.tier)
                else:
                    evidence = fn(args.tier)
            except Exception as exc:
                logs = broker_logs()
                print("\n---- broker logs (tail) ----\n" + logs, flush=True)
                failures.append({"scenario": name, "error": str(exc)})
                print(f"FAIL {name}: {exc}", flush=True)
                continue
            elapsed = time.monotonic() - started
            results.append({"scenario": name, "tier": args.tier,
                            "seconds": round(elapsed, 2), "evidence": evidence})
            print(f"PASS {name} ({elapsed:.1f}s) {json.dumps(evidence, sort_keys=True)}",
                  flush=True)
    finally:
        globals()["clear_data"] = original_clear
        try:
            stop_broker(args.tier)
        except Exception:
            pass

    if args.report:
        RELIABILITY.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        out = RELIABILITY / f"report-osbuffer-{'-'.join(names)}-{stamp}.json"
        out.write_text(json.dumps(
            {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
             "results": results, "failures": failures},
            indent=2, sort_keys=True) + "\n")
        print(f"\nREPORT {out}", flush=True)

    passed = len(results)
    print(f"SUMMARY passed={passed} failed={len(failures)}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
