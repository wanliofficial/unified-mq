#!/usr/bin/env python3
"""S2-B real-machine power-loss driver: dm-flakey on a loopback ext4 (design §4.2).

The SOC-level companion to `tools/fault/os_buffer_loss.py`.  That driver proves
the `os_buffer` loss boundary deterministically by truncating the segment at the
fsync barrier.  This one does the same experiment on a **real filesystem that
really loses writes**, so the report is evidence about the kernel, not just about
the model:

  1. a loop device holds an ext4 image, with `dm-flakey` mapped on top and
     mounted as the broker's data directory;
  2. a one-shot probe appends K `fsync` records (each group-committed; the
     fsync'd prefix is durable) and then M `os_buffer` records -- offsets are
     returned (the weak ACK) but nothing is synced, so the tail lives only in
     the page cache;
  3. the flakey layer is switched to `drop_writes`, then the host `sync`es: the
     kernel believes the dirty tail was written, the device silently discards
     it -- a power loss in which the device lied about flush;
  4. the filesystem is unmounted while dirty, the flakey layer is replaced by a
     plain `linear` mapping, and the filesystem is mounted again (ext4 journal
     replay if the superblock write was in the dropped window);
  5. the recovery probe asserts the same invariants as the surrogate: `high`
     is back at the durable end (K+1), the K fsync records read back gap-free,
     the M os_buffer offsets are gone and `committed` never crossed K.  The
     surviving fsync prefix after a *real* lost write is the `fsync`-tier
     control.

Why the probe does not `close/1` the handle: `wal_writer:close/1` fsyncs, which
would flush the tail and destroy the window under test.  The probe halts after
printing, which closes the fd without fsync; the dirty pages stay in the page
cache for the device to drop.

Privileges: `losetup`, `dmsetup`, `mkfs.ext4`, `mount`, `sync` all need root.
Runs on a CI host are therefore out of scope; the CI-resident proof is the
surrogate (`tools/fault/os_buffer_loss.py`).  This script exists so an operator
can produce the real-machine report required by design §4.2:

    sudo python3 tools/docker/s2_powerloss.py --scenario powerloss

Without root it can still print what it would do:

    python3 tools/docker/s2_powerloss.py --skip-prereq

The report lands in docs/benchmarks/reliability/.  Inputs: the `t14` release
(`rebar3 as t14 release`), Docker with the erlang:28.3.1.0 image, and ext4 space
for the image (default 256 MiB, NOT under /tmp -- that is tmpfs on this host).
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

TOOLS = Path(__file__).resolve().parents[1]

# Reuse the T14 harness (container lifecycle, config rendering, one-shot Erlang
# probes) and the surrogate's probe bodies, so both proofs assert the same
# invariants from the same source.
sys.path.insert(0, str(TOOLS / "docker"))
sys.path.insert(0, str(TOOLS / "fault"))

import mq_harness as harness  # noqa: E402
from os_buffer_loss import (  # noqa: E402
    PARTITION,
    SEGMENT,
    _INJECT,
    _VERIFY,
    _erl,
    _parse_kv,
    _read_records,
    _wal_lines,
)

RELIABILITY = TOOLS / "docs" / "benchmarks" / "reliability"

# State paths.  The image lives inside the repo tree on purpose: this host's
# /tmp is tmpfs (design J11), and a power-loss test on a RAM disk would test
# nothing.  Everything is overridable so an operator can move it to a dedicated
# disk.
IMAGE = Path(os.environ.get("S2B_DM_IMAGE", str(TOOLS / "docker" / "powerloss.img")))
MOUNT = Path(os.environ.get("S2B_DM_MOUNT", "/var/lib/unified-mq-powerloss"))
DM_NAME = os.environ.get("S2B_DM_NAME", "s2b-powerloss")
IMAGE_MB = int(os.environ.get("S2B_DM_IMAGE_MB", "256"))
DM_DEV = Path("/dev/mapper") / DM_NAME

# Flakey table windows: "up" long enough that the device is healthy for the whole
# setup+injection phase, then a reload with up=0 switches it to unreliable
# immediately.  drop_writes (not error_writes) is what a lying disk does.
UP_SECONDS = 3600


# ---------------------------------------------------------------------------
# Shell
# ---------------------------------------------------------------------------

def run(cmd, check=True, timeout=600):
    argv = [str(c) for c in cmd]
    print("+ " + " ".join(argv), flush=True)
    proc = subprocess.run(argv, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout)
    if proc.stdout:
        print(proc.stdout.rstrip(), flush=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(argv)}")
    return proc


def require_root():
    if os.geteuid() != 0:
        raise SystemExit(
            "this driver needs root (losetup/dmsetup/mkfs.ext4/mount/sync);\n"
            "re-run with sudo, or use --skip-prereq to print the plan.\n"
            "The CI-resident proof is tools/fault/os_buffer_loss.py.")


def dm_table(loop, sectors, *, drop, up, down):
    """dm-flakey table for the two states we need.

    drop=True  -> immediately unreliable, writes silently discarded
    drop=False -> healthy for `up` seconds
    """
    if drop:
        return f"0 {sectors} flakey {loop} 0 0 {down} drop_writes"
    return f"0 {sectors} flakey {loop} 0 {up} {down}"


def flakey_load(sectors, loop, *, drop):
    # Suspend -> reload -> resume: the table swap must not race in-flight
    # writeback, otherwise a dirty page could sneak through the old (healthy)
    # table between the reload and its activation.
    run(["dmsetup", "suspend", DM_NAME])
    run(["dmsetup", "reload", DM_NAME, "--table",
         dm_table(loop, sectors, drop=drop, up=UP_SECONDS, down=3600)])
    run(["dmsetup", "resume", DM_NAME])


# ---------------------------------------------------------------------------
# Device lifecycle
# ---------------------------------------------------------------------------

class Device:
    """Loop image + dm-flakey + mount, torn down in reverse on any failure."""

    def __init__(self):
        self.loop = None
        self.sectors = 0
        self.mounted = False

    def create(self):
        IMAGE.parent.mkdir(parents=True, exist_ok=True)
        if IMAGE.exists():
            IMAGE.unlink()
        run(["truncate", "-s", f"{IMAGE_MB}M", IMAGE])
        self.loop = run(["losetup", "--find", "--show", IMAGE]).stdout.strip()
        self.sectors = int(run(["blockdev", "--getsz", self.loop]).stdout.strip())
        run(["dmsetup", "create", DM_NAME, "--table",
             dm_table(self.loop, self.sectors, drop=False, up=UP_SECONDS, down=3600)])
        run(["mkfs.ext4", "-F", "-q", DM_DEV])
        self.mount()

    def mount(self):
        MOUNT.mkdir(parents=True, exist_ok=True)
        run(["mount", DM_DEV, MOUNT])
        self.mounted = True

    def unmount(self):
        if self.mounted:
            run(["umount", MOUNT], check=False)
            self.mounted = False

    def drop_writes(self):
        """The power-loss instant: device starts lying about flush."""
        flakey_load(self.sectors, self.loop, drop=True)
        # Push every dirty page at the device; the ones that only existed in the
        # page cache are now reported written and silently discarded.
        run(["sync"])

    def restore(self):
        """Replace the flakey layer with a plain linear mapping."""
        run(["dmsetup", "suspend", DM_NAME])
        run(["dmsetup", "reload", DM_NAME, "--table",
             f"0 {self.sectors} linear {self.loop} 0"])
        run(["dmsetup", "resume", DM_NAME])

    def destroy(self, keep_image):
        self.unmount()
        run(["dmsetup", "remove", DM_NAME], check=False)
        if self.loop:
            run(["losetup", "-d", self.loop], check=False)
        if not keep_image and IMAGE.exists():
            IMAGE.unlink()
        if MOUNT.exists():
            try:
                MOUNT.rmdir()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Scenario
# ---------------------------------------------------------------------------

def scenario_powerloss(k, m, tier, dev):
    # The harness binds `DATA` into both the broker container and the probe;
    # point it at the flakey mount before any docker call.
    harness.DATA = MOUNT
    if MOUNT.exists():
        # The mount is fresh (mkfs), but be explicit: recovery refuses a dirty
        # layout, so the run must start from an empty directory.
        for child in MOUNT.iterdir():
            shutil.rmtree(child) if child.is_dir() else child.unlink()
    harness.render_config(tier)

    # 1+2. K fsync records (durable) then M os_buffer records (page cache only).
    out = harness.erl_probe(_erl(_INJECT, P=PARTITION, K=k, M=m, SEG=SEGMENT),
                            tier, timeout=300)
    kv = _parse_kv(out)
    if "BARRIER" not in kv:
        raise AssertionError(f"injection probe did not report a barrier:\n{out}")
    barrier = int(kv["BARRIER"][0])
    tail_count = int(kv["TAIL_COUNT"][0])
    fsync_count = int(kv["FSYNC_COUNT"][0])
    seg = MOUNT / "wal" / str(PARTITION) / SEGMENT
    size_before = seg.stat().st_size
    harness.assert_true(fsync_count == k, f"fsync prefix count {fsync_count} != {k}")
    harness.assert_true(tail_count == m, f"os_buffer tail count {tail_count} != {m}")
    harness.assert_true(size_before > barrier,
                        f"segment {size_before} not past the barrier {barrier}")

    # 3. Power loss: drop writes, flush the dirty tail into the void.
    dev.drop_writes()

    # 4. Detach while dirty, restore a truthful device, mount again.
    dev.unmount()
    dev.restore()
    dev.mount()

    # 5. Recovery asserts the same invariants as the surrogate.
    vout = harness.erl_probe(_erl(_VERIFY, P=PARTITION), tier, timeout=300)
    wal = {int(r[1]): {"committed": int(r[2]), "high": int(r[3])}
           for r in _wal_lines(vout)}
    harness.assert_true(PARTITION in wal,
                        f"recovery probe did not report partition {PARTITION}:\n{vout}")
    harness.assert_true(wal[PARTITION]["high"] == k + 1,
                        f"high[{PARTITION}]={wal[PARTITION]['high']} != {k + 1}: "
                        "the os_buffer tail survived a real power loss")
    harness.assert_true(wal[PARTITION]["committed"] == k,
                        f"committed={wal[PARTITION]['committed']} != {k}: "
                        "recovery advanced past the fsync prefix")
    reads = _read_records(vout)
    expected = [f"fsync-{i}".encode() for i in range(1, k + 1)]
    harness.assert_true([p for _, p in reads] == expected,
                        f"fsync prefix did not survive: {[p for _, p in reads][:3]}...")
    harness.assert_true(not any(p.startswith(b"osbuf-") for _, p in reads),
                        "an os_buffer record survived the power loss")

    # 6. The real node must also boot on the recovered filesystem (this is the
    #    difference from the surrogate: the mount, the journal and the release
    #    all take part).
    booted = False
    run(["docker", "rm", "-f", harness.CONTAINER], check=False)
    try:
        harness.start_broker(tier)
        booted = True
    finally:
        harness.stop_broker(tier)

    return {"k": k, "m": m, "barrier": barrier, "size_before": size_before,
            "device": {"image": str(IMAGE), "image_mb": IMAGE_MB,
                       "dm": str(DM_DEV), "mount": str(MOUNT),
                       "loop": dev.loop, "sectors": dev.sectors},
            "wal": {str(p): wal[p] for p in sorted(wal)},
            "read": len(reads), "broker_booted": booted}


SCENARIOS = {"powerloss": scenario_powerloss}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def plan(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="powerloss")
    parser.add_argument("--tier", choices=harness.TIERS, default="default")
    parser.add_argument("--fsync", type=int, default=16, help="K: fsync-mode records")
    parser.add_argument("--osbuffer", type=int, default=64, help="M: os_buffer-mode records")
    parser.add_argument("--skip-prereq", action="store_true",
                        help="print the privileged steps and exit 0")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep-image", action="store_true",
                        help="leave the loop image behind for inspection")
    return parser.parse_args(argv)


def main(argv=None):
    args = plan(argv)
    steps = [
        f"truncate -s {IMAGE_MB}M {IMAGE}",
        f"losetup --find --show {IMAGE}",
        f"dmsetup create {DM_NAME} --table '0 <sectors> flakey <loop> 0 {UP_SECONDS} 3600'",
        f"mkfs.ext4 -F -q {DM_DEV}",
        f"mount {DM_DEV} {MOUNT}",
        "# inject: K fsync records + M os_buffer records (no close -> tail unsynced)",
        f"dmsetup reload {DM_NAME} --table '0 <sectors> flakey <loop> 0 0 3600 drop_writes'",
        f"dmsetup suspend {DM_NAME} && dmsetup resume {DM_NAME}",
        "sync   # dirty tail reported written, then discarded",
        f"umount {MOUNT}",
        f"dmsetup reload {DM_NAME} --table '0 <sectors> linear <loop> 0'",
        f"mount {DM_DEV} {MOUNT}   # ext4 journal replay",
        "# verify: high == K+1, committed == K, K fsync records readable, no osbuf",
    ]
    if args.skip_prereq:
        print("S2-B dm-flakey power-loss plan (requires root):", flush=True)
        for step in steps:
            print("  " + step, flush=True)
        print("\nRun it with: sudo python3 tools/docker/s2_powerloss.py "
              f"--scenario {args.scenario}", flush=True)
        return 0

    require_root()
    if not args.skip_build:
        harness.ensure_release()
    if not (harness.RELEASE / "bin" / "unified_mq").exists():
        raise SystemExit(f"release missing: {harness.RELEASE} (run without --skip-build)")

    dev = Device()
    started = time.monotonic()
    try:
        dev.create()
        evidence = scenario_powerloss(args.fsync, args.osbuffer, args.tier, dev)
    finally:
        dev.destroy(args.keep_image)

    elapsed = time.monotonic() - started
    RELIABILITY.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = RELIABILITY / f"s2-powerloss-{stamp}.json"
    out.write_text(json.dumps(
        {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
         "mode": "dm-flakey drop_writes",
         "kernel": os.uname().release,
         "scenario": args.scenario, "tier": args.tier,
         "seconds": round(elapsed, 2), "evidence": evidence},
        indent=2, sort_keys=True) + "\n")
    print(f"\nPASS {args.scenario} ({elapsed:.1f}s)", flush=True)
    print(f"REPORT {out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
