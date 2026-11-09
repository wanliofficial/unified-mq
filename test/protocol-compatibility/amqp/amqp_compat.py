#!/usr/bin/env python3
"""S5 AMQP: real third-party client compatibility.

Gate S5 requires every protocol to pass a real third-party client integration
suite, "no in-house client talking to itself" (`docs/roadmap.md` §7, Gate S5;
`docs/protocol-compatibility.md` §3). This script drives the broker's AMQP
listener with **`amqp091-go` v1.10.0** (the Go AMQP 0-9-1 client named in the
S0.4 matrix for AMQP), a general-purpose library the broker does not control.
The wire protocol is AMQP 0-9-1, implemented independently by `amqp091-go`.

`main.go` opens real TCP connections and calls `amqp.Dial`, `Channel`,
`ExchangeDeclare`, `QueueDeclare`, `QueueBind`, `Consume`, `Publish`, `Ack`,
`Nack`, ...; this script only boots the broker and relays the Go program's
pass/fail report. The broker is a real `erl -noshell -s amqp_broker_boot run`
node on an ephemeral ranch port with a WAL data dir, mirroring
`amqp_conn_SUITE init_per_testcase`.

Usage:
    python3 test/protocol-compatibility/amqp/amqp_compat.py --all
    python3 test/protocol-compatibility/amqp/amqp_compat.py --case amqp-pubsub
    python3 test/protocol-compatibility/amqp/amqp_compat.py --all --skip-build
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = ROOT / "test" / "protocol-compatibility" / "amqp"
DATA = HERE / "data"
RUN = HERE / "run"
GO = os.environ.get("GO_BIN", "/home/wanli/Development/go1.26.10/bin/go")


def erl_bin(name="erl"):
    otp = os.environ.get("OTP_ROOT", "/home/wanli/Development/otp-28.3.1/bin")
    cand = Path(otp) / name
    return str(cand) if cand.exists() else name


def app_ebins():
    lib = ROOT / "_build" / "default" / "lib"
    return [str(p) for p in sorted(lib.glob("*/ebin"))]


def build_go():
    RUN.mkdir(parents=True, exist_ok=True)
    # Offline by default (this host keeps its module cache); CI overrides
    # GOPROXY so a fresh runner can download the pinned modules.
    env = dict(os.environ, GOFLAGS="-mod=mod")
    env.setdefault("GOPROXY", "off")
    subprocess.run([GO, "build", "-o", str(RUN / "amqp_compat"), "."],
                   cwd=str(HERE), env=env, check=True)


def boot_module_dir():
    out = HERE / "_build"
    out.mkdir(parents=True, exist_ok=True)
    src = HERE / "amqp_broker_boot.erl"
    subprocess.run([erl_bin("erlc"), "-o", str(out), str(src)],
                   check=True, capture_output=True)
    return str(out)


def start_broker():
    DATA.mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["BROKER_DATA_DIR"] = str(DATA)
    pa = []
    for d in app_ebins() + [boot_module_dir()]:
        pa += ["-pa", d]
    log_path = RUN / "broker.log"
    log_file = open(log_path, "wb")
    proc = subprocess.Popen(
        [erl_bin(), *pa, "-noshell", "-s", "amqp_broker_boot", "run"],
        cwd=str(ROOT), env=env, stdin=subprocess.PIPE,
        stdout=log_file, stderr=subprocess.STDOUT,
    )
    proc._log_file = log_file
    return proc


def wait_ready(proc, timeout=60):
    log_path = RUN / "broker.log"
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            time.sleep(0.2)
            raise SystemExit("broker exited early:\n" +
                             log_path.read_text("utf-8", "replace")[-4000:])
        text = log_path.read_text("utf-8", "replace") if log_path.exists() else ""
        for line in text.splitlines():
            if line.startswith("PORT "):
                return int(line.split()[1])
        if "START-ERROR" in text:
            raise SystemExit("broker failed to start:\n" + text[-4000:])
        time.sleep(0.2)
    raise SystemExit("broker did not become ready in time")


def stop_broker(proc):
    if proc.poll() is None:
        try:
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--case", default="")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--skip-build", action="store_true")
    args = ap.parse_args()
    if not args.case and not args.all:
        args.all = True

    if not args.skip_build:
        build_go()

    if DATA.exists():
        import shutil
        shutil.rmtree(DATA)

    proc = start_broker()
    try:
        port = wait_ready(proc)
        cmd = [str(RUN / "amqp_compat"), "-addr", f"127.0.0.1:{port}"]
        if args.case:
            cmd += ["-case", args.case]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        sys.stdout.write(out.stdout)
        if out.stderr:
            sys.stderr.write(out.stderr)
        report = {"port": port, "exit": out.returncode,
                  "stdout": out.stdout, "stderr": out.stderr}
        (RUN / "report.json").write_text(json.dumps(report, indent=2))
        if out.returncode != 0:
            raise SystemExit(out.returncode)
    finally:
        stop_broker(proc)


if __name__ == "__main__":
    main()
