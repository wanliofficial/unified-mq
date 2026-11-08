#!/usr/bin/env python3
"""S5 Kafka: real third-party client compatibility.

Gate S5 requires every protocol to pass a real third-party client integration
suite, "no in-house client talking to itself" (`docs/roadmap.md` §7, Gate S5;
`docs/protocol-compatibility.md` §3). This script drives the broker's Kafka
listener with the official **Apache `kafka-clients` 3.7.1** Java library --
the client named in the S0.4 matrix -- and never speaks the wire protocol by
hand.

`KafkaCompat.java` builds a real `KafkaProducer`/`KafkaConsumer`, calls
`send`, `poll`, `commitSync`, `subscribe`, `partitionsFor`, `listOffsets`, ...
and this script only boots the broker and relays the Java program's
pass/fail report. The broker is a real `erl -noshell -s kafka_broker_boot run`
node on an ephemeral ranch port with a WAL data dir.

The jars come from the local Maven cache (`~/.m2`) because this host is
offline; `javac` is used directly so no Maven resolution is needed.

Usage:
    python3 test/protocol-compatibility/kafka/kafka_compat.py --all
    python3 test/protocol-compatibility/kafka/kafka_compat.py --case kafka-produce-consume
    python3 test/protocol-compatibility/kafka/kafka_compat.py --all --skip-build
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HERE = ROOT / "test" / "protocol-compatibility" / "kafka"
DATA = HERE / "data"
RUN = HERE / "run"
CLASSES = HERE / "_build" / "classes"

M2 = Path(os.environ.get("M2_REPO", str(Path.home() / ".m2" / "repository")))
JARS = [
    ("org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar", True),
    ("com/github/luben/zstd-jni/1.5.6-3/zstd-jni-1.5.6-3.jar", True),
    ("org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar", True),
    ("org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar", True),
    ("org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar", True),
    ("org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar", False),
]


def erl_bin(name="erl"):
    otp = os.environ.get("OTP_ROOT", "/home/wanli/Development/otp-28.3.1/bin")
    cand = Path(otp) / name
    return str(cand) if cand.exists() else name


def classpath():
    parts = []
    for rel, required in JARS:
        jar = M2 / rel
        if jar.exists():
            parts.append(str(jar))
        elif required:
            raise SystemExit(f"missing jar in the Maven cache: {jar}")
    return os.pathsep.join(parts)


def app_ebins():
    lib = ROOT / "_build" / "default" / "lib"
    return [str(p) for p in sorted(lib.glob("*/ebin"))]


def build_java():
    CLASSES.mkdir(parents=True, exist_ok=True)
    cp = classpath()
    subprocess.run(["javac", "-cp", cp, "-d", str(CLASSES),
                    str(HERE / "KafkaCompat.java")], check=True)


def boot_module_dir():
    out = HERE / "_build"
    out.mkdir(parents=True, exist_ok=True)
    subprocess.run([erl_bin("erlc"), "-o", str(out), str(HERE / "kafka_broker_boot.erl")],
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
        [erl_bin(), *pa, "-noshell", "-s", "kafka_broker_boot", "run"],
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
        build_java()

    if DATA.exists():
        shutil.rmtree(DATA)

    proc = start_broker()
    try:
        port = wait_ready(proc)
        cp = os.pathsep.join([str(CLASSES), classpath()])
        cmd = ["java", "-cp", cp, "KafkaCompat", "--bootstrap",
               f"127.0.0.1:{port}"]
        if args.case:
            cmd += ["--case", args.case]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
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
