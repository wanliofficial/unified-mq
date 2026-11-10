#!/usr/bin/env python3
"""S8-C MQTT-SN: real third-party client compatibility.

Gate S8 requires every new protocol to pass a real third-party client
integration suite, "no in-house client talking to itself" (`docs/roadmap.md`
§7), the same rule S5 and S8-B satisfy. This script drives the MQTT-SN gateway
with **`mqtt-sn-tools` 0.0.7** (njh/mqtt-sn-tools), a standalone C implementation
of MQTT-SN 1.2 that this repository does not control. Our own `mqtt_sn_codec` is
never used as a client.

Cases (each maps to one observable protocol contract):

  sn-connect          CONNECT -> CONNACK rc=0, then DISCONNECT -> DISCONNECT
  sn-qos0             QoS0 publish reaches a subscriber, no ack involved
  sn-qos1             QoS1 uplink is PUBACKed and delivered (subscriber acks too)
  sn-register         client REGISTER -> REGACK rc=0, then PUBLISH by that id
  sn-register-downlink the gateway REGISTERs a concrete topic with a wildcard
                      subscriber before publishing to it (downlink REGACK)
  sn-short-topic      a two-character topic travels as itself (short topic id)
  sn-predefined-topic `-T 1' uses the gateway's configured predefined table
  sn-retain           a retained publish is replayed to a subscriber that
                      arrives later
  sn-sleep-disconnect `-e <n>' (DISCONNECT with a duration) is acknowledged with
                      the granted duration -- the sleep half of sleep/wake
  sn-qos-minus1       QoS -1 publishes with **no CONNECT at all** (the tool
                      skips the session, exactly as the level defines)

What this client cannot cover, and why (quoted in docs/protocol-compatibility.md
§4.11 rather than hidden):

  * **QoS 2** -- `mqtt-sn-tools` has no QoS 2 code path (`-q` accepts 0/1/-1
    only). The full PUBREC/PUBREL/PUBCOMP handshake is covered by
    `mqtt_sn_conn_SUITE` in both directions.
  * **Will** -- the tool has no Will option and never sends WILLTOPIC/WILLMSG.
    Will-vs-takeover is covered by `mqtt_sn_conn_SUITE`.
  * **Sleep wake** -- the tool sends `PINGREQ` as a bare two-byte packet, with
    no client id, so it can never wake a sleeping session by name. The wake
    path (PINGREQ(client id) -> backlog) is covered by `mqtt_sn_conn_SUITE`.

Usage:
    python3 test/protocol-compatibility/mqtt_sn/mqtt_sn_compat.py --all
    python3 test/protocol-compatibility/mqtt_sn/mqtt_sn_compat.py --case sn-qos1

The script starts one broker node on an ephemeral UDP port
(`mqtt_sn_broker_boot.erl`, the same boot path as `mqtt_sn_conn_SUITE`), runs
the cases, and writes `run/report.json`. `run/` and `data/` are gitignored.

The C tools are looked up, in order, at `$MQTT_SN_TOOLS_DIR`, in
`_build/mqtt-sn-tools/bin`, and on `PATH`; `--build-tools SRC` compiles them from
a source tree into `_build/mqtt-sn-tools/bin`. See README.md for the exact
upstream commit.
"""

import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATA = HERE / "data"
RUN = HERE / "run"
BUILD = HERE / "_build"
TOOLS_BUILD = ROOT / "_build" / "mqtt-sn-tools" / "bin"

TOOLS = ("mqtt-sn-pub", "mqtt-sn-sub")
PINNED_COMMIT = "a1dd870"


class CheckError(AssertionError):
    pass


def check(cond, msg):
    if not cond:
        raise CheckError(msg)


def erl_bin(name="erl"):
    otp = os.environ.get("OTP_ROOT", "/home/wanli/Development/otp-28.3.1/bin")
    cand = Path(otp) / name
    return str(cand) if cand.exists() else name


def app_ebins():
    lib = ROOT / "_build" / "default" / "lib"
    return [str(p) for p in sorted(lib.glob("*/ebin"))]


# ---------------------------------------------------------------------------
# Third-party tools
# ---------------------------------------------------------------------------

def tool_dirs():
    env = os.environ.get("MQTT_SN_TOOLS_DIR")
    return [d for d in (Path(env) if env else None, TOOLS_BUILD) if d]


def find_tools():
    for d in tool_dirs():
        if all((d / t).is_file() for t in TOOLS):
            return d
    found = {}
    for t in TOOLS:
        p = shutil.which(t)
        if p:
            found[t] = p
    if len(found) == len(TOOLS):
        return Path(next(iter(found.values()))).parent
    raise SystemExit(
        "mqtt-sn-tools not found.\n"
        "Build them once with:\n"
        "  git clone https://github.com/njh/mqtt-sn-tools\n"
        "  cd mqtt-sn-tools && git checkout %s && make\n"
        "then either put the directory on PATH, set MQTT_SN_TOOLS_DIR, or run:\n"
        "  %s --build-tools /path/to/mqtt-sn-tools --all"
        % (PINNED_COMMIT, sys.argv[0]))


def build_tools(src):
    src = Path(src)
    check((src / "Makefile").is_file(), f"{src} does not look like mqtt-sn-tools")
    TOOLS_BUILD.mkdir(parents=True, exist_ok=True)
    subprocess.run(["make", "-C", str(src)], check=True, capture_output=True)
    for t in TOOLS:
        shutil.copy2(src / t, TOOLS_BUILD / t)
    return TOOLS_BUILD


# ---------------------------------------------------------------------------
# Broker node
# ---------------------------------------------------------------------------

def boot_module_dir():
    BUILD.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [erl_bin("erlc"), "-o", str(BUILD), str(HERE / "mqtt_sn_broker_boot.erl")],
        check=True, capture_output=True,
    )
    return str(BUILD)


def start_broker():
    DATA.mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["BROKER_DATA_DIR"] = str(DATA)
    pa = []
    for d in app_ebins() + [boot_module_dir()]:
        pa += ["-pa", d]
    log_file = open(RUN / "broker.log", "wb")
    proc = subprocess.Popen(
        [erl_bin(), *pa, "-noshell", "-s", "mqtt_sn_broker_boot", "run"],
        cwd=str(ROOT), env=env, stdin=subprocess.PIPE,
        stdout=log_file, stderr=subprocess.STDOUT,
    )
    proc._log_file = log_file
    return proc


def wait_ready(proc, timeout=60):
    log_path = RUN / "broker.log"
    deadline = time.time() + timeout
    pos = 0
    while time.time() < deadline:
        if proc.poll() is not None:
            time.sleep(0.2)
            raise SystemExit("broker exited early:\n"
                             + log_path.read_text("utf-8", "replace")[-4000:])
        with open(log_path, "rb") as f:
            f.seek(pos)
            chunk = f.read()
            pos = f.tell()
        if chunk:
            text = chunk.decode("utf-8", "replace")
            for line in text.splitlines():
                if line.startswith("PORT "):
                    return int(line.split()[1])
            if "START-ERROR" in text:
                raise SystemExit("broker failed to start:\n"
                                 + log_path.read_text("utf-8", "replace")[-4000:])
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


def broker_log_tail(n=8000):
    log_path = RUN / "broker.log"
    return log_path.read_text("utf-8", "replace")[-n:] if log_path.exists() else ""


# ---------------------------------------------------------------------------
# mqtt-sn-tools wrappers
# ---------------------------------------------------------------------------

class Tool:
    """A running mqtt-sn-tools process with line-oriented capture."""

    def __init__(self, binary, args, name):
        self.name = name
        self.out = []
        self.err = []
        self._lines = queue.Queue()
        self.proc = subprocess.Popen(
            [str(binary), *args],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        )
        self._threads = [
            threading.Thread(target=self._pump, args=(self.proc.stdout, self.out),
                             daemon=True),
            threading.Thread(target=self._pump, args=(self.proc.stderr, self.err),
                             daemon=True),
        ]
        for t in self._threads:
            t.start()

    def _pump(self, stream, sink):
        for line in stream:
            sink.append(line.rstrip("\n"))
            self._lines.put(line.rstrip("\n"))

    def wait_for(self, pattern, timeout=15, stream="both"):
        """Block until a line matching `pattern' appears (or raise)."""
        rx = re.compile(pattern)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                line = self._lines.get(timeout=max(0.05, deadline - time.time()))
            except queue.Empty:
                break
            if rx.search(line):
                return line
            # Lines consumed here are still in self.out/self.err for reports.
        raise CheckError(
            f"{self.name}: no line matching {pattern!r} within {timeout}s\n"
            f"  stdout: {self.stdout_text()}\n  stderr: {self.stderr_text()}")

    def wait(self, timeout=20):
        try:
            rc = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            raise CheckError(f"{self.name}: still running after {timeout}s\n"
                             f"  stdout: {self.stdout_text()}\n"
                             f"  stderr: {self.stderr_text()}")
        for t in self._threads:
            t.join(timeout=2)
        return rc

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass

    def stdout_text(self):
        return "\n".join(self.out)

    def stderr_text(self):
        return "\n".join(self.err)


def run_pub(tools, port, args, timeout=20):
    tool = Tool(tools / "mqtt-sn-pub", ["-h", "127.0.0.1", "-p", str(port), *args],
                "mqtt-sn-pub")
    rc = tool.wait(timeout=timeout)
    return tool, rc


def start_sub(tools, port, args):
    return Tool(tools / "mqtt-sn-sub", ["-h", "127.0.0.1", "-p", str(port), *args],
                "mqtt-sn-sub")


def wait_subscribed(sub, timeout=15):
    """`-d' makes the tool print the SUBACK it just received, so a case never
    has to sleep and hope the SUBSCRIBE landed first."""
    sub.wait_for(r"SUBACK return code: 0x00", timeout=timeout)


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def case_sn_connect(tools, port):
    """CONNECT -> CONNACK rc=0, PUBLISH, DISCONNECT -> DISCONNECT.

    A QoS1 publish is the shortest exchange that proves all three: the tool
    exits non-zero on a refused CONNACK, warns if the PUBACK never arrives, and
    fails if the DISCONNECT is not acknowledged.
    """
    tool, rc = run_pub(tools, port, ["-q", "1", "-t", "sn/compat/connect",
                                     "-m", "hello", "-d"])
    check(rc == 0, f"mqtt-sn-pub exited {rc}\n{tool.stderr_text()}")
    err = tool.stderr_text()
    check("CONNACK return code: 0x00" in err, "no CONNACK rc=0 in the tool log")
    check("Received PUBACK" in err, "no PUBACK in the tool log")
    check("Failed to disconnect" not in err, "DISCONNECT was not acknowledged")
    return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
            "evidence": "CONNACK rc=0, PUBACK, DISCONNECT acknowledged"}


def pubsub(tools, port, topic, pub_args, sub_args, expect_out, pub_evidence=()):
    """One subscriber, one publisher, one payload.

    `pub_evidence' is a list of substrings the *publisher's* own debug log must
    contain -- that is where an uplink ack is observable, because the tool only
    prints "Received PUBACK" after parsing one off the socket.
    """
    sub = start_sub(tools, port, ["-1", "-v", "-d", *sub_args, "-t", topic])
    try:
        wait_subscribed(sub)
        tool, rc = run_pub(tools, port, [*pub_args, "-d", "-t", topic,
                                        "-m", "payload"])
        check(rc == 0, f"publish failed rc={rc}\n{tool.stderr_text()}")
        err = tool.stderr_text()
        for needle in pub_evidence:
            check(needle in err, f"publisher log lacks {needle!r}\n{err}")
        sub_rc = sub.wait(timeout=20)
        check(sub_rc == 0, f"subscriber exited {sub_rc}\n{sub.stderr_text()}")
        out = sub.stdout_text()
        check(expect_out in out,
              f"subscriber did not print {expect_out!r}; got {out!r}")
        evidence = f"subscriber printed {expect_out!r}"
        if pub_evidence:
            evidence += "; publisher saw " + ", ".join(pub_evidence)
        return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
                "evidence": evidence}
    finally:
        sub.kill()


def case_sn_qos0(tools, port):
    return pubsub(tools, port, "sn/compat/q0", ["-q", "0"],
                  ["-q", "0"], "sn/compat/q0: payload")


def case_sn_qos1(tools, port):
    """Both directions are distinct on the wire: the publisher's log shows the
    uplink PUBACK, and the subscriber prints the payload only after it has
    received the downlink PUBLISH and answered with its own PUBACK."""
    return pubsub(tools, port, "sn/compat/q1", ["-q", "1"],
                  ["-q", "1"], "sn/compat/q1: payload",
                  pub_evidence=("Received PUBACK",))


def case_sn_register(tools, port):
    """A client that has never subscribed REGISTERs the name, gets an id back,
    and publishes to that id -- the dynamic-registration path."""
    sub = start_sub(tools, port, ["-1", "-v", "-d", "-q", "1",
                                  "-t", "sn/compat/register"])
    try:
        wait_subscribed(sub)
        tool, rc = run_pub(tools, port, ["-q", "1", "-d",
                                         "-t", "sn/compat/register",
                                         "-m", "registered"])
        check(rc == 0, f"publish failed rc={rc}\n{tool.stderr_text()}")
        err = tool.stderr_text()
        check("REGACK return code: 0x00" in err,
              "publisher never received REGACK rc=0")
        check("REGACK topic id: 0x" in err, "no topic id in the REGACK")
        check(sub.wait(timeout=20) == 0, "subscriber failed")
        check("payload" in sub.stdout_text() or "registered" in sub.stdout_text(),
              f"subscriber did not print the payload: {sub.stdout_text()!r}")
        return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
                "evidence": "REGACK rc=0 with an id, then PUBLISH by that id"}
    finally:
        sub.kill()


def case_sn_register_downlink(tools, port):
    """The mirror image of `sn-register`: the *gateway* registers the concrete
    topic with the subscriber before publishing to a wildcard subscription.

    A subscriber to `sn/down/#' has no topic id for `sn/down/x' -- only the
    gateway can hand one out, and it must do so *before* the PUBLISH that names
    that id. The tool prints "Received ... Type=REGISTER" and answers REGACK,
    then prints the payload under the resolved name.
    """
    sub = start_sub(tools, port, ["-1", "-v", "-d", "-q", "1",
                                  "-t", "sn/down/#"])
    try:
        wait_subscribed(sub)
        tool, rc = run_pub(tools, port, ["-q", "1", "-t", "sn/down/x",
                                         "-m", "payload"])
        check(rc == 0, f"publish failed rc={rc}\n{tool.stderr_text()}")
        check(sub.wait(timeout=20) == 0, "subscriber failed")
        err = sub.stderr_text()
        check("Type=REGISTER" in err,
              "the gateway never sent a downlink REGISTER for the concrete "
              "topic:\n" + err)
        check("sn/down/x: payload" in sub.stdout_text(),
              f"subscriber did not resolve the registered topic: "
              f"{sub.stdout_text()!r}")
        return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
                "evidence": "gateway REGISTERed the concrete topic, client "
                            "REGACKed, PUBLISH then arrived under that name"}
    finally:
        sub.kill()


def case_sn_short_topic(tools, port):
    return pubsub(tools, port, "s1", ["-q", "0"], ["-q", "0"], "s1: payload")


def case_sn_predefined_topic(tools, port):
    """The gateway boots with `predefined_topics => #{1 => <<"sn/pre">>}'; the
    client names id 1 with `-T' and never registers anything."""
    sub = start_sub(tools, port, ["-1", "-v", "-d", "-T", "1"])
    try:
        wait_subscribed(sub)
        tool, rc = run_pub(tools, port, ["-q", "1", "-d", "-T", "1",
                                         "-m", "payload"])
        check(rc == 0, f"publish failed rc={rc}\n{tool.stderr_text()}")
        check("REGACK" not in tool.stderr_text(),
              "a predefined id must not trigger REGISTER")
        check(sub.wait(timeout=20) == 0, "subscriber failed")
        out = sub.stdout_text()
        check("0001: payload" in out,
              f"subscriber did not print the predefined id: {out!r}")
        return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
                "evidence": "id 1 resolved through the gateway's predefined table"}
    finally:
        sub.kill()


def case_sn_retain(tools, port):
    tool, rc = run_pub(tools, port, ["-q", "1", "-r", "-d",
                                     "-t", "sn/compat/retain", "-m", "kept"])
    check(rc == 0, f"retained publish failed rc={rc}\n{tool.stderr_text()}")
    check("Received PUBACK" in tool.stderr_text(), "retained publish not acked")
    # A brand-new client, after the publisher is gone.
    sub = start_sub(tools, port, ["-1", "-v", "-d", "-t", "sn/compat/retain"])
    try:
        check(sub.wait(timeout=20) == 0, f"subscriber failed\n{sub.stderr_text()}")
        out = sub.stdout_text()
        check("sn/compat/retain: kept" in out,
              f"retained message not replayed: {out!r}")
        return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
                "evidence": "retained publish replayed to a later subscriber"}
    finally:
        sub.kill()


def case_sn_sleep_disconnect(tools, port):
    """`-e <n>' makes the tool send DISCONNECT **with a duration** and wait for
    the gateway's DISCONNECT. MQTT-SN 1.2 §6.14 answers a sleep request with the
    granted duration; the tool logs that as a warning, which is exactly the
    observable proof that the sleep request was understood (a plain disconnect
    would answer with a two-byte packet and no warning)."""
    tool, rc = run_pub(tools, port, ["-q", "0", "-e", "5", "-d",
                                     "-t", "s2", "-m", "asleep"])
    check(rc == 0, f"sleep disconnect failed rc={rc}\n{tool.stderr_text()}")
    err = tool.stderr_text()
    check("Gateway returned duration in disconnect packet" in err,
          "the gateway did not acknowledge the sleep duration\n" + err)
    return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
            "evidence": "DISCONNECT with duration acknowledged with the granted "
                        "duration (sleep accepted)"}


def case_sn_qos_minus1(tools, port):
    """QoS -1 is the level that needs no session: the tool deliberately skips
    CONNECT entirely, so this case also proves the stateless path."""
    sub = start_sub(tools, port, ["-1", "-v", "-d", "-q", "0", "-t", "m1"])
    try:
        wait_subscribed(sub)
        tool, rc = run_pub(tools, port, ["-q", "-1", "-d", "-t", "m1",
                                         "-m", "payload"])
        check(rc == 0, f"QoS -1 publish failed rc={rc}\n{tool.stderr_text()}")
        err = tool.stderr_text()
        check("Connecting..." not in err,
              "the tool opened a session; QoS -1 must not need one")
        check(sub.wait(timeout=20) == 0, "subscriber failed")
        out = sub.stdout_text()
        check("m1: payload" in out, f"subscriber did not get it: {out!r}")
        return {"client": "mqtt-sn-tools 0.0.7", "third_party": True,
                "evidence": "QoS -1 PUBLISH with no CONNECT reached a subscriber"}
    finally:
        sub.kill()


CASES = {
    "sn-connect": case_sn_connect,
    "sn-qos0": case_sn_qos0,
    "sn-qos1": case_sn_qos1,
    "sn-register": case_sn_register,
    "sn-register-downlink": case_sn_register_downlink,
    "sn-short-topic": case_sn_short_topic,
    "sn-predefined-topic": case_sn_predefined_topic,
    "sn-retain": case_sn_retain,
    "sn-sleep-disconnect": case_sn_sleep_disconnect,
    "sn-qos-minus1": case_sn_qos_minus1,
}

NOT_COVERED = {
    "qos2": "mqtt-sn-tools 0.0.7 has no QoS 2 code path; covered by "
            "mqtt_sn_conn_SUITE (both directions).",
    "will": "mqtt-sn-tools has no Will option; covered by mqtt_sn_conn_SUITE "
            "(Will vs. takeover).",
    "sleep-wake": "the tool's PINGREQ carries no client id, so it cannot wake a "
                  "sleeping session; covered by mqtt_sn_conn_SUITE.",
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--all", action="store_true", help="run every case")
    ap.add_argument("--case", action="append", default=[], help="run one case")
    ap.add_argument("--keep-going", action="store_true",
                    help="run every requested case even after a failure")
    ap.add_argument("--keep-data", action="store_true",
                    help="reuse the WAL data dir from a previous run")
    ap.add_argument("--build-tools", metavar="SRC",
                    help="build mqtt-sn-tools from SRC into _build/mqtt-sn-tools")
    args = ap.parse_args(argv)

    if args.build_tools:
        tools = build_tools(args.build_tools)
    else:
        tools = find_tools()

    selected = list(CASES) if args.all or not args.case else args.case
    unknown = [c for c in selected if c not in CASES]
    if unknown:
        raise SystemExit(f"unknown case(s): {', '.join(unknown)}\n"
                         f"known: {', '.join(CASES)}")

    if not args.keep_data:
        shutil.rmtree(DATA, ignore_errors=True)
    RUN.mkdir(parents=True, exist_ok=True)

    broker = start_broker()
    report = {"tools": str(tools), "cases": {}, "not_covered": NOT_COVERED}
    failed = []
    try:
        port = wait_ready(broker)
        report["port"] = port
        print(f"broker on udp 127.0.0.1:{port}, tools {tools}")
        for name in selected:
            started = time.time()
            try:
                result = CASES[name](tools, port)
                result["ok"] = True
                print(f"PASS {name} ({time.time() - started:.1f}s)")
            except CheckError as exc:
                result = {"ok": False, "error": str(exc)}
                failed.append(name)
                print(f"FAIL {name}: {exc}")
                (RUN / f"broker-{name}.log").write_text(broker_log_tail())
                if not args.keep_going:
                    report["cases"][name] = result
                    break
            report["cases"][name] = result
    finally:
        stop_broker(broker)
        (RUN / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    if failed:
        print(f"\n{len(failed)} failed: {', '.join(failed)}")
        print(f"report: {RUN / 'report.json'}")
        return 1
    print(f"\nall {len(report['cases'])} cases passed; report: {RUN / 'report.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
