#!/usr/bin/env python3
"""Shared docker/probe harness for the T14 (S1) and S2-B fault-injection drivers.

Extracted verbatim from tools/docker/s1_recovery.py so the S1 crash matrix and
the S2-B reliability drivers (tools/fault/os_buffer_loss.py,
tools/docker/s2_powerloss.py) share ONE implementation of the container
lifecycle, config rendering, the one-shot Erlang probes, the paho MQTT client
and the scenario assertion helpers.

The S1 scenarios (R1a..R7) live in s1_recovery.py and import everything from
here; the S2-B drivers add their own sys.path entry for tools/docker and import
it the same way.  See tools/docker/README.md for the evidence trail.
"""

import argparse
import base64
import glob
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

import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[2]
DOCKER = ROOT / "tools" / "docker"
DATA = DOCKER / "data"
RUN = DOCKER / "run"
RELEASE = ROOT / "_build" / "t14" / "rel" / "unified_mq"
CONTAINER = "unified-mq-s1-broker"
IMAGE = "erlang:28.3.1.0"
HOST = "127.0.0.1"
PORT = int(os.environ.get("S1_BROKER_PORT", "18830"))
DATA_IN_CONTAINER = "/var/lib/unified-mq"
OTP_ROOT = Path("/home/wanli/Development/otp-28.3.1/bin")

SCENARIOS = ["r1a", "r1b", "r2", "r3", "r4", "r5", "r6", "r7"]
TIERS = ["default", "zero"]

# R3's hard limit is "20k records replay in under 500 ms" (design §8.2); leave
# 3x headroom for a cold page cache on the CI host, exactly as the design does.
R6_BUDGET_MS = 500.0


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------

def _print_cmd(args):
    print("+ " + " ".join(str(a) for a in args), flush=True)


def run(cmd, check=True, timeout=600, cwd=ROOT, env=None):
    _print_cmd(cmd)
    proc = subprocess.run(
        [str(c) for c in cmd],
        cwd=str(cwd),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
    )
    if proc.stdout:
        print(proc.stdout.rstrip(), flush=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(map(str, cmd))}")
    return proc


def docker_args(tier):
    """Mounts and env shared by the broker container and the one-shot probes.

    The release is read-only: a crash must not leave state inside the artifact,
    and the only writable path is the real data directory.
    """
    return [
        "--user", f"{os.getuid()}:{os.getgid()}",
        "-w", DATA_IN_CONTAINER,
        "-e", f"HOME={DATA_IN_CONTAINER}",
        "-e", f"ERL_CRASH_DUMP={DATA_IN_CONTAINER}/erl_crash.dump",
        "-e", "ERL_LIBS=/opt/unified_mq/lib",
        "-e", f"S1_TIER={tier}",
        "-v", f"{RELEASE}:/opt/unified_mq:ro",
        "-v", f"{DOCKER / 'entrypoint.sh'}:/opt/s1/entrypoint.sh:ro",
        "-v", f"{RUN / 'sys.config'}:/opt/unified_mq/releases/0.1.0/sys.config:ro",
        "-v", f"{RUN / 'vm.args'}:/opt/unified_mq/releases/0.1.0/vm.args:ro",
        "-v", f"{DATA}:{DATA_IN_CONTAINER}",
    ]


def ensure_release():
    env = os.environ.copy()
    if (OTP_ROOT / "erl").exists():
        env["PATH"] = f"{OTP_ROOT}:{env.get('PATH', '')}"
    print("== release: rebar3 as t14 release", flush=True)
    run(["rebar3", "as", "t14", "release"], env=env, timeout=600)
    binary = RELEASE / "bin" / "unified_mq"
    if not binary.exists():
        raise RuntimeError(f"release missing: {binary}")
    # The harness image is OTP 28.3.1 (erts 16.2); a release built by another
    # OTP major would fail inside the container rather than silently drift.
    erl = shutil.which("erl", path=env.get("PATH", ""))
    if erl is None:
        raise RuntimeError("erl not found; export the project's OTP 28.3.1 PATH")
    ver = run([erl, "-noshell", "-eval",
               'io:format("~s/~s~n",[erlang:system_info(otp_release),'
               'erlang:system_info(version)]),halt().'], env=env).stdout.strip()
    print(f"== host OTP: {ver}", flush=True)
    # The release embeds this ERTS version in start_erl.data and the image
    # supplies exactly it; anything else would make the container warn (or
    # fail) instead of testing the pinned runtime.
    if ver != "28/16.2":
        raise RuntimeError(
            f"host OTP/ERTS is {ver}; T14 expects OTP 28.3.1 / ERTS 16.2 "
            f"(set PATH to /home/wanli/Development/otp-28.3.1/bin)")


def render_config(tier):
    """Render the shipped sys.config.src into the release's sys.config.

    One source of truth: the substitutions below are asserted, so a future edit
    to config/sys.config.src that removes a knob fails loudly instead of running
    the wrong tier.
    """
    text = (ROOT / "config" / "sys.config.src").read_text()
    data_pat = re.compile(r'\{data_dir,\s*"[^"]*"\}')

    def sub_once(pattern, repl, count=1, flags=0):
        nonlocal text
        text, n = re.subn(pattern, repl, text, count=count, flags=flags)
        if n != count:
            raise RuntimeError(f"sys.config render: {pattern!r} matched {n}, expected {count}")

    sub_once(data_pat, '{data_dir, "%s"}' % DATA_IN_CONTAINER, count=2)
    if tier == "default":
        sub_once(r'\{cursor_max_regress,\s*\d+\}', "{cursor_max_regress, 64}")
        sub_once(r'\{cursor_flush_interval_ms,\s*\d+\}', "{cursor_flush_interval_ms, 100}")
        sub_once(r'\{inflight_flush_interval_ms,\s*\d+\}', "{inflight_flush_interval_ms, 100}")
    elif tier == "zero":
        sub_once(r'\{cursor_max_regress,\s*\d+\}', "{cursor_max_regress, 1}")
        sub_once(r'\{cursor_flush_interval_ms,\s*\d+\}', "{cursor_flush_interval_ms, 0}")
        sub_once(r'\{inflight_flush_interval_ms,\s*\d+\}', "{inflight_flush_interval_ms, 0}")
    else:
        raise ValueError(tier)

    sub_once(r'"log/s1\.log"', '"%s/log/s1.log"' % DATA_IN_CONTAINER)

    RUN.mkdir(parents=True, exist_ok=True)
    (RUN / "sys.config").write_text(text)
    (RUN / "tier").write_text(tier + "\n")

    # Keep the production VM args verbatim, including -name: relx's launcher
    # requires exactly one node name.  The entrypoint starts a local epmd in the
    # container's network namespace, so no host cookie/epmd state is involved.
    vm = (ROOT / "config" / "vm.args.src").read_text()
    (RUN / "vm.args").write_text(vm)
    print(f"== rendered tier={tier}: {RUN / 'sys.config'}", flush=True)


def clear_data():
    if DATA.exists():
        for _ in range(3):
            try:
                shutil.rmtree(DATA)
                break
            except OSError as exc:
                print(f"   data cleanup retry: {exc}", flush=True)
                time.sleep(0.5)
        else:
            raise RuntimeError(f"could not remove {DATA}")
    DATA.mkdir(parents=True)
    (DATA / "log").mkdir()
    RUN.mkdir(parents=True, exist_ok=True)


def wait_port(open_timeout=20.0):
    """Wait until an MQTT CONNECT gets a CONNACK (the listener is last, so the
    port accepting is already past every app start)."""
    deadline = time.monotonic() + open_timeout
    last = None
    while time.monotonic() < deadline:
        c = Client(f"s1t14-probe-{os.getpid()}-{int(time.time() * 1000)}", clean=True)
        try:
            c.connect(timeout=0.5)
            c.close()
            return
        except Exception as exc:  # ConnectionRefused / timeout / protocol
            last = exc
            c.force_close()
            time.sleep(0.1)
    raise RuntimeError(f"broker did not become ready: {last!r}")


def broker_running():
    out = run(["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER],
              check=False, timeout=30)
    return out.stdout.strip() == "true"


def kill_broker(tier):
    run(["docker", "kill", "-s", "KILL", CONTAINER], timeout=60)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if not broker_running():
            return
        time.sleep(0.05)
    raise RuntimeError("broker container still running after kill -9")


def stop_broker(tier):
    run(["docker", "stop", "-t", "10", CONTAINER], timeout=60)


def start_broker(tier):
    # After kill -9 the container exists in `exited` state; docker start re-runs
    # the entrypoint with the same mounts/env.  Only a fresh scenario creates a
    # new container.
    state = run(["docker", "inspect", "-f", "{{.State.Status}}", CONTAINER],
                check=False, timeout=30).stdout.strip()
    if state in ("exited", "created", "paused"):
        run(["docker", "start", CONTAINER], timeout=120)
    else:
        run(["docker", "run", "-d", "--name", CONTAINER, *docker_args(tier),
             "-p", f"127.0.0.1:{PORT}:1883", "--stop-timeout", "10",
             "--entrypoint", "/bin/sh", IMAGE, "/opt/s1/entrypoint.sh"], timeout=120)
    wait_port()


def fresh_start(tier):
    run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)
    clear_data()
    render_config(tier)
    start_broker(tier)


def broker_logs(tail=200):
    proc = run(["docker", "logs", "--tail", str(tail), CONTAINER],
               check=False, timeout=60)
    return proc.stdout


def erl_probe(code, tier, timeout=120):
    """Run a one-shot Erlang VM with the release's code path and data volume."""
    return run(
        ["docker", "run", "--rm", *docker_args(tier),
         "--entrypoint", "erl", IMAGE,
         "-noshell", "-boot", "no_dot_erlang", "-config",
         "/opt/unified_mq/releases/0.1.0/sys.config",
         "-eval", code],
        check=True, timeout=timeout,
    ).stdout


def probe_recover(tier, timeout=180):
    """Recover every partition in a throwaway VM and report per-partition
    watermarks plus the total replay time in microseconds."""
    code = (
        'application:load(broker_storage),'
        'application:load(broker_queue),'
        '{ok, _} = application:ensure_all_started(broker_storage),'
        'Backend = application:get_env(broker_storage, backend, broker_storage_wal),'
        'Config = broker_queue:storage_config(),'
        'Ps = broker_queue:partition_ids(),'
        '{Micros, Results} = timer:tc(fun() -> '
        '  [{P, Backend:recover(P, Config)} || P <- Ps] end),'
        'lists:foreach(fun({P, R}) -> case R of '
        '  {ok, _, C, H} -> io:format("WAL\\t~p\\t~p\\t~p~n", [P, C, H]);'
        '  Other -> io:format("WALERR\\t~p\\t~p~n", [P, Other]) end end, Results),'
        'io:format("RECOVERY_US\\t~p~n", [Micros]), halt(0).'
    )
    out = erl_probe(code, tier, timeout=timeout)
    highs, errs, micros = {}, [], None
    for line in out.splitlines():
        fields = line.split("\t")
        if len(fields) == 4 and fields[0] == "WAL":
            highs[int(fields[1])] = {"committed": int(fields[2]), "high": int(fields[3])}
        elif len(fields) == 3 and fields[0] == "WALERR":
            errs.append({"partition": fields[1], "error": fields[2]})
        elif len(fields) == 2 and fields[0] == "RECOVERY_US":
            micros = int(fields[1])
    if errs or micros is None:
        raise RuntimeError(f"recovery probe failed: {out}")
    return {"highs": highs, "micros": micros, "raw": out}


def probe_cursors(client_id, tier):
    path = DATA / "sessions" / (b64url(client_id) + ".cursors")
    if not path.exists():
        return {"exists": False, "cursors": []}
    code = (
        f'Path = "{DATA_IN_CONTAINER}/sessions/{path.name}",'
        'case file:read_file(Path) of '
        '  {ok, B} -> {vsn, 1, _, Body} = binary_to_term(B),'
        '    lists:foreach(fun({F, Ps}) -> lists:foreach(fun({P, O}) ->'
        '      io:format("CURSOR\\t~s\\t~p\\t~p~n", [F, P, O]) end, Ps) end, Body);'
        '  {error, R} -> io:format("CURSOR_ERR\\t~p~n", [R])'
        'end, halt(0).'
    )
    out = erl_probe(code, tier, timeout=60)
    entries = []
    for line in out.splitlines():
        fields = line.split("\t")
        if len(fields) == 4 and fields[0] == "CURSOR":
            entries.append({"filter": fields[1], "partition": int(fields[2]),
                            "offset": int(fields[3])})
        elif line.startswith("CURSOR_ERR"):
            print(line, flush=True)
    return {"exists": True, "cursors": entries}


def b64url(client_id):
    return base64.urlsafe_b64encode(client_id.encode()).decode().rstrip("=")


def subs_path(client_id):
    return DATA / "sessions" / (b64url(client_id) + ".subs")


# ---------------------------------------------------------------------------
# MQTT client
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, client_id, clean=True, manual_ack=False):
        self.client_id = client_id
        self._lock = threading.Lock()
        self._connected = threading.Event()
        self._session_present = None
        self._reason = None
        self._messages = queue.Queue()
        self._pubacked = set()
        self._pubcv = threading.Condition(self._lock)
        self._subacked = set()
        self._subcv = threading.Condition(self._lock)
        self._c = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=clean,
            protocol=mqtt.MQTTv311,
            reconnect_on_failure=False,
            manual_ack=manual_ack,
        )
        self._c.username_pw_set("guest", "guest")
        self._c.max_inflight_messages_set(200)
        self._c.max_queued_messages_set(0)
        self._c.on_connect = self._on_connect
        self._c.on_disconnect = self._on_disconnect
        self._c.on_message = self._on_message
        self._c.on_publish = self._on_publish
        self._c.on_subscribe = self._on_subscribe

    # Callbacks (paho 2.x VERSION2 signatures; *args keeps them version-proof).
    def _on_connect(self, _client, _userdata, flags, reason, *_rest):
        self._session_present = bool(getattr(flags, "session_present", False))
        self._reason = reason
        self._connected.set()

    def _on_disconnect(self, _client, _userdata, *_rest):
        self._connected.clear()

    def _on_message(self, _client, _userdata, msg):
        self._messages.put(msg)

    def _on_publish(self, _client, _userdata, mid, *_rest):
        with self._pubcv:
            self._pubacked.add(mid)
            self._pubcv.notify_all()

    def _on_subscribe(self, _client, _userdata, mid, *_rest):
        with self._subcv:
            self._subacked.add(mid)
            self._subcv.notify_all()

    def connect(self, timeout=5.0):
        rc = self._c.connect(HOST, PORT, keepalive=30)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"connect returned {rc}")
        self._c.loop_start()
        if not self._connected.wait(timeout):
            self.force_close()
            raise RuntimeError(f"no CONNACK for {self.client_id}")
        return self._session_present

    def subscribe(self, topic_filter, qos=1, timeout=5.0):
        rc, mid = self._c.subscribe(topic_filter, qos)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"subscribe returned {rc}")
        with self._subcv:
            if not self._subcv.wait_for(lambda: mid in self._subacked, timeout):
                raise RuntimeError(f"no SUBACK for {topic_filter}")
        return True

    def publish(self, topic, payload, qos=1, wait=True, timeout=10.0):
        info = self._c.publish(topic, payload, qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"publish returned {info.rc}")
        mid = info.mid
        if qos == 1 and wait:
            with self._pubcv:
                if not self._pubcv.wait_for(lambda: mid in self._pubacked, timeout):
                    raise RuntimeError(f"no PUBACK for mid {mid}")
        return mid

    def wait_pubacks(self, mids, timeout=180.0):
        wanted = set(mids)
        deadline = time.monotonic() + timeout
        with self._pubcv:
            while True:
                missing = wanted - self._pubacked
                if not missing:
                    return
                left = deadline - time.monotonic()
                if left <= 0:
                    raise RuntimeError(f"missing {len(missing)} PUBACKs")
                self._pubcv.wait(min(left, 1.0))

    def publish_many(self, topics, payloads, qos=1, timeout=180.0):
        mids = [self.publish(t, p, qos, wait=False) for t, p in zip(topics, payloads)]
        if qos == 1:
            self.wait_pubacks(mids, timeout=timeout)

    def next_message(self, timeout=5.0):
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def collect(self, quiet=0.6, total=6.0):
        """Collect until the socket has been quiet for `quiet' seconds."""
        out = []
        start = time.monotonic()
        last = start
        while time.monotonic() - start < total:
            try:
                msg = self._messages.get(timeout=0.05)
            except queue.Empty:
                if out and time.monotonic() - last >= quiet:
                    break
                if not out and time.monotonic() - start >= total:
                    break
            else:
                out.append(msg)
                last = time.monotonic()
        return out

    def ack(self, msg):
        if msg.qos:
            self._c.ack(msg.mid, msg.qos)

    def close(self, timeout=2.0):
        try:
            self._c.disconnect()
        except Exception:
            pass
        self._connected.clear()
        try:
            self._c.loop_stop()
        except Exception:
            pass

    def force_close(self):
        try:
            self._c.loop_stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------

def offline_subscriber(client_id, topic_filter, qos):
    c = Client(client_id, clean=False)
    c.connect()
    c.subscribe(topic_filter, qos)
    c.close()
    path = subs_path(client_id)
    if not path.exists():
        raise RuntimeError(f"R1b/R2 fixture: {path} was not written")
    return path.stat().st_size


def reconnect(client_id, manual_ack=False):
    c = Client(client_id, clean=False, manual_ack=manual_ack)
    session_present = c.connect()
    return c, session_present


def payload_indices(messages, prefix="msg-"):
    out = []
    for m in messages:
        text = m.payload.decode()
        if not text.startswith(prefix):
            raise AssertionError(f"unexpected/corrupt payload {m.payload!r}")
        out.append(int(text[len(prefix):]))
    return out


def assert_true(cond, message):
    if not cond:
        raise AssertionError(message)

