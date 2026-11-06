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

The release must be buildable with the host's OTP 28.3.1; the image
`erlang:28.3.1.0` supplies the matching ERTS (16.2), so no host binaries are
copied into the container.  See tools/docker/README.md for the evidence trail.
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


SCENARIO_FUNCS = {
    "r1a": scenario_r1a,
    "r1b": scenario_r1b,
    "r2": scenario_r2,
    "r3": scenario_r3,
    "r4": scenario_r4,
    "r5": scenario_r5,
    "r6": scenario_r6,
    "r7": scenario_r7,
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
