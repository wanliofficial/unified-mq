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
import socket
import struct
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

SCENARIOS = ["r1a", "r1b", "r2", "r3", "r4", "r5", "r6", "r7", "r8", "r9"]
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


def substitute_data_dirs(text):
    """Rewrite every `data_dir' to the bind mount, preserving its suffix.

    Five layers own the key (storage, subscription, retain, and the S4 control
    plane's Mnesia/ra stores). Keeping the relative suffix means ra lands in
    `DATA_IN_CONTAINER/ra' instead of at the mount root. The count is asserted
    so a new app cannot silently keep pointing at the dev host.
    """
    data_pat = re.compile(r'\{data_dir,\s*"([^"]*)"\}')

    def map_data_dir(match):
        host = Path(match.group(1))
        try:
            rel = host.relative_to(ROOT / "data")
        except ValueError:
            raise RuntimeError(
                f"sys.config render: data_dir {host} is not under {ROOT / 'data'}")
        target = DATA_IN_CONTAINER if str(rel) == "." else str(
            Path(DATA_IN_CONTAINER) / rel)
        return '{data_dir, "%s"}' % target

    text, n = data_pat.subn(map_data_dir, text)
    if n != 5:
        raise RuntimeError(f"sys.config render: data_dir matched {n}, expected 5")
    return text


def render_config(tier):
    """Render the shipped sys.config.src into the release's sys.config.

    One source of truth: the substitutions below are asserted, so a future edit
    to config/sys.config.src that removes a knob fails loudly instead of running
    the wrong tier.
    """
    text = (ROOT / "config" / "sys.config.src").read_text()

    def sub_once(pattern, repl, count=1, flags=0):
        nonlocal text
        text, n = re.subn(pattern, repl, text, count=count, flags=flags)
        if n != count:
            raise RuntimeError(f"sys.config render: {pattern!r} matched {n}, expected {count}")

    # Five layers own a `data_dir' key (storage, subscription, retain, and the
    # S4 control plane's Mnesia/ra stores). Rewrite each to the bind mount while
    # keeping its relative suffix, so ra lands in DATA_IN_CONTAINER/ra instead
    # of at the mount root.
    text = substitute_data_dirs(text)
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


def probe_cursors(client_id, tier, topic_filter, kind="exclusive"):
    """Read one v2 `<subref>.cursors' file (ADR-019 §19.3).

    The file is `{vsn, 2, SubRef, #{kind, rows}}'; a plain row carries `offset'
    and a shared row carries `low' (the first offset nobody acked), so the
    reported `offset' is the last committed position either way.
    """
    path = DATA / "sessions" / (subref_name(kind, client_id, topic_filter) + ".cursors")
    if not path.exists():
        return {"exists": False, "cursors": []}
    code = (
        f'Path = "{DATA_IN_CONTAINER}/sessions/{path.name}",'
        'case file:read_file(Path) of '
        '  {ok, B} -> case binary_to_term(B) of'
        '    {vsn, 2, _SubRef, Body} ->'
        '      maps:foreach(fun(P, Row) ->'
        '        K = maps:get(kind, Row),'
        '        O = case Row of #{low := L} -> L - 1; #{offset := Off} -> Off end,'
        '        io:format("CURSOR\\t~p\\t~w\\t~p\\t~p~n",'
        '                  [P, K, O, map_size(maps:get(acked, Row, #{}))])'
        '      end, maps:get(rows, Body));'
        '    Other -> io:format("CURSOR_ERR\\t~p~n", [Other])'
        '  end;'
        '  {error, R} -> io:format("CURSOR_ERR\\t~p~n", [R])'
        'end, halt(0).'
    )
    out = erl_probe(code, tier, timeout=60)
    entries = []
    for line in out.splitlines():
        fields = line.split("\t")
        if len(fields) == 5 and fields[0] == "CURSOR":
            entries.append({"partition": int(fields[1]), "kind": fields[2],
                            "offset": int(fields[3]), "acked": int(fields[4])})
        elif line.startswith("CURSOR_ERR"):
            print(line, flush=True)
    return {"exists": True, "cursors": entries}


def b64url(value):
    if isinstance(value, str):
        value = value.encode()
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def subref_name(kind, key, topic_filter):
    """`<kind>.<b64(key)>.<b64(filter)>' -- the v2 filename stem."""
    return f"{kind}.{b64url(key)}.{b64url(topic_filter)}"


def subs_path(client_id, topic_filter, kind="exclusive", key=None):
    key = client_id if key is None else key
    return DATA / "sessions" / (subref_name(kind, key, topic_filter) + ".subs")


def author_shared_group(tier, group, topic_filter, consumers, qos=1):
    """Write a Shared `.subs' before boot (ADR-019 §19.3).

    Shared subscriptions have no wire syntax until S2.5 (`$share/...`), so the
    crash matrix authors the group's session file with the broker's own writer
    and lets boot restore it exactly the way a real one would be restored.
    """
    peers = ", ".join('<<"%s">>' % c for c in consumers)
    body = (
        '#{kind => shared, key => <<"%s">>, filter => <<"%s">>, qos => %d,'
        ' consumers => [%s], init_cursors => [],'
        ' created_at => erlang:system_time(millisecond)}'
        % (group, topic_filter, qos, peers)
    )
    code = (
        'application:load(broker_subscription),'
        f'Body = {body},'
        f'ok = broker_subscription_files:write_subs('
        f'{{shared, <<"{group}">>, <<"{topic_filter}">>}}, Body),'
        'io:format("AUTHORED\\t~s\\n", [broker_subscription_files:subs_path('
        f'{{shared, <<"{group}">>, <<"{topic_filter}">>}})]),'
        'halt(0).'
    )
    out = erl_probe(code, tier, timeout=60)
    marker = [l for l in out.splitlines() if l.startswith("AUTHORED")]
    if not marker:
        raise RuntimeError(f"shared group was not authored: {out}")
    return marker[0].split("\t", 1)[1]


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
        # QoS 2 completes on PUBCOMP, which paho reports through the same
        # on_publish callback as PUBACK (ADR-018 §18.3: the broker only finishes
        # the exchange there), so the wait is identical.
        if qos in (1, 2) and wait:
            with self._pubcv:
                if not self._pubcv.wait_for(lambda: mid in self._pubacked, timeout):
                    raise RuntimeError(f"no {'PUBACK' if qos == 1 else 'PUBCOMP'} "
                                       f"for mid {mid}")
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
# Raw MQTT client (R8: the kill has to land between PUBREC and PUBREL, which
# paho's loop thread will not let us time)
# ---------------------------------------------------------------------------

def _mqtt_str(value):
    raw = value.encode() if isinstance(value, str) else value
    return struct.pack("!H", len(raw)) + raw


def _mqtt_varint(n):
    out = bytearray()
    while True:
        byte = n % 128
        n //= 128
        if n:
            byte |= 0x80
        out.append(byte)
        if not n:
            return bytes(out)


def _mqtt_frame(type_flags, body=b""):
    return bytes([type_flags]) + _mqtt_varint(len(body)) + body


class RawClient:
    """A hand-rolled MQTT 3.1.1 client for the handful of bytes paho hides.

    Only what R8 needs: CONNECT, SUBSCRIBE, PUBLISH at QoS 0/1/2, and the
    PUBREC/PUBREL/PUBCOMP half-frames.  Framing is decoded strictly enough to
    read the frames the broker sends back, so a malformed answer fails loudly
    instead of being mis-parsed as a different packet type.
    """

    CONNECT, CONNACK = 0x10, 0x20
    PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP = 0x30, 0x40, 0x50, 0x62, 0x70
    SUBSCRIBE, SUBACK = 0x82, 0x90

    def __init__(self, client_id, clean=True, will=None, keepalive=60):
        self.client_id = client_id
        self.clean = clean
        self.will = will
        self.keepalive = keepalive
        self.sock = None
        self._buf = b""

    # -- connection ------------------------------------------------------
    def connect(self, timeout=10.0):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        flags = 0x02 if self.clean else 0x00
        payload = _mqtt_str(self.client_id)
        if self.will:
            flags |= 0x04 | ((self.will.get("qos", 0) & 0x03) << 3)
            if self.will.get("retain"):
                flags |= 0x20
            payload += _mqtt_str(self.will["topic"]) + _mqtt_str(self.will["payload"])
        body = (_mqtt_str("MQTT") + bytes([4, flags])
                + struct.pack("!H", self.keepalive) + payload)
        self._send(_mqtt_frame(self.CONNECT, body))
        kind, body = self.recv()
        assert_true(kind == self.CONNACK, f"R8: expected CONNACK, got 0x{kind:02x}")
        return body[1] & 0x01  # session_present

    def close(self):
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def disconnect(self):
        self._send(_mqtt_frame(0xE0))
        self.close()

    # -- frames ----------------------------------------------------------
    def _send(self, raw):
        self.sock.sendall(raw)

    def recv(self, timeout=15.0):
        """Read one frame; returns `(type_byte, body)`."""
        self.sock.settimeout(timeout)
        while True:
            if len(self._buf) >= 2:
                mult, value, idx = 1, 0, 1
                complete = False
                while idx < len(self._buf):
                    byte = self._buf[idx]
                    value += (byte & 0x7F) * mult
                    mult *= 128
                    idx += 1
                    if not byte & 0x80:
                        complete = True
                        break
                    if idx > 4:
                        raise RuntimeError("R8: malformed remaining length")
                if complete and len(self._buf) >= idx + value:
                    kind = self._buf[0] & 0xF0
                    body = self._buf[idx:idx + value]
                    self._buf = self._buf[idx + value:]
                    return kind, body
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RuntimeError("R8: connection closed while waiting for a frame")
            self._buf += chunk

    def subscribe(self, topic_filter, qos=1, packet_id=1):
        body = (struct.pack("!H", packet_id) + _mqtt_str(topic_filter)
                + bytes([qos & 0x03]))
        self._send(_mqtt_frame(self.SUBSCRIBE, body))
        kind, body = self.recv()
        assert_true(kind == self.SUBACK, f"R8: expected SUBACK, got 0x{kind:02x}")
        return body[2]  # the granted code

    def publish(self, topic, payload, qos=0, packet_id=None, retain=False):
        if isinstance(payload, str):
            payload = payload.encode()
        head = _mqtt_str(topic)
        flags = (qos & 0x03) << 1
        if retain:
            flags |= 0x01
        if qos:
            assert_true(packet_id is not None and 0 < packet_id < 65536,
                        "R8: a QoS>0 PUBLISH needs a packet id")
            head += struct.pack("!H", packet_id)
        self._send(_mqtt_frame(self.PUBLISH | flags, head + payload))

    def pubrel(self, packet_id):
        self._send(_mqtt_frame(self.PUBREL, struct.pack("!H", packet_id)))

    def pubrec(self, packet_id):
        self._send(_mqtt_frame(self.PUBREC, struct.pack("!H", packet_id)))

    def pubcomp(self, packet_id):
        self._send(_mqtt_frame(self.PUBCOMP, struct.pack("!H", packet_id)))

def packet_id(body):
    """The two-byte id that every ack frame in §3.4-3.7 carries."""
    return struct.unpack("!H", body[:2])[0]


def publish_topic(body):
    """The topic of a PUBLISH body (its payload follows the packet id)."""
    tlen = struct.unpack("!H", body[:2])[0]
    return body[2:2 + tlen].decode()

# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------

def offline_subscriber(client_id, topic_filter, qos):
    c = Client(client_id, clean=False)
    c.connect()
    c.subscribe(topic_filter, qos)
    c.close()
    path = subs_path(client_id, topic_filter)
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

