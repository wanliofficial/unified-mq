#!/usr/bin/env python3
"""T15: real-client compatibility for the S1 MQTT slice (design §4.1/§4.5).

Runs the T14 release in an OTP 28.3.1 container and puts a transparent TCP tee
between real clients and the broker.  Every byte the client sends is decoded by
the production codec (`mqtt_frame`) and re-encoded; a frame that does not
round-trip byte-for-byte means our encoder cannot reproduce what a real client
actually put on the wire -- the gap T8's hand-written golden vectors cannot
see (design §13 M3).

Clients:
  * paho-mqtt 2.x (Python, host)
  * mosquitto_pub / mosquitto_sub 2.1.2 (eclipse-mosquitto:2, host network)

Cases:
  paho311-full       CONNECT(+creds)/SUBSCRIBE/PUBLISH qos1/PUBACK/PINGREQ/
                     PINGRESP/DISCONNECT, every client frame byte-identical
  paho-v5-connect    MQTT 5 CONNECT accepted (S2.5): v5 CONNACK 0x00
  paho-qos2          full QoS2 in both directions (S2-C, ADR-018 §18.3)
  paho-retain        retain stored on subscribe, retain=0 on forward,
                     empty payload clears it
  paho-will          Will published on an abnormal close (S2-C, §18.1)
  paho-bad-filter    NUL in filter (client accepts it) -> close, no SUBACK
  mosq-pubsub        mosquitto 2.1.2 QoS1 round trip + byte-level compare
  mosq-qos2          mosquitto 2.1.2 QoS2 round trip
  mosq-v5-pubsub     mosquitto MQTT 5 publish accepted (S2.5)

Usage:
    python3 test/protocol-compatibility/mqtt/t15_compat.py
    python3 test/protocol-compatibility/mqtt/t15_compat.py --case paho311-full
    python3 test/protocol-compatibility/mqtt/t15_compat.py --skip-build --keep-data
"""

import argparse
import base64
import json
import os
import queue
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt

ROOT = Path(__file__).resolve().parents[3]
MQTT_DIR = Path(__file__).resolve().parent
RUN = MQTT_DIR / "run"
DATA = MQTT_DIR / "data"
EBIN = RUN / "ebin"
CAPTURES = RUN / "captures"
RELEASE = ROOT / "_build" / "t14" / "rel" / "unified_mq"
ENTRYPOINT = ROOT / "tools" / "docker" / "entrypoint.sh"
IMAGE = "erlang:28.3.1.0"
MOSQ_IMAGE = "eclipse-mosquitto:2"
CONTAINER = "unified-mq-t15-broker"
HOST = "127.0.0.1"
BROKER_PORT = int(os.environ.get("S1_BROKER_PORT", "18831"))
PROXY_PORT = int(os.environ.get("S1_PROXY_PORT", "18832"))
DATA_IN_CONTAINER = "/var/lib/unified-mq"
OTP_ROOT = Path("/home/wanli/Development/otp-28.3.1/bin")
ANALYZER_SRC = MQTT_DIR / "t15_capture.erl"

CASES = ["paho311-full", "paho-v5-connect", "paho-qos2", "paho-retain",
         "paho-will", "paho-will-skipped", "paho-bad-filter", "mosq-pubsub",
         "mosq-qos2", "mosq-v5-pubsub"]


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------

def run(cmd, check=True, timeout=600, env=None, cwd=ROOT):
    print("+ " + " ".join(str(c) for c in cmd), flush=True)
    proc = subprocess.run([str(c) for c in cmd], cwd=str(cwd), env=env, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=timeout)
    if proc.stdout:
        print(proc.stdout.rstrip(), flush=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(map(str, cmd))}")
    return proc


def otp_env():
    env = os.environ.copy()
    if (OTP_ROOT / "erl").exists():
        env["PATH"] = f"{OTP_ROOT}:{env.get('PATH', '')}"
    return env


def ensure_release():
    env = otp_env()
    if not (RELEASE / "bin" / "unified_mq").exists():
        print("== release: rebar3 as t14 release", flush=True)
        run(["rebar3", "as", "t14", "release"], env=env, timeout=600)


def compile_analyzer():
    EBIN.mkdir(parents=True, exist_ok=True)
    erlc = shutil.which("erlc", path=otp_env()["PATH"])
    if erlc is None:
        raise RuntimeError("erlc not found; export the project's OTP 28.3.1 PATH")
    run([erlc, "-o", EBIN, "-I", ROOT / "apps/broker_proto_mqtt/include",
         ANALYZER_SRC], env=otp_env())


def render_config():
    text = (ROOT / "config" / "sys.config.src").read_text()
    # Five layers own a `data_dir' key (storage, subscription, retain, and the
    # S4 control plane's Mnesia/ra stores). Rewrite each to the bind mount while
    # keeping its relative suffix, so ra lands in DATA_IN_CONTAINER/ra instead
    # of at the mount root; the count is asserted so a new app cannot silently
    # keep pointing at the dev host.
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
    # The admin backup directory is the sixth on-disk path; mount it too so a
    # backup created in the container lands on the bind-mounted data volume.
    text, n = re.subn(r'\{backup_dir,\s*"[^"]*"\}',
                      '{backup_dir, "%s"}' % (Path(DATA_IN_CONTAINER) / "backups"),
                      text)
    if n != 1:
        raise RuntimeError(f"sys.config render: backup_dir matched {n}, expected 1")
    text, n = re.subn(r'"log/s1\.log"', '"%s/log/s1.log"' % DATA_IN_CONTAINER, text)
    if n != 1:
        raise RuntimeError(f"sys.config render: log path matched {n}, expected 1")

    RUN.mkdir(parents=True, exist_ok=True)
    (RUN / "sys.config").write_text(text)
    # Keep vm.args verbatim (including -name): relx's launcher requires exactly
    # one node name, and the entrypoint starts a container-local epmd.
    (RUN / "vm.args").write_text((ROOT / "config" / "vm.args.src").read_text())


def clear_runtime():
    for path in (DATA, CAPTURES):
        if path.exists():
            shutil.rmtree(path)
    DATA.mkdir(parents=True)
    (DATA / "log").mkdir()


def docker_args():
    return [
        "--user", f"{os.getuid()}:{os.getgid()}",
        "-w", DATA_IN_CONTAINER,
        "-e", f"HOME={DATA_IN_CONTAINER}",
        "-e", f"ERL_CRASH_DUMP={DATA_IN_CONTAINER}/erl_crash.dump",
        "-e", "ERL_LIBS=/opt/unified_mq/lib",
        "-v", f"{RELEASE}:/opt/unified_mq:ro",
        "-v", f"{ENTRYPOINT}:/opt/s1/entrypoint.sh:ro",
        "-v", f"{RUN / 'sys.config'}:/opt/unified_mq/releases/0.1.0/sys.config:ro",
        "-v", f"{RUN / 'vm.args'}:/opt/unified_mq/releases/0.1.0/vm.args:ro",
        "-v", f"{DATA}:{DATA_IN_CONTAINER}",
    ]


def start_broker(fresh):
    if fresh:
        run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)
        clear_runtime()
        render_config()
        run(["docker", "run", "-d", "--name", CONTAINER, *docker_args(),
             "-p", f"127.0.0.1:{BROKER_PORT}:1883", "--stop-timeout", "10",
             "--entrypoint", "/bin/sh", IMAGE, "/opt/s1/entrypoint.sh"], timeout=120)
    else:
        run(["docker", "start", CONTAINER], timeout=120)
    wait_broker()


def wait_broker(timeout=40.0):
    """Wait for a real CONNACK, not just a TCP accept.

    Docker publishes the host port before the release has bound ranch's
    listener, so `connect()` can succeed against an empty forward and the
    CONNECT then vanishes.  A full handshake is the only honest readiness
    check; every case starts from here.
    """
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        got = threading.Event()
        probe = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                            client_id=f"t15-probe-{os.getpid()}-{time.time_ns()}",
                            clean_session=True, protocol=mqtt.MQTTv311,
                            reconnect_on_failure=False)
        probe.on_connect = lambda *_a, **_k: got.set()
        try:
            probe.connect(HOST, BROKER_PORT, keepalive=5)
            probe.loop_start()
            if got.wait(2.0):
                probe.disconnect()
                time.sleep(0.05)
                probe.loop_stop()
                return
            last = "no CONNACK within 2s"
        except Exception as exc:  # refused / timeout / protocol
            last = exc
        try:
            probe.loop_stop()
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"broker did not become ready: {last!r}")


def stop_broker():
    run(["docker", "rm", "-f", CONTAINER], check=False, timeout=60)


def broker_logs():
    return run(["docker", "logs", CONTAINER], check=False, timeout=60).stdout


# ---------------------------------------------------------------------------
# Transparent tee
# ---------------------------------------------------------------------------

class Tee:
    """Accept MQTT connections on `listen_port`, forward to `upstream_port`,
    and record each direction to `<prefix>-conn<N>-{c2s,s2c}.bin`."""

    def __init__(self, listen_port, upstream_port, prefix):
        self.listen_port = listen_port
        self.upstream_port = upstream_port
        self.prefix = prefix
        self._stop = threading.Event()
        self._threads = []
        self._sockets = []
        self._lock = threading.Lock()
        self._n = 0
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind((HOST, listen_port))
        self._srv.listen(16)
        self._srv.settimeout(0.2)

    def start(self):
        t = threading.Thread(target=self._accept, daemon=True)
        t.start()
        self._threads.append(t)

    def _accept(self):
        while not self._stop.is_set():
            try:
                client, _ = self._srv.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                idx = self._n
                self._n += 1
            try:
                upstream = socket.create_connection((HOST, self.upstream_port), timeout=5)
            except OSError:
                client.close()
                continue
            for s in (client, upstream):
                s.settimeout(None)
                with self._lock:
                    self._sockets.append(s)
            c2s = open(CAPTURES / f"{self.prefix}-conn{idx}-c2s.bin", "wb")
            s2c = open(CAPTURES / f"{self.prefix}-conn{idx}-s2c.bin", "wb")
            for src, dst, fh, name in ((client, upstream, c2s, "c2s"),
                                       (upstream, client, s2c, "s2c")):
                t = threading.Thread(target=self._relay,
                                     args=(src, dst, fh, name), daemon=True)
                t.start()
                self._threads.append(t)

    def _relay(self, src, dst, fh, name):
        try:
            while not self._stop.is_set():
                data = src.recv(65536)
                if not data:
                    break
                fh.write(data)
                fh.flush()
                dst.sendall(data)
        except OSError:
            pass
        finally:
            try:
                fh.close()
            except Exception:
                pass
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def stop(self):
        self._stop.set()
        try:
            self._srv.close()
        except OSError:
            pass
        with self._lock:
            socks = list(self._sockets)
        for s in socks:
            try:
                s.close()
            except OSError:
                pass
        for t in self._threads:
            t.join(timeout=2.0)
        # Give relay threads one more moment to close their capture files.
        time.sleep(0.1)


# ---------------------------------------------------------------------------
# paho client
# ---------------------------------------------------------------------------

class Client:
    def __init__(self, client_id, clean=True, protocol=mqtt.MQTTv311,
                 keepalive=30, will=None, username="guest", password="guest",
                 manual_ack=False):
        self.client_id = client_id
        self.keepalive = keepalive
        self._connected = threading.Event()
        self._disconnected = threading.Event()
        self._session_present = None
        self._connect_reason = None
        self._messages = queue.Queue()
        self._ack = threading.Condition()
        self._pubacked = set()
        self._subacked = set()
        self._clean = clean
        self._protocol = protocol
        kwargs = {}
        if protocol == mqtt.MQTTv5:
            # paho 2.x takes `clean_start` on connect(), not the constructor.
            pass
        else:
            kwargs["clean_session"] = clean
        self._c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                              client_id=client_id, protocol=protocol,
                              reconnect_on_failure=False,
                              manual_ack=manual_ack, **kwargs)
        if username is not None:
            self._c.username_pw_set(username, password)
        if will is not None:
            topic, payload, qos = will
            self._c.will_set(topic, payload, qos=qos)
        self._c.max_inflight_messages_set(64)
        self._c.on_connect = self._on_connect
        self._c.on_disconnect = self._on_disconnect
        self._c.on_message = self._on_message
        self._c.on_publish = self._on_publish
        self._c.on_subscribe = self._on_subscribe

    def _on_connect(self, _c, _u, flags, reason, *_rest):
        self._session_present = bool(getattr(flags, "session_present", False))
        self._connect_reason = reason
        self._connected.set()

    def _on_disconnect(self, _c, _u, *_rest):
        self._connected.clear()
        self._disconnected.set()

    def _on_message(self, _c, _u, msg):
        self._messages.put(msg)

    def _on_publish(self, _c, _u, mid, *_rest):
        with self._ack:
            self._pubacked.add(mid)
            self._ack.notify_all()

    def _on_subscribe(self, _c, _u, mid, *_rest):
        with self._ack:
            self._subacked.add(mid)
            self._ack.notify_all()

    def connect(self, timeout=10.0):
        if self._protocol == mqtt.MQTTv5:
            rc = self._c.connect(HOST, PROXY_PORT, keepalive=self.keepalive,
                                 clean_start=self._clean)
        else:
            rc = self._c.connect(HOST, PROXY_PORT, keepalive=self.keepalive)
        self._c.loop_start()
        if not self._connected.wait(timeout):
            raise RuntimeError(f"no CONNACK for {self.client_id} (rc={rc})")
        return self._session_present

    def subscribe(self, topic_filter, qos=1, timeout=10.0):
        rc, mid = self._c.subscribe(topic_filter, qos)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"subscribe returned {rc}")
        with self._ack:
            if not self._ack.wait_for(lambda: mid in self._subacked, timeout):
                raise RuntimeError(f"no SUBACK for {topic_filter}")
        return mid

    def publish(self, topic, payload, qos=1, retain=False, wait=True, timeout=15.0):
        info = self._c.publish(topic, payload, qos, retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"publish returned {info.rc}")
        # paho reports QoS2 completion through the same on_publish callback as
        # QoS1, but only after PUBCOMP (it drives PUBREC/PUBREL itself), so the
        # wait is identical and covers both.
        if qos in (1, 2) and wait:
            with self._ack:
                if not self._ack.wait_for(lambda: info.mid in self._pubacked, timeout):
                    raise RuntimeError(
                        f"no {'PUBACK' if qos == 1 else 'PUBCOMP'} for mid {info.mid}")
        return info.mid

    def next_message(self, timeout=5.0):
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def wait_disconnect(self, timeout=5.0):
        return self._disconnected.wait(timeout)

    def close(self):
        try:
            self._c.disconnect()
        except Exception:
            pass
        time.sleep(0.1)
        self._c.loop_stop()

    def abort(self):
        """Close the TCP socket without DISCONNECT (abnormal disconnect)."""
        sock = self._c.socket()
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        try:
            self._c.loop_stop()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Capture analysis
# ---------------------------------------------------------------------------

def analyze_file(path):
    env = otp_env()
    erl = shutil.which("erl", path=env["PATH"])
    proc = subprocess.run(
        [erl, "-noshell", "-pa", str(EBIN),
         "-pa", str(ROOT / "_build/default/lib/broker_proto_mqtt/ebin"),
         "-eval", f't15_capture:main(["{path}"])', "-s", "init", "stop"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        timeout=120, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"analyzer failed on {path}: {proc.stdout}")
    events = []
    for line in proc.stdout.splitlines():
        fields = line.split("\t")
        if fields[0] == "FRAME":
            events.append({"kind": "frame", "index": int(fields[1]),
                           "type": fields[2], "roundtrip": fields[3],
                           "hex": fields[4], "info": parse_info(fields[5]),
                           "extra": fields[6:]})
        elif fields[0] == "ERROR":
            events.append({"kind": "error", "reason": fields[1],
                           "hex": fields[2] if len(fields) > 2 else ""})
        elif fields[0] == "PARTIAL":
            events.append({"kind": "partial", "hex": fields[1]})
        elif fields[0] == "DONE":
            events.append({"kind": "done", "frames": int(fields[1])})
    return events


def parse_info(text):
    out = {}
    for part in text.split(";"):
        if not part:
            continue
        key, _, value = part.partition("=")
        out[key] = value
    return out


def analyze_capture(prefix):
    result = {}
    for path in sorted(CAPTURES.glob(f"{prefix}-conn*-*.bin")):
        m = re.match(rf"{re.escape(prefix)}-conn(\d+)-(c2s|s2c)\.bin$", path.name)
        if not m:
            continue
        conn, direction = m.group(1), m.group(2)
        result.setdefault(f"conn{conn}", {})[direction] = analyze_file(path)
    return result


def frames(cap, direction="c2s", ftype=None):
    out = []
    for conn in cap.values():
        for ev in conn.get(direction, []):
            if ev["kind"] == "frame" and (ftype is None or ev["type"] == ftype):
                out.append(ev)
    return out


def events_of(cap, direction, kind):
    out = []
    for conn in cap.values():
        out.extend(ev for ev in conn.get(direction, []) if ev["kind"] == kind)
    return out


def s2c_hexes(cap):
    """Every raw broker->client byte chunk: decoded frames, errors and tails.

    A v5-framed CONNACK is not necessarily a `frame` for the 3.1.1 analyzer,
    so the v5 checks search all three event shapes.
    """
    out = [ev["hex"] for ev in frames(cap, "s2c")]
    out += [ev["hex"] for ev in events_of(cap, "s2c", "error")]
    out += [ev["hex"] for ev in events_of(cap, "s2c", "partial")]
    return out


def assert_ok(cond, message):
    if not cond:
        raise AssertionError(message)


def assert_roundtrip(cap, direction="c2s"):
    bad = [ev for ev in frames(cap, direction) if ev["roundtrip"] != "roundtrip=ok"]
    assert_ok(not bad,
              f"{direction} frames not byte-identical after decode/encode: "
              f"{[(e['type'], e['roundtrip'], e['extra']) for e in bad]}")


def b64text(value):
    return base64.b64decode(value)


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def case_paho311_full():
    c = Client("t15-paho-311", clean=True, keepalive=1)
    sp = c.connect()
    assert_ok(sp is False, "first CONNECT must report session_present=0")
    sub_mid = c.subscribe("t15/paho/#", qos=1)
    pub_mid = c.publish("t15/paho/a", b"hello-311", qos=1)
    msg = c.next_message(10.0)
    assert_ok(msg is not None and msg.payload == b"hello-311",
              f"no loopback delivery: {msg!r}")
    assert_ok(msg.qos == 1, f"delivered qos={msg.qos}")
    # keepalive=1: stay silent long enough for paho to send PINGREQ.
    time.sleep(2.6)
    c.close()
    return {"sub_mid": sub_mid, "pub_mid": pub_mid, "session_present": sp,
            "delivered_qos": msg.qos, "payload": msg.payload.decode()}


def case_paho_v5_connect():
    """S2.5 (ADR-017): a v5 CONNECT is accepted, not refused.

    The S1-era v5 refusal case asserted a `0x87` refusal because the
    codec spoke 3.1.1 only. S2.5 implements the v5 subset, so the same real
    paho v5 client must now see reason `0x00` and a v5-framed CONNACK.
    """
    c = Client("t15-paho-v5", clean=True, protocol=mqtt.MQTTv5)
    sp = c.connect(timeout=5.0)
    reason = c._connect_reason
    code = int(getattr(reason, "value", reason))
    c.close()
    return {"client": "paho-mqtt5", "reason_code": code,
            "reason": str(reason), "session_present": sp}


def case_paho_qos2():
    """QoS2 in both directions with a real client.

    Inbound: paho drives PUBLISH -> PUBREC -> PUBREL -> PUBCOMP by itself, and
    `publish(qos=2, wait=True)' only returns once PUBCOMP arrived, so the case
    fails loudly if any of the four steps is missing. Outbound: the subscriber
    receives the message at QoS2, which makes paho run the receiver half of the
    handshake (PUBREC / PUBREL / PUBCOMP) against us.
    """
    sub = Client("t15-paho-qos2-sub", clean=True)
    sub.connect()
    sub.subscribe("t15/qos2/#", qos=2)
    pub = Client("t15-paho-qos2-pub", clean=True)
    pub.connect()
    # No wait: the subscriber is the one whose side we want to observe, and the
    # publish completes on its own.
    pub_mid = pub.publish("t15/qos2/a", b"exactly-once-hopefully", qos=2, wait=False)
    msg = sub.next_message(15.0)
    assert_ok(msg is not None, "QoS2 message never delivered")
    assert_ok(msg.payload == b"exactly-once-hopefully", f"payload {msg.payload!r}")
    assert_ok(msg.qos == 2, f"delivered at qos={msg.qos}, not 2")
    # paho auto-acks in the loop thread; give it a moment to finish PUBCOMP.
    time.sleep(0.3)
    pub.close()
    sub.close()
    return {"pub_mid": pub_mid, "delivered_qos": msg.qos,
            "payload": msg.payload.decode()}


def case_paho_retain():
    """Retain end to end: store on subscribe, forward with retain=0, clear."""
    topic = "t15/retain/a"
    seed = Client("t15-paho-retain-seed", clean=True)
    seed.connect()
    seed.publish(topic, b"retained-value", qos=1, retain=True)
    seed.close()

    # A subscriber that arrives AFTER the publish must get the stored value
    # with retain=1 -- that is the only way a client can tell a replay from
    # live traffic (§3.3.1.3).
    replay = Client("t15-paho-retain-replay", clean=True)
    replay.connect()
    replay.subscribe("t15/retain/#", qos=1)
    stored = replay.next_message(10.0)
    assert_ok(stored is not None, "retained message not replayed on subscribe")
    assert_ok(stored.payload == b"retained-value", f"stored payload {stored.payload!r}")
    assert_ok(stored.retain is True,
              f"replay must carry retain=1, got retain={stored.retain}")
    replay.close()

    # A live subscriber gets retain=0, even though the publisher set retain=1.
    sub = Client("t15-paho-retain-live", clean=True)
    sub.connect()
    sub.subscribe("t15/retain/#", qos=1)
    live_replay = sub.next_message(10.0)
    assert_ok(live_replay is not None and live_replay.retain is True,
              "live subscriber did not get the retained replay first")
    pub = Client("t15-paho-retain-pub", clean=True)
    pub.connect()
    pub.publish(topic, b"live-value", qos=1, retain=True)
    live = sub.next_message(10.0)
    assert_ok(live is not None and live.payload == b"live-value",
              f"no live delivery: {live!r}")
    assert_ok(live.retain is False,
              f"retain bit leaked to live delivery (retain={live.retain})")
    pub.close()
    sub.close()

    # An empty payload deletes the entry: a fresh subscriber gets nothing.
    clear = Client("t15-paho-retain-clear", clean=True)
    clear.connect()
    clear.publish(topic, b"", qos=1, retain=True)
    clear.close()
    after = Client("t15-paho-retain-after", clean=True)
    after.connect()
    after.subscribe("t15/retain/#", qos=1)
    gone = after.next_message(2.5)
    assert_ok(gone is None,
              f"cleared retained message still replayed: {getattr(gone, 'payload', None)!r}")
    after.close()
    return {"topic": topic, "replay_retain": stored.retain,
            "live_retain": live.retain, "cleared": True}


def case_paho_will():
    """The Will must be published on an abnormal close (S2-C, §18.1)."""
    watcher = Client("t15-paho-will-watch", clean=True)
    watcher.connect()
    watcher.subscribe("t15/will/#", qos=1)
    doomed = Client("t15-paho-will-doomed", clean=True,
                    will=("t15/will/a", b"i-was-here", 1))
    doomed.connect()
    time.sleep(0.5)
    doomed.abort()  # TCP close without DISCONNECT
    will = watcher.next_message(10.0)
    assert_ok(will is not None,
              "Will was NOT published on an abnormal close")
    assert_ok(will.payload == b"i-was-here", f"Will payload {will.payload!r}")
    assert_ok(will.qos == 1, f"Will delivered at qos={will.qos}, not 1")
    watcher.close()
    return {"will_topic": "t15/will/a", "delivered": True,
            "payload": will.payload.decode(), "qos": will.qos}


def case_paho_will_skipped_on_disconnect():
    """A clean DISCONNECT suppresses the Will (§3.14)."""
    watcher = Client("t15-paho-will2-watch", clean=True)
    watcher.connect()
    watcher.subscribe("t15/will2/#", qos=1)
    polite = Client("t15-paho-will2-polite", clean=True,
                    will=("t15/will2/a", b"should-not-appear", 1))
    polite.connect()
    polite.close()  # DISCONNECT
    leaked = watcher.next_message(2.5)
    assert_ok(leaked is None,
              f"Will published despite DISCONNECT: "
              f"{getattr(leaked, 'payload', None)!r}")
    watcher.close()
    return {"will_topic": "t15/will2/a", "delivered": False}


def case_paho_bad_filter():
    c = Client("t15-paho-bad-filter", clean=True)
    c.connect()
    # paho only validates wildcard placement; U+0000 passes the client and is
    # rejected by the broker's Annex A validation.
    try:
        c._c.subscribe("t15/bad/\x00", 1)
    except ValueError as exc:
        raise AssertionError(f"paho refused to send the malformed filter: {exc}")
    assert_ok(c.wait_disconnect(5.0),
              "broker did not close the connection on a malformed filter")
    c.abort()
    return {"filter": "t15/bad/<NUL>"}


def mosq(extra, timeout=30):
    return subprocess.run(
        ["docker", "run", "--rm", "--network", "host", MOSQ_IMAGE, *extra],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)


def case_mosq_pubsub():
    sub = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", MOSQ_IMAGE,
         "mosquitto_sub", "-h", HOST, "-p", str(PROXY_PORT), "-V", "mqttv311",
         "-q", "1", "-i", "t15-mosq-sub", "-t", "t15/mosq/#", "-C", "1", "-W", "15"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.0)  # let SUBSCRIBE land before the publish
    pub = mosq(["mosquitto_pub", "-h", HOST, "-p", str(PROXY_PORT),
                "-V", "mqttv311", "-q", "1", "-i", "t15-mosq-pub",
                "-t", "t15/mosq/a", "-m", "hello-mosq"], timeout=30)
    assert_ok(pub.returncode == 0,
              f"mosquitto_pub failed rc={pub.returncode}: {pub.stderr}")
    out, err = sub.communicate(timeout=30)
    assert_ok(out.strip() == "hello-mosq",
              f"mosquitto_sub got {out!r} (stderr={err!r})")
    return {"payload": out.strip(), "pub_rc": pub.returncode}


def case_mosq_qos2():
    """mosquitto 2.1.2 QoS2 round trip through the tee."""
    sub = subprocess.Popen(
        ["docker", "run", "--rm", "--network", "host", MOSQ_IMAGE,
         "mosquitto_sub", "-h", HOST, "-p", str(PROXY_PORT), "-V", "mqttv311",
         "-q", "2", "-i", "t15-mosq-q2-sub", "-t", "t15/mosq2/#", "-C", "1",
         "-W", "20"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    time.sleep(1.0)  # let SUBSCRIBE land before the publish
    pub = mosq(["mosquitto_pub", "-h", HOST, "-p", str(PROXY_PORT),
                "-V", "mqttv311", "-q", "2", "-i", "t15-mosq-q2-pub",
                "-t", "t15/mosq2/a", "-m", "hello-mosq-qos2"], timeout=40)
    assert_ok(pub.returncode == 0,
              f"mosquitto_pub (qos2) failed rc={pub.returncode}: {pub.stderr}")
    out, err = sub.communicate(timeout=40)
    assert_ok(out.strip() == "hello-mosq-qos2",
              f"mosquitto_sub (qos2) got {out!r} (stderr={err!r})")
    return {"payload": out.strip(), "pub_rc": pub.returncode}


def case_mosq_v5_pubsub():
    """S2.5: mosquitto 2.1.2 speaking MQTT 5.0 can publish to the broker."""
    pub = mosq(["mosquitto_pub", "-h", HOST, "-p", str(PROXY_PORT),
                "-V", "mqttv5", "-i", "t15-mosq-v5",
                "-t", "t15/v5/a", "-m", "x"], timeout=30)
    assert_ok(pub.returncode == 0,
              f"mosquitto MQTT5 publish failed: {pub.stderr}")
    return {"pub_rc": pub.returncode,
            "stderr": (pub.stderr or "").strip().splitlines()[:2]}


CASE_FUNCS = {
    "paho311-full": case_paho311_full,
    "paho-v5-connect": case_paho_v5_connect,
    "paho-qos2": case_paho_qos2,
    "paho-retain": case_paho_retain,
    "paho-will": case_paho_will,
    "paho-will-skipped": case_paho_will_skipped_on_disconnect,
    "paho-bad-filter": case_paho_bad_filter,
    "mosq-pubsub": case_mosq_pubsub,
    "mosq-qos2": case_mosq_qos2,
    "mosq-v5-pubsub": case_mosq_v5_pubsub,
}


# ---------------------------------------------------------------------------
# Capture assertions per case
# ---------------------------------------------------------------------------

def check_paho311(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    conn = next(iter(cap.values()))
    c2s = [e for e in conn["c2s"] if e["kind"] == "frame"]
    s2c = [e for e in conn["s2c"] if e["kind"] == "frame"]
    connect = next(e for e in c2s if e["type"] == "connect")
    assert_ok(connect["info"]["proto_level"] == "4", "CONNECT proto_level != 4")
    assert_ok(connect["info"]["clean"] == "1", "CONNECT clean_session != 1")
    assert_ok(connect["info"]["keepalive"] == "1", "CONNECT keepalive != 1")
    assert_ok(b64text(connect["info"]["client"]) == b"t15-paho-311",
              "CONNECT client_id mismatch")
    assert_ok(connect["info"]["user"] == "1" and connect["info"]["pass"] == "1",
              "CONNECT credentials flags missing")
    sub = next(e for e in c2s if e["type"] == "subscribe")
    assert_ok(sub["info"]["topics"] == base64.b64encode(b"t15/paho/#").decode() + ":1",
              f"SUBSCRIBE topics={sub['info']['topics']}")
    pub = next(e for e in c2s if e["type"] == "publish")
    assert_ok(pub["info"]["qos"] == "1", "PUBLISH qos != 1")
    assert_ok(b64text(pub["info"]["topic"]) == b"t15/paho/a", "PUBLISH topic mismatch")
    assert_ok(b64text(pub["info"]["payload"]) == b"hello-311", "PUBLISH payload mismatch")
    puback = next(e for e in c2s if e["type"] == "puback")
    delivered = [e for e in s2c if e["type"] == "publish"]
    assert_ok(delivered, "no PUBLISH delivered on s2c")
    assert_ok(b64text(delivered[0]["info"]["payload"]) == b"hello-311",
              "delivered payload mismatch")
    acked_id = int(delivered[0]["info"]["packet_id"])
    assert_ok(puback["hex"] == "4002%04x" % acked_id,
              f"client PUBACK bytes {puback['hex']} != 4002{acked_id:04x} "
              f"(delivered packet_id)")
    assert_ok(any(e["type"] == "puback"
                  and e["hex"] == "4002%04x" % evidence["pub_mid"]
                  for e in s2c),
              "broker PUBACK does not carry the client's publish packet id")
    assert_ok(any(e["type"] == "ping" and e["info"].get("kind") == "req"
                  for e in c2s), "no PINGREQ captured")
    assert_ok(any(e["type"] == "disconnect" for e in c2s), "no DISCONNECT captured")

    connack = next(e for e in s2c if e["type"] == "connack")
    assert_ok(connack["hex"] == "20020000", f"CONNACK bytes {connack['hex']}")
    suback = next(e for e in s2c if e["type"] == "suback")
    assert_ok(suback["hex"] == "9003%04x01" % evidence["sub_mid"],
              f"SUBACK bytes {suback['hex']}")
    pingresp = next(e for e in s2c if e["type"] == "ping"
                    and e["info"].get("kind") == "resp")
    assert_ok(pingresp["hex"] == "d000", f"PINGRESP bytes {pingresp['hex']}")


def check_paho_v5(name, evidence, cap):
    assert_ok(evidence["reason_code"] == 0x00,
              f"paho v5 read reason {evidence['reason_code']:#04x}, not 0x00")
    connect = next((e for e in frames(cap, "c2s", "connect")), None)
    assert_ok(connect and connect["info"]["proto_level"] == "5",
              f"CONNECT proto_level {connect['info'] if connect else None} != 5")
    # S2.5 answers a v5 CONNECT with a v5-framed CONNACK (3-byte remaining
    # length, zero properties). The analyzer is a 3.1.1 decoder, so depending
    # on where it stops the bytes may surface as a frame, an error or a
    # partial tail; assert on the raw prefix seen in any of them.
    assert_ok(any(h.startswith("20030000") for h in s2c_hexes(cap)),
              f"no v5 CONNACK success captured: {s2c_hexes(cap)}")


def check_mosq_v5(name, evidence, cap):
    assert_ok(evidence["pub_rc"] == 0,
              f"mosquitto MQTT5 publish failed: {evidence['stderr']}")
    assert_ok(any(h.startswith("20030000") for h in s2c_hexes(cap)),
              f"no v5 CONNACK success captured: {s2c_hexes(cap)}")


def _publishes(cap, direction, topic_prefix):
    return [e for e in frames(cap, direction) if e["type"] == "publish"
            and b64text(e["info"]["topic"]).startswith(topic_prefix)]


def check_paho_qos2(name, evidence, cap):
    # The whole point of the case: a real client's four-step handshake must
    # round-trip byte-for-byte in BOTH directions.
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    c2s_types = [e["type"] for e in frames(cap, "c2s")]
    s2c_types = [e["type"] for e in frames(cap, "s2c")]
    # Inbound half (client -> broker): PUBLISH qos2, then the client's PUBREL.
    inbound = _publishes(cap, "c2s", b"t15/qos2/")
    assert_ok(inbound, "no QoS2 PUBLISH captured from paho")
    assert_ok(inbound[0]["info"]["qos"] == "2",
              f"inbound PUBLISH qos={inbound[0]['info']['qos']}")
    assert_ok("pubrel" in c2s_types,
              f"client never sent PUBREL (c2s={c2s_types})")
    # Broker side of the inbound half.
    assert_ok("pubrec" in s2c_types and "pubcomp" in s2c_types,
              f"broker did not complete the inbound handshake (s2c={s2c_types})")
    # Outbound half (broker -> client): PUBLISH qos2 with a packet id, then the
    # broker's PUBREL once paho's PUBREC arrives.
    outbound = _publishes(cap, "s2c", b"t15/qos2/")
    assert_ok(outbound, "no QoS2 PUBLISH delivered to the subscriber")
    assert_ok(outbound[0]["info"]["qos"] == "2",
              f"delivered qos={outbound[0]['info']['qos']}, not 2")
    assert_ok(outbound[0]["info"]["packet_id"] != "none",
              "QoS2 delivery carried no packet id")
    assert_ok("pubrec" in c2s_types,
              "subscriber never answered the delivery with PUBREC")
    assert_ok("pubrel" in s2c_types,
              f"broker never released the outbound delivery (s2c={s2c_types})")
    # The delivered payload is byte-identical to the published one.
    assert_ok(b64text(outbound[0]["info"]["payload"]) ==
              b64text(inbound[0]["info"]["payload"]),
              "delivered payload differs from the published bytes")


def check_paho_retain(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    inbound = _publishes(cap, "c2s", b"t15/retain/")
    assert_ok(inbound, "no inbound retained PUBLISH captured")
    assert_ok(all(e["info"]["retain"] == "1" for e in inbound),
              "a seeding PUBLISH was not sent with retain=1")
    # Every replayed/forwarded message on the server side must carry retain=0;
    # the replay's retain=1 comes from the broker's own replay path, but a
    # LIVE forward (payload `live-value') must have retain=0.
    forwards = [e for e in _publishes(cap, "s2c", b"t15/retain/")
                if b64text(e["info"]["payload"]) == b"live-value"]
    assert_ok(forwards, "live retained publish was never forwarded")
    assert_ok(forwards[0]["info"]["retain"] == "0",
              f"live forward kept retain={forwards[0]['info']['retain']}")
    # The clear: an empty-payload PUBLISH with retain=1.
    clears = [e for e in inbound if e["info"]["payload_bytes"] == "0"]
    assert_ok(clears, "the empty-payload clearing PUBLISH was not captured")


def check_paho_will(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    connects = [e for e in frames(cap, "c2s") if e["type"] == "connect"]
    assert_ok(any(e["info"]["will"] == "1" for e in connects),
              "will flags never sent by paho")
    wills = [e for e in _publishes(cap, "s2c", b"t15/will/")
             if b64text(e["info"]["payload"]) == b"i-was-here"]
    assert_ok(wills, "Will message was NOT published after the abnormal close")
    # The trigger has to be a crash: that client's own connection must show no
    # DISCONNECT frame, otherwise the case would be asserting the wrong path.
    doomed = [conn for conn in cap.values()
              if any(e["type"] == "connect"
                     and b64text(e["info"].get("client", "")) ==
                     b"t15-paho-will-doomed"
                     for e in conn.get("c2s", []) if e["kind"] == "frame")]
    assert_ok(doomed, "no capture for the doomed client")
    doomed_frames = [e for e in doomed[0]["c2s"] if e["kind"] == "frame"]
    assert_ok(all(e["type"] != "disconnect" for e in doomed_frames),
              "the doomed client sent a DISCONNECT, so this was not a crash")


def check_paho_will_skipped(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    assert_ok(any(e["type"] == "disconnect" for e in frames(cap, "c2s")),
              "no DISCONNECT captured from the polite client")
    leaked = [e for e in _publishes(cap, "s2c", b"t15/will2/")]
    assert_ok(not leaked,
              "Will published despite a clean DISCONNECT")


def check_paho_bad_filter(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    assert_ok(events_of(cap, "c2s", "error"),
              "malformed SUBSCRIBE was not rejected")
    s2c_types = [e["type"] for e in frames(cap, "s2c")]
    assert_ok("suback" not in s2c_types,
              f"broker sent SUBACK for a malformed filter: {s2c_types}")
    assert_ok(all(e["hex"] == "20020000" for e in frames(cap, "s2c")
                  if e["type"] == "connack"),
              "unexpected CONNACK before the malformed filter")


def check_mosq_pubsub(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    assert_ok(any(e["type"] == "subscribe" for e in frames(cap, "c2s")),
              "no SUBSCRIBE from mosquitto_sub")
    pub = next(e for e in frames(cap, "c2s") if e["type"] == "publish")
    assert_ok(pub["info"]["qos"] == "1", f"mosquitto_pub qos {pub['info']['qos']}")
    assert_ok(b64text(pub["info"]["payload"]) == b"hello-mosq",
              "mosquitto PUBLISH payload mismatch")
    assert_ok(any(e["hex"] == "4002%04x" % int(pub["info"]["packet_id"], 16)
                  for e in frames(cap, "s2c") if e["type"] == "puback"),
              "PUBACK bytes do not match the client packet id")
    assert_ok(frames(cap, "s2c", "suback"), "no SUBACK for mosquitto_sub")


def check_mosq_qos2(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    c2s_types = [e["type"] for e in frames(cap, "c2s")]
    s2c_types = [e["type"] for e in frames(cap, "s2c")]
    inbound = _publishes(cap, "c2s", b"t15/mosq2/")
    assert_ok(inbound and inbound[0]["info"]["qos"] == "2",
              f"mosquitto_pub qos={inbound[0]['info']['qos'] if inbound else None}")
    outbound = _publishes(cap, "s2c", b"t15/mosq2/")
    assert_ok(outbound and outbound[0]["info"]["qos"] == "2",
              "no QoS2 delivery captured for mosquitto_sub")
    assert_ok("pubrec" in s2c_types and "pubcomp" in s2c_types,
              f"broker did not answer the inbound QoS2 (s2c={s2c_types})")
    assert_ok("pubrel" in c2s_types and "pubrel" in s2c_types,
              f"missing PUBREL on one side (c2s={c2s_types}, s2c={s2c_types})")


def check_mosq_v5(name, evidence, cap):
    assert_ok(evidence["pub_rc"] == 0,
              f"mosquitto MQTT5 publish failed: {evidence['stderr']}")
    s2c = [e["hex"] for e in events_of(cap, "s2c", "error")]
    s2c += [e["hex"] for e in events_of(cap, "s2c", "partial")]
    assert_ok(any(h.startswith("20030000") for h in s2c),
              f"no v5 CONNACK success captured: {s2c}")


CHECKS = {
    "paho311-full": check_paho311,
    "paho-v5-connect": check_paho_v5,
    "paho-qos2": check_paho_qos2,
    "paho-retain": check_paho_retain,
    "paho-will": check_paho_will,
    "paho-will-skipped": check_paho_will_skipped,
    "paho-bad-filter": check_paho_bad_filter,
    "mosq-pubsub": check_mosq_pubsub,
    "mosq-qos2": check_mosq_qos2,
    "mosq-v5-pubsub": check_mosq_v5,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_case(name):
    print(f"\n===== {name} =====", flush=True)
    CAPTURES.mkdir(parents=True, exist_ok=True)
    prefix = name
    tee = Tee(PROXY_PORT, BROKER_PORT, prefix)
    tee.start()
    started = time.monotonic()
    try:
        evidence = CASE_FUNCS[name]()
        time.sleep(0.4)  # allow the last bytes/DISCONNECT to reach the capture
    finally:
        tee.stop()
    cap = analyze_capture(prefix)
    CHECKS[name](name, evidence, cap)
    elapsed = time.monotonic() - started
    print(f"PASS {name} ({elapsed:.1f}s) {json.dumps(evidence, sort_keys=True)}",
          flush=True)
    return {"case": name, "seconds": round(elapsed, 2), "evidence": evidence,
            "capture": cap}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--case", choices=CASES + ["all"], default="all")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    parser.add_argument("--keep-going", action="store_true")
    args = parser.parse_args(argv)

    cases = CASES if args.case == "all" else [args.case]
    if not args.skip_build:
        ensure_release()
    if not (RELEASE / "bin" / "unified_mq").exists():
        raise SystemExit(f"release missing: {RELEASE} (run without --skip-build)")
    compile_analyzer()

    results, failures = [], []
    start_broker(fresh=True)
    try:
        for name in cases:
            try:
                results.append(run_case(name))
            except Exception as exc:
                failures.append({"case": name, "error": str(exc)})
                print(f"FAIL {name}: {exc}", flush=True)
                logs = broker_logs()
                (RUN / f"broker-{name}.log").write_text(logs)
                if not args.keep_going:
                    raise
    finally:
        if not args.keep_data:
            stop_broker()

    report = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
              "results": results, "failures": failures}
    out = RUN / f"report-{'-'.join(cases)}.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"\nREPORT {out}", flush=True)
    print(f"SUMMARY passed={len(results)} failed={len(failures)}", flush=True)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
