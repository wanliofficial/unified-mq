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
  paho-v5-refused    MQTT 5 CONNECT -> CONNACK 0x87 (20 02 00 87) then close
  paho-qos2-refused  QoS2 PUBLISH -> close, no PUBREC/PUBCOMP
  paho-retain        inbound retain=1 is stored/delivered with retain=0
  paho-will-ignored  Will parsed, never published on abnormal close
  paho-bad-filter    NUL in filter (client accepts it) -> close, no SUBACK
  mosq-pubsub        mosquitto 2.1.2 QoS1 round trip + byte-level compare
  mosq-v5-refused    mosquitto MQTT 5 -> CONNACK 0x87

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

CASES = ["paho311-full", "paho-v5-refused", "paho-qos2-refused", "paho-retain",
         "paho-will-ignored", "paho-bad-filter", "mosq-pubsub", "mosq-v5-refused"]


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
    text, n = re.subn(r'\{data_dir,\s*"[^"]*"\}',
                      '{data_dir, "%s"}' % DATA_IN_CONTAINER, text)
    if n != 2:
        raise RuntimeError(f"sys.config render: data_dir matched {n}, expected 2")
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
        if qos == 1 and wait:
            with self._ack:
                if not self._ack.wait_for(lambda: info.mid in self._pubacked, timeout):
                    raise RuntimeError(f"no PUBACK for mid {info.mid}")
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


def case_paho_v5_refused():
    c = Client("t15-paho-v5", clean=True, protocol=mqtt.MQTTv5)
    sp = c.connect(timeout=5.0)
    reason = c._connect_reason
    code = int(getattr(reason, "value", reason))
    closed = c.wait_disconnect(5.0)
    c.abort()
    return {"client": "paho-mqtt5", "reason_code": code,
            "reason": str(reason), "session_present": sp, "closed": closed}


def case_paho_qos2_refused():
    c = Client("t15-paho-qos2", clean=True)
    c.connect()
    info = c._c.publish("t15/qos2/a", b"must-not-land", qos=2)
    assert_ok(info.rc == mqtt.MQTT_ERR_SUCCESS, f"publish rc={info.rc}")
    assert_ok(c.wait_disconnect(5.0),
              "broker did not close the connection on QoS2 PUBLISH")
    c.abort()
    return {"mid": info.mid}


def case_paho_retain():
    sub = Client("t15-paho-retain-sub", clean=True)
    sub.connect()
    sub.subscribe("t15/retain/#", qos=1)
    pub = Client("t15-paho-retain-pub", clean=True)
    pub.connect()
    pub_mid = pub.publish("t15/retain/a", b"retained?", qos=1, retain=True)
    msg = sub.next_message(10.0)
    assert_ok(msg is not None and msg.payload == b"retained?", f"no delivery: {msg!r}")
    assert_ok(msg.retain is False,
              f"retain bit leaked to delivery (retain={msg.retain})")
    sub.close()
    return {"pub_mid": pub_mid, "delivered_retain": msg.retain}


def case_paho_will_ignored():
    watcher = Client("t15-paho-will-watch", clean=True)
    watcher.connect()
    watcher.subscribe("t15/will/#", qos=1)
    doomed = Client("t15-paho-will-doomed", clean=True,
                    will=("t15/will/a", b"i-was-here", 1))
    doomed.connect()
    time.sleep(0.5)
    doomed.abort()  # abnormal disconnect: a real broker would publish the Will
    leaked = watcher.next_message(2.5)
    assert_ok(leaked is None,
              f"Will was published on abnormal close: "
              f"{getattr(leaked, 'payload', None)!r}")
    watcher.close()
    return {"will_topic": "t15/will/a", "delivered": False}


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


def case_mosq_v5_refused():
    pub = mosq(["mosquitto_pub", "-h", HOST, "-p", str(PROXY_PORT),
                "-V", "mqttv5", "-t", "t15/v5/a", "-m", "x"], timeout=30)
    assert_ok(pub.returncode != 0,
              "mosquitto MQTT5 publish unexpectedly succeeded")
    return {"pub_rc": pub.returncode,
            "stderr": (pub.stderr or "").strip().splitlines()[:2]}


CASE_FUNCS = {
    "paho311-full": case_paho311_full,
    "paho-v5-refused": case_paho_v5_refused,
    "paho-qos2-refused": case_paho_qos2_refused,
    "paho-retain": case_paho_retain,
    "paho-will-ignored": case_paho_will_ignored,
    "paho-bad-filter": case_paho_bad_filter,
    "mosq-pubsub": case_mosq_pubsub,
    "mosq-v5-refused": case_mosq_v5_refused,
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
    assert_ok(evidence["reason_code"] == 0x87,
              f"paho v5 read reason {evidence['reason_code']:#04x}, not 0x87")
    assert_ok(evidence["closed"], "broker did not close after the v5 refusal")
    assert_ok(events_of(cap, "c2s", "error"),
              "MQTT5 CONNECT was not rejected at parse time")
    err = events_of(cap, "c2s", "error")[0]
    assert_ok("unacceptable_protocol_version" in err["reason"],
              f"unexpected parse error {err['reason']}")
    assert_ok(err["hex"].startswith("10"), f"v5 CONNECT bytes {err['hex'][:20]}")
    # The analyzer deliberately cannot decode a v5 CONNACK (the S1 codec speaks
    # 3.1.1 only); the verbatim refusal bytes are the assertion.
    s2c_errs = events_of(cap, "s2c", "error")
    assert_ok(s2c_errs, "no v5 CONNACK captured")
    assert_ok(s2c_errs[0]["hex"] == "2003008700",
              f"v5 CONNACK bytes {s2c_errs[0]['hex']} != 2003008700")
    assert_ok(not frames(cap, "s2c"),
              "decoded an s2c frame, so the v5 CONNACK leaked a 3.1.1 shape")


def check_paho_qos2(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    errs = events_of(cap, "c2s", "error")
    assert_ok(errs, "QoS2 PUBLISH was not rejected at parse time")
    assert_ok("bad_publish_qos" in errs[0]["reason"],
              f"unexpected parse error {errs[0]['reason']}")
    raw = bytes.fromhex(errs[0]["hex"])
    assert_ok(raw[0] == 0x34,
              f"rejected frame fixed header {raw[0]:#04x} != 0x34 (QoS2 PUBLISH)")
    assert_ok(b"t15/qos2/a" in raw and b"must-not-land" in raw,
              f"rejected frame is not the QoS2 payload: {raw!r}")
    s2c_types = {e["type"] for e in frames(cap, "s2c")}
    assert_ok("pubrec" not in s2c_types and "pubcomp" not in s2c_types,
              f"broker acked QoS2: {sorted(s2c_types)}")
    assert_ok("publish" not in s2c_types, "QoS2 message was stored/delivered")


def check_paho_retain(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    inbound = [e for e in frames(cap, "c2s") if e["type"] == "publish"
               and b64text(e["info"]["topic"]) == b"t15/retain/a"]
    assert_ok(inbound and inbound[0]["info"]["retain"] == "1",
              "inbound retain bit not captured")
    outbound = [e for e in frames(cap, "s2c") if e["type"] == "publish"
                and b64text(e["info"]["topic"]) == b"t15/retain/a"]
    assert_ok(outbound and outbound[0]["info"]["retain"] == "0",
              "delivered PUBLISH kept retain=1")


def check_paho_will(name, evidence, cap):
    assert_roundtrip(cap)
    assert_roundtrip(cap, "s2c")
    connects = [e for e in frames(cap, "c2s") if e["type"] == "connect"]
    assert_ok(any(e["info"]["will"] == "1" for e in connects),
              "will flags never sent by paho")
    leaked = [e for e in frames(cap, "s2c") if e["type"] == "publish"
              and b64text(e["info"]["topic"]).startswith(b"t15/will/")]
    assert_ok(not leaked, "Will message was published after abnormal close")


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


def check_mosq_v5(name, evidence, cap):
    s2c_errs = events_of(cap, "s2c", "error")
    assert_ok(s2c_errs and s2c_errs[0]["hex"] == "2003008700",
              f"v5 CONNACK bytes {[e['hex'] for e in s2c_errs]}")
    assert_ok(evidence["pub_rc"] != 0,
              "mosquitto MQTT5 publish unexpectedly succeeded")


CHECKS = {
    "paho311-full": check_paho311,
    "paho-v5-refused": check_paho_v5,
    "paho-qos2-refused": check_paho_qos2,
    "paho-retain": check_paho_retain,
    "paho-will-ignored": check_paho_will,
    "paho-bad-filter": check_paho_bad_filter,
    "mosq-pubsub": check_mosq_pubsub,
    "mosq-v5-refused": check_mosq_v5,
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
