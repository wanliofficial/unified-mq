#!/usr/bin/env python3
"""S8-B MQTT over WebSocket: real third-party client compatibility.

Gate S8 requires every new protocol to pass a real third-party client
integration suite, "no in-house client talking to itself" (`docs/roadmap.md`
§7); the same rule the S5 protocols satisfy. This script drives the MQTT-over-WS
listener with **`paho-mqtt` 2.1.0** (`transport="websockets"`), a general MQTT
library the broker does not control. Everything MQTT crosses WebSocket binary
frames handled by the library's own RFC 6455 implementation; our own
`ws_frame`/`mqtt_frame` code is never used as a client.

Cases (each maps to one observable protocol contract):

  ws-connect        CONNECT -> CONNACK rc=0 over wss-less WebSocket
  ws-pubsub-qos0    QoS0 publish reaches a subscriber, no ack involved
  ws-pubsub-qos1    QoS1 round trip: PUBACK on both sides
  ws-pubsub-qos2    QoS2 round trip: PUBREC/PUBREL/PUBCOMP on both sides
  ws-retain         retained publish is replayed to a later subscriber
  ws-will           abrupt socket close publishes the Will
  ws-mqtt5          MQTT 5.0 CONNECT/PUBLISH/SUBSCRIBE over the same transport
  ws-path           a non-root URL path (`/mqtt`) is accepted, as the release
                    configures it

`ws-raw-framing` is **not** third-party evidence and is marked as such in its
report entry: it is a hand-rolled socket probe for two transport properties no
MQTT client library exposes (a packet split across two frames; a text frame
answered with close 1003). It lives here because it needs the same broker, and
`docs/protocol-compatibility.md` quotes it only as a transport probe.

Usage:
    python3 test/protocol-compatibility/mqtt_ws/mqtt_ws_compat.py --all
    python3 test/protocol-compatibility/mqtt_ws/mqtt_ws_compat.py --case ws-pubsub-qos1

The script starts one broker on an ephemeral port (`mqtt_ws_broker_boot.erl`,
same boot path as `mqtt_ws_conn_SUITE`), runs the cases, and writes
`run/report.json`. `run/` and `data/` are gitignored.
"""

import argparse
import base64
import json
import os
import queue
import shutil
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DATA = HERE / "data"
RUN = HERE / "run"
BUILD = HERE / "_build"

TOPIC_ROOT = "compat/ws"


class CheckError(AssertionError):
    pass


def check(cond, msg):
    if not cond:
        raise CheckError(msg)


def rc_value(rc):
    """paho 2.x hands a ReasonCode to `on_connect`; 3.1.1 sometimes an int."""
    return getattr(rc, "value", rc)


def erl_bin(name="erl"):
    otp = os.environ.get("OTP_ROOT", "/home/wanli/Development/otp-28.3.1/bin")
    cand = Path(otp) / name
    return str(cand) if cand.exists() else name


def app_ebins():
    lib = ROOT / "_build" / "default" / "lib"
    return [str(p) for p in sorted(lib.glob("*/ebin"))]


def boot_module_dir():
    BUILD.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [erl_bin("erlc"), "-o", str(BUILD), str(HERE / "mqtt_ws_broker_boot.erl")],
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
        [erl_bin(), *pa, "-noshell", "-s", "mqtt_ws_broker_boot", "run"],
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
            raise SystemExit("broker exited early:\n" + log_path.read_text("utf-8", "replace")[-4000:])
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
                raise SystemExit("broker failed to start:\n" + log_path.read_text("utf-8", "replace")[-4000:])
        time.sleep(0.2)
    raise SystemExit("broker did not become ready in time")


def broker_log_tail(n=8000):
    log_path = RUN / "broker.log"
    return log_path.read_text("utf-8", "replace")[-n:] if log_path.exists() else ""


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


class Client:
    """A paho client over WebSocket, with event waits instead of sleeps."""

    def __init__(self, port, client_id, protocol=mqtt.MQTTv311,
                 clean_session=True, will=None, path="/mqtt",
                 keepalive=30):
        self.port = port
        self.messages = queue.Queue()
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.connect_rc = None
        self.subscribed = queue.Queue()
        kwargs = {"client_id": client_id, "protocol": protocol,
                  "transport": "websockets"}
        if protocol != mqtt.MQTTv5:
            kwargs["clean_session"] = clean_session
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2, **kwargs)
        self.client.ws_set_options(path=path)
        if will is not None:
            self.client.will_set(*will)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_subscribe = self._on_subscribe
        self.protocol = protocol
        self.keepalive = keepalive

    # -- callbacks ---------------------------------------------------------
    def _on_connect(self, _client, _userdata, _flags, reason_code, _props=None):
        self.connect_rc = rc_value(reason_code)
        self.connected.set()

    def _on_disconnect(self, _client, _userdata, _flags, reason_code, _props=None):
        self.disconnected.set()

    def _on_message(self, _client, _userdata, msg):
        self.messages.put(msg)

    def _on_subscribe(self, _client, _userdata, mid, reason_codes, _props=None):
        self.subscribed.put((mid, [rc_value(r) for r in reason_codes]))

    # -- lifecycle ---------------------------------------------------------
    def connect(self, timeout=15):
        kwargs = {"clean_start": True} if self.protocol == mqtt.MQTTv5 else {}
        self.client.connect("127.0.0.1", self.port, keepalive=self.keepalive, **kwargs)
        self.client.loop_start()
        check(self.connected.wait(timeout), f"{self.client._client_id!r}: no CONNACK")
        check(self.connect_rc == 0, f"CONNACK reason {self.connect_rc}")
        return self

    def subscribe(self, topic, qos=0, timeout=15):
        rc, mid = self.client.subscribe(topic, qos)
        check(rc == mqtt.MQTT_ERR_SUCCESS, f"subscribe refused locally: {rc}")
        mid2, codes = self.subscribed.get(timeout=timeout)
        check(mid2 == mid, f"SUBACK mid {mid2} != {mid}")
        check(all(c < 0x80 for c in codes), f"SUBACK refused: {codes}")
        return codes

    def publish(self, topic, payload, qos=0, retain=False, timeout=15):
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        check(info.rc == mqtt.MQTT_ERR_SUCCESS, f"publish refused locally: {info.rc}")
        if qos > 0:
            # `wait_for_publish` returns None (it only raises on some errors);
            # `is_published()` is the actual answer.
            info.wait_for_publish(timeout=timeout)
            check(info.is_published(), f"QoS{qos} publish did not complete")
        return info

    def next_message(self, timeout=15):
        try:
            return self.messages.get(timeout=timeout)
        except queue.Empty:
            raise CheckError("no message arrived")

    def close(self):
        try:
            self.client.disconnect()
        except Exception:
            pass
        self.client.loop_stop()

    def abort(self):
        """Close the socket WITHOUT a DISCONNECT -- the Will case's trigger."""
        sock = None
        for getter in ("socket", "_sock_close"):
            try:
                sock = getattr(self.client, getter)
            except AttributeError:
                continue
            break
        try:
            if callable(sock):
                if getattr(sock, "__name__", "") == "_sock_close":
                    sock()
                else:
                    s = sock()
                    s.close()
            elif sock is not None:
                sock.close()
        except Exception:
            pass
        try:
            self.client.loop_stop()
        except Exception:
            pass


#%% Cases -------------------------------------------------------------------

def case_connect(port):
    c = Client(port, "compat-connect").connect()
    c.close()
    return {"connack_reason": 0}


def case_pubsub_qos0(port):
    sub = Client(port, "compat-q0-sub").connect()
    pub = Client(port, "compat-q0-pub").connect()
    topic = f"{TOPIC_ROOT}/q0"
    sub.subscribe(topic, 0)
    pub.publish(topic, b"qos0-payload", qos=0)
    msg = sub.next_message()
    check(msg.topic == topic, f"topic {msg.topic}")
    check(msg.payload == b"qos0-payload", f"payload {msg.payload!r}")
    check(msg.qos == 0, f"qos {msg.qos}")
    sub.close()
    pub.close()
    return {"topic": topic, "payload": "qos0-payload", "qos": 0}


def case_pubsub_qos1(port):
    sub = Client(port, "compat-q1-sub").connect()
    pub = Client(port, "compat-q1-pub").connect()
    topic = f"{TOPIC_ROOT}/q1"
    sub.subscribe(topic, 1)
    pub.publish(topic, b"qos1-payload", qos=1)
    msg = sub.next_message()
    check(msg.payload == b"qos1-payload", f"payload {msg.payload!r}")
    check(msg.qos == 1, f"qos {msg.qos}")
    check(msg.mid is not None, "QoS1 delivery without a packet id")
    sub.close()
    pub.close()
    return {"topic": topic, "payload": "qos1-payload", "qos": 1, "mid": msg.mid}


def case_pubsub_qos2(port):
    sub = Client(port, "compat-q2-sub").connect()
    pub = Client(port, "compat-q2-pub").connect()
    topic = f"{TOPIC_ROOT}/q2"
    sub.subscribe(topic, 2)
    info = pub.publish(topic, b"qos2-payload", qos=2)
    check(info.is_published(), "QoS2 publish did not finish (no PUBCOMP)")
    msg = sub.next_message()
    check(msg.payload == b"qos2-payload", f"payload {msg.payload!r}")
    check(msg.qos == 2, f"qos {msg.qos}")
    sub.close()
    pub.close()
    return {"topic": topic, "payload": "qos2-payload", "qos": 2}


def case_retain(port):
    pub = Client(port, "compat-retain-pub").connect()
    topic = f"{TOPIC_ROOT}/retained"
    pub.publish(topic, b"kept", qos=1, retain=True)
    pub.close()
    sub = Client(port, "compat-retain-sub").connect()
    sub.subscribe(topic, 1)
    msg = sub.next_message()
    check(msg.payload == b"kept", f"payload {msg.payload!r}")
    check(msg.retain, "retained delivery without the retain flag")
    sub.close()
    return {"topic": topic, "payload": "kept", "retain": True}


def case_will(port):
    watcher = Client(port, "compat-will-watch").connect()
    topic = f"{TOPIC_ROOT}/will"
    watcher.subscribe(topic, 0)
    victim = Client(port, "compat-will-victim",
                    will=(topic, b"gone", 0, False)).connect()
    victim.abort()
    msg = watcher.next_message(timeout=20)
    check(msg.topic == topic, f"will topic {msg.topic}")
    check(msg.payload == b"gone", f"will payload {msg.payload!r}")
    watcher.close()
    return {"topic": topic, "payload": "gone", "trigger": "socket closed without DISCONNECT"}


def case_mqtt5(port):
    sub = Client(port, "compat-v5-sub", protocol=mqtt.MQTTv5).connect()
    pub = Client(port, "compat-v5-pub", protocol=mqtt.MQTTv5).connect()
    topic = f"{TOPIC_ROOT}/v5"
    sub.subscribe(topic, 1)
    pub.publish(topic, b"v5-payload", qos=1)
    msg = sub.next_message()
    check(msg.payload == b"v5-payload", f"payload {msg.payload!r}")
    sub.close()
    pub.close()
    return {"protocol": "MQTTv5", "topic": topic, "payload": "v5-payload"}


def case_path(port):
    # The release config puts the listener on 8083 with no path requirement;
    # paho hands the path to the HTTP upgrade request.
    c = Client(port, "compat-path", path="/mqtt").connect()
    c.close()
    return {"path": "/mqtt"}


# -- transport probe (NOT third-party evidence) -----------------------------

def _ws_connect(port, path=b"/mqtt"):
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    key = base64.b64encode(os.urandom(16))
    request = (b"GET " + path + b" HTTP/1.1\r\nHost: 127.0.0.1\r\n"
               b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
               b"Sec-WebSocket-Key: " + key + b"\r\n"
               b"Sec-WebSocket-Version: 13\r\n"
               b"Sec-WebSocket-Protocol: mqtt\r\n\r\n")
    s.sendall(request)
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = s.recv(4096)
        check(chunk, "server closed during the opening handshake")
        response += chunk
    head, rest = response.split(b"\r\n\r\n", 1)
    check(head.startswith(b"HTTP/1.1 101"), f"no upgrade: {head[:80]!r}")
    check(b"sec-websocket-protocol: mqtt" in head.lower(), f"no mqtt subprotocol: {head[:200]!r}")
    return s, rest


def _frame(opcode, payload):
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", 0x80 | opcode, 0x80 | length)
    elif length < 65536:
        header = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, length)
    else:
        header = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, length)
    return header + mask + masked


def _read_frame(s, buf=b""):
    def need(n):
        nonlocal buf
        while len(buf) < n:
            chunk = s.recv(4096)
            check(chunk, "server closed mid-frame")
            buf += chunk
        out, buf = buf[:n], buf[n:]
        return out

    b0, b1 = need(2)
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", need(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", need(8))[0]
    check(not (b1 & 0x80), "server-to-client frames must not be masked")
    return b0 & 0x0F, need(length)


def _connect_packet(client_id):
    # MQTT 3.1.1 CONNECT, clean session, no credentials.
    var = b"\x00\x04MQTT\x04\x02\x00\x3c"
    payload = struct.pack("!H", len(client_id)) + client_id
    body = var + payload
    return b"\x10" + bytes([len(body)]) + body


def _close_code(payload):
    return struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else None


def case_raw_framing(port):
    # 1. One MQTT packet split across two WebSocket frames.
    s, _rest = _ws_connect(port)
    packet = _connect_packet(b"raw-split")
    half = len(packet) // 2
    s.sendall(_frame(0x2, packet[:half]))
    s.sendall(_frame(0x2, packet[half:]))
    opcode, payload = _read_frame(s)
    check(opcode == 0x2, f"expected a binary frame, got opcode {opcode}")
    check(payload[:4] == b"\x20\x02\x00\x00", f"unexpected CONNACK bytes {payload[:4]!r}")
    s.close()

    # 2. A text frame is a protocol error: close 1003.
    s, _rest = _ws_connect(port)
    s.sendall(_frame(0x1, b"MQTT"))
    opcode, payload = _read_frame(s)
    check(opcode == 0x8, f"expected a close frame, got opcode {opcode}")
    check(_close_code(payload) == 1003, f"close code {_close_code(payload)} != 1003")
    s.close()
    return {"third_party": False,
            "probe": ["packet split across two binary frames -> CONNACK",
                      "text frame -> close 1003"]}


CASES = {
    "ws-connect": case_connect,
    "ws-pubsub-qos0": case_pubsub_qos0,
    "ws-pubsub-qos1": case_pubsub_qos1,
    "ws-pubsub-qos2": case_pubsub_qos2,
    "ws-retain": case_retain,
    "ws-will": case_will,
    "ws-mqtt5": case_mqtt5,
    "ws-path": case_path,
    "ws-raw-framing": case_raw_framing,
}


def run_case(name, port):
    started = time.monotonic()
    try:
        evidence = CASES[name](port)
        elapsed = time.monotonic() - started
        print(f"PASS {name} ({elapsed:.2f}s) {json.dumps(evidence, sort_keys=True)}",
              flush=True)
        return {"case": name, "client": "paho-mqtt"
                if name != "ws-raw-framing" else "raw-socket-probe",
                "seconds": round(elapsed, 2), "evidence": evidence}
    except Exception as exc:
        elapsed = time.monotonic() - started
        print(f"FAIL {name}: {exc}", flush=True)
        (RUN / f"broker-{name}.log").write_text(broker_log_tail(), "utf-8")
        return {"case": name, "seconds": round(elapsed, 2),
                "error": f"{type(exc).__name__}: {exc}"}


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--case", choices=sorted(CASES), default=None)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--keep-data", action="store_true",
                        help="keep the WAL data dir (default: start clean, so a "
                             "persistent session from a prior run cannot leak in)")
    args = parser.parse_args()
    if not args.all and not args.case:
        parser.error("specify --all or --case")

    DATA.mkdir(parents=True, exist_ok=True)
    RUN.mkdir(parents=True, exist_ok=True)
    if not args.keep_data:
        for child in DATA.iterdir():
            shutil.rmtree(child, ignore_errors=True) if child.is_dir() else child.unlink()

    print("Starting MQTT-over-WS compat broker (ephemeral port) ...", flush=True)
    proc = start_broker()
    results = []
    failures = 0
    try:
        port = wait_ready(proc)
        print(f"Broker ready on 127.0.0.1:{port}.", flush=True)
        names = sorted(CASES) if args.all else [args.case]
        for name in names:
            r = run_case(name, port)
            results.append(r)
            failures += 1 if "error" in r else 0
    finally:
        stop_broker(proc)

    third_party = [r for r in results if r.get("client") == "paho-mqtt"]
    report = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "client": "paho-mqtt 2.1.0 (transport=websockets)",
        "third_party_cases": len(third_party),
        "transport_probes": len(results) - len(third_party),
        "results": results,
        "failures": failures,
    }
    out = RUN / "report.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"\nREPORT {out}", flush=True)
    print(f"SUMMARY passed={len(results) - failures} failed={failures} "
          f"third_party={len(third_party)}/{len(results)}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
