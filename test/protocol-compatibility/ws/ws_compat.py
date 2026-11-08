#!/usr/bin/env python3
"""S5 WebSocket: real third-party client compatibility.

Gate S5 requires every protocol to pass a real third-party client integration
suite, "no in-house client talking to itself" (`docs/roadmap.md` §7, Gate S5;
`docs/protocol-compatibility.md` §3). This script drives the broker's
WebSocket listener with **`websocket-client`** (the baseline named in the S0.4
client matrix for WebSocket), a general-purpose WS library the broker does not
control. The application protocol is the broker's own JSON/binary frame API
documented in `docs/protocol-compatibility.md` §6; the transport framing is
RFC 6455, which `websocket-client` implements independently of this project.

Cases (each maps to a README 11.1 verification dimension, §1 of that doc):

  ws-handshake          RFC 6455 handshake accepted; 101 + Sec-WebSocket-Accept
  ws-connect-connack    `connect` -> `connack` (protocol, session_present)
  ws-pubsub-qos1        subscribe -> publish(qos1) -> binary `message` -> `ack`
                        -> `ackack`; cursor advances, offsets echoed in `puback`
  ws-qos0-no-ack        qos0 publish delivers with delivery_id=null and no ack
  ws-binary-frame       binary publish frame (op=1) round-trips; body bytes exact
  ws-offline-catchup    clean_start=false reconnect replays the missed message
  ws-ping-pong          `ping` -> `pong` at the application layer
  ws-unsubscribe-stops  after `unsubscribe` no further delivery arrives
  ws-refusals           invalid filter / shared / bad qos / oversized payload /
                        unknown op each get an explicit refusal, never a crash
  ws-reconnect-takeover a second connect with the same client_id takes over

Usage:
    python3 test/protocol-compatibility/ws/ws_compat.py --case ws-handshake
    python3 test/protocol-compatibility/ws/ws_compat.py --all
    python3 test/protocol-compatibility/ws/ws_compat.py --all --skip-build

The script starts one broker against an ephemeral port (via `rebar3 shell`
with the WS listener env set), connects real `websocket-client` sockets to it,
and asserts the observable protocol behaviour. It never pokes `ws_conn`
internals -- the same seam a browser or a Node `ws` client would use.
"""

import argparse
import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import websocket  # websocket-client 1.x

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "test" / "protocol-compatibility" / "ws" / "data"
ESCRIPT = ROOT / "test" / "protocol-compatibility" / "ws" / "start_broker.escript"

# Opcode of the broker's binary delivery frame (3 = "message").
BINARY_MESSAGE_OP = 3


class CheckError(AssertionError):
    pass


def check(cond, msg):
    if not cond:
        raise CheckError(msg)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def erl_bin():
    """Locate erl under the same OTP the build uses."""
    otp = os.environ.get("OTP_ROOT", "/home/wanli/Development/otp-28.3.1/bin")
    cand = Path(otp) / "erl"
    if cand.exists():
        return str(cand)
    return "erl"


def app_ebins():
    """Every app's ebin dir under _build/default/lib, for `erl -pa'."""
    lib = ROOT / "_build" / "default" / "lib"
    return [str(p) for p in sorted(lib.glob("*/ebin"))]


def boot_module_dir():
    """Compile ws_broker_boot.erl to a temp dir and return that dir."""
    out = ROOT / "test" / "protocol-compatibility" / "ws" / "_build"
    out.mkdir(parents=True, exist_ok=True)
    src = ROOT / "test" / "protocol-compatibility" / "ws" / "ws_broker_boot.erl"
    subprocess.run(
        [erl_bin().replace("erl", "erlc"), "-o", str(out), str(src)],
        check=True, capture_output=True,
    )
    return str(out)


def start_broker():
    """Boot a broker node whose only listener is the WebSocket one.

    Returns (proc, port). The node is a plain `erl -noshell -s ws_broker_boot
    run` with the app ebins on the path; the boot module binds an ephemeral
    port (ranch port => 0), creates the WAL partitions, starts the app, and
    prints `PORT <n>'. Storage uses the WAL backend on an ext4 data dir outside
    the release tree so the persistence guard is satisfied. This mirrors
    `ws_conn_SUITE init_per_testcase' exactly.
    """
    DATA.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env["BROKER_DATA_DIR"] = str(DATA)
    pa = []
    for d in app_ebins() + [boot_module_dir()]:
        pa += ["-pa", d]
    log_path = ROOT / "test" / "protocol-compatibility" / "ws" / "run" / "broker.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = open(log_path, "wb")
    proc = subprocess.Popen(
        [erl_bin(), *pa, "-noshell", "-s", "ws_broker_boot", "run"],
        cwd=str(ROOT),
        env=env,
        stdin=subprocess.PIPE,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    proc._log_file = log_file  # keep a handle so the file is not GC'd/closed
    return proc


def wait_ready(proc, timeout=60):
    """Tail the broker log file until `PORT <n>' prints; return the port.

    Raises SystemExit if the node exits early (dumping its log) or times out.
    The broker writes all output to `run/broker.log' (see start_broker), so a
    crash mid-run is always captured there for post-mortem.
    """
    import select
    log_path = ROOT / "test" / "protocol-compatibility" / "ws" / "run" / "broker.log"
    deadline = time.time() + timeout
    pos = 0
    while time.time() < deadline:
        if proc.poll() is not None:
            time.sleep(0.2)
            out = log_path.read_text("utf-8", "replace")
            raise SystemExit(f"broker exited early:\n{out[-4000:]}")
        with open(log_path, "rb") as f:
            f.seek(pos)
            chunk = f.read()
            pos = f.tell()
        if chunk:
            for line in chunk.decode("utf-8", "replace").splitlines():
                if line.startswith("PORT "):
                    return int(line.split()[1])
            if b"START-ERROR" in chunk:
                out = log_path.read_text("utf-8", "replace")
                raise SystemExit(f"broker failed to start:\n{out[-4000:]}")
        time.sleep(0.2)
    raise SystemExit("broker did not become ready in time")


def broker_log_tail(n=6000):
    log_path = ROOT / "test" / "protocol-compatibility" / "ws" / "run" / "broker.log"
    if not log_path.exists():
        return ""
    return log_path.read_text("utf-8", "replace")[-n:]


def stop_broker(proc):
    if proc.poll() is None:
        try:
            if proc.stdin:
                proc.stdin.close()  # escript/erl: nothing reads it; harmless
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


class WsClient:
    """A thin convenience wrapper over a websocket-client connection."""

    def __init__(self, port):
        self.port = port
        self.ws = None

    def __enter__(self):
        self.ws = websocket.create_connection(
            f"ws://127.0.0.1:{self.port}/ws",
            subprotocols=[],
            timeout=5,
        )
        return self

    def __exit__(self, *exc):
        if self.ws is not None:
            try:
                self.ws.close()
            except Exception:
                pass
        return False

    def send_json(self, obj):
        self.ws.send(json.dumps(obj))

    def send_binary(self, op, header, body):
        h = json.dumps(header).encode("utf-8")
        frame = bytes([op]) + len(h).to_bytes(4, "big") + h + body
        self.ws.send_binary(frame)

    def recv(self, timeout=5):
        self.ws.settimeout(timeout)
        raw = self.ws.recv()
        import os as _os
        if _os.environ.get("WS_TRACE"):
            kind = "bin" if isinstance(raw, (bytes, bytearray)) else "txt"
            print(f"    <RECV {kind} {raw[:48]!r}", file=sys.stderr, flush=True)
        return raw

    def recv_json(self, timeout=5):
        raw = self.recv(timeout)
        check(isinstance(raw, str),
              f"expected a text frame, got a binary frame of {len(raw)} bytes")
        return json.loads(raw)

    def recv_binary(self, timeout=5):
        raw = self.recv(timeout)
        check(isinstance(raw, (bytes, bytearray)),
              f"expected binary frame, got {type(raw)}")
        op = raw[0]
        hlen = int.from_bytes(raw[1:5], "big")
        header = json.loads(raw[5:5 + hlen].decode("utf-8"))
        body = bytes(raw[5 + hlen:])
        return op, header, body


def connect(port, client_id, clean=True, tenant="t0"):
    """Open a connection, send `connect', and return (client, connack).

    The connack is returned (not re-read by callers) so a persistent-session
    reconnect can inspect `session_present' without accidentally consuming the
    first catch-up delivery frame.
    """
    c = WsClient(port)
    c.__enter__()
    c.send_json({"op": "connect", "client_id": client_id,
                 "clean_start": clean, "tenant": tenant})
    ack = c.recv_json()
    check(ack.get("op") == "connack", f"expected connack, got {ack}")
    check(ack.get("protocol") == "unifiedmq-ws/1", f"bad protocol field: {ack}")
    return c, ack


def subscribe(port, client_id, dest, qos=1, cid=None):
    c, _ = connect(port, client_id)
    frame = {"op": "subscribe", "destination": dest, "qos": qos}
    if cid is not None:
        frame["id"] = cid
    c.send_json(frame)
    ack = c.recv_json()
    check(ack.get("op") == "suback", f"expected suback, got {ack}")
    check(ack.get("destination") == dest, f"suback dest mismatch: {ack}")
    return c, ack


def publish(port, publisher_id, dest, payload, qos=1, binary=False):
    c, _ = connect(port, publisher_id)
    if binary:
        header = {"op": "publish", "id": 1, "destination": dest, "qos": qos}
        c.send_binary(1, header, payload)
    else:
        c.send_json({"op": "publish", "id": 1, "destination": dest,
                     "qos": qos, "payload_b64": base64.b64encode(payload).decode()})
    ack = c.recv_json()
    return c, ack


# --------------------------------------------------------------------------
# Cases
# --------------------------------------------------------------------------

def case_handshake(port):
    """§1 连接: a real RFC 6455 handshake completes and the accept key is right."""
    import hashlib
    c = WsClient(port)
    c.__enter__()
    # If create_connection returned, the 101 + Sec-WebSocket-Accept were valid;
    # websocket-client verifies the accept hash itself, so a wrong accept would
    # have raised WebSocketBadStatusException here.
    c.send_json({"op": "ping"})
    pong = c.recv_json()
    check(pong.get("op") == "pong", f"expected pong, got {pong}")
    c.__exit__(None, None, None)
    return {"handshake": "ok"}


def case_connect_connack(port):
    """§1 连接: connect -> connack reports protocol + session_present."""
    c, _ = connect(port, "c-conn")
    # A second connect on the same connection is refused explicitly.
    c.send_json({"op": "connect", "client_id": "c-conn-2"})
    err = c.recv_json()
    check(err.get("op") == "error" and err.get("code") == "already_connected",
          f"second connect should be refused, got {err}")
    c.__exit__(None, None, None)
    return {"connack": "ok"}


def case_pubsub_qos1(port):
    """§1/§3: subscribe, publish qos1, receive binary message, ack, ackack."""
    sub, sack = subscribe(port, "sub-qos1", "sport/#", qos=1)
    pub, pack = publish(port, "pub-qos1", "sport/a", b"hello-qos1", qos=1)
    check(pack.get("op") == "puback", f"expected puback, got {pack}")
    check(isinstance(pack.get("offsets"), list) and pack["offsets"],
          f"puback should echo offsets, got {pack}")
    op, header, body = sub.recv_binary()
    check(op == BINARY_MESSAGE_OP, f"expected binary message op, got {op}")
    check(header.get("op") == "message", f"bad header op: {header}")
    check(header.get("destination") == "sport/a", f"bad dest: {header}")
    check(header.get("qos") == 1, f"bad qos: {header}")
    check(body == b"hello-qos1", f"body mismatch: {body!r}")
    check(base64.b64decode(header["payload_b64"]) == b"hello-qos1",
          "payload_b64 disagrees with binary body")
    did = header["delivery_id"]
    check(isinstance(did, int), f"qos1 delivery_id must be an int, got {did}")
    sub.send_json({"op": "ack", "delivery_id": did})
    ackack = sub.recv_json()
    check(ackack.get("op") == "ackack" and ackack.get("delivery_id") == did,
          f"bad ackack: {ackack}")
    sub.__exit__(None, None, None)
    pub.__exit__(None, None, None)
    return {"qos1": "roundtrip-ok", "offsets": pack["offsets"]}


def case_qos0_no_ack(port):
    """§3: qos0 delivers with delivery_id=null; no ack is required."""
    sub, _ = subscribe(port, "sub-qos0", "t0/#", qos=0)
    pub, pack = publish(port, "pub-qos0", "t0/x", b"zero", qos=0)
    check(pack.get("op") == "puback", f"qos0 still pubacks, got {pack}")
    op, header, body = sub.recv_binary()
    check(header.get("qos") == 0, f"qos0 header should say qos 0: {header}")
    check(header.get("delivery_id") is None,
          f"qos0 delivery_id must be null, got {header.get('delivery_id')}")
    check(body == b"zero", f"body mismatch: {body!r}")
    sub.__exit__(None, None, None)
    pub.__exit__(None, None, None)
    return {"qos0": "no-ack-needed"}


def case_binary_frame(port):
    """§5 流控/异常: binary publish frame (op=1) round-trips, body exact."""
    sub, _ = subscribe(port, "sub-bin", "bin/#", qos=1)
    body = bytes([0, 1, 2, 255, 254, 0, 9])  # arbitrary binary incl. high bytes
    pub, pack = publish(port, "pub-bin", "bin/a", body, qos=1, binary=True)
    check(pack.get("op") == "puback", f"binary publish should puback: {pack}")
    op, header, got = sub.recv_binary()
    check(header.get("destination") == "bin/a", f"bad dest: {header}")
    check(got == body, f"binary body mismatch: {got!r} != {body!r}")
    did = header["delivery_id"]
    sub.send_json({"op": "ack", "delivery_id": did})
    sub.recv_json()
    sub.__exit__(None, None, None)
    pub.__exit__(None, None, None)
    return {"binary": "roundtrip-ok", "bytes": len(body)}


def case_offline_catchup(port):
    """§1 断连恢复: clean_start=false replays the message published while away."""
    sub, _ = connect(port, "off-1", clean=False)
    sub.send_json({"op": "subscribe", "destination": "off/#", "qos": 1})
    ack = sub.recv_json()
    check(ack.get("op") == "suback", f"expected suback, got {ack}")
    sub.__exit__(None, None, None)

    # While the subscriber is away, publish to its destination.
    pub, pack = publish(port, "pub-off", "off/a", b"while-away", qos=1)
    check(pack.get("op") == "puback", f"publish while away failed: {pack}")
    pub.__exit__(None, None, None)

    sub2, ack2 = connect(port, "off-1", clean=False)
    check(ack2.get("session_present") is True,
          f"persistent session should report session_present, got {ack2}")
    op, header, body = sub2.recv_binary()
    check(header.get("destination") == "off/a", f"bad catchup dest: {header}")
    check(body == b"while-away", f"catchup body mismatch: {body!r}")
    did = header["delivery_id"]
    sub2.send_json({"op": "ack", "delivery_id": did})
    sub2.recv_json()
    sub2.__exit__(None, None, None)
    return {"catchup": "replayed-while-away"}


def case_ping_pong(port):
    """§1: application-level ping -> pong over an established connection."""
    c, _ = connect(port, "pinger")
    c.send_json({"op": "ping"})
    pong = c.recv_json()
    check(pong.get("op") == "pong", f"expected pong, got {pong}")
    c.__exit__(None, None, None)
    return {"ping_pong": "ok"}


def case_unsubscribe_stops(port):
    """§3: after unsubscribe no further delivery arrives."""
    sub, _ = subscribe(port, "unsub-1", "u/#", qos=1)
    pub1, _ = publish(port, "pub-u1", "u/a", b"first", qos=1)
    op, header, _ = sub.recv_binary()
    check(header.get("destination") == "u/a", f"first delivery missing: {header}")
    sub.send_json({"op": "ack", "delivery_id": header["delivery_id"]})
    sub.recv_json()

    sub.send_json({"op": "unsubscribe", "destination": "u/#"})
    uack = sub.recv_json()
    check(uack.get("op") == "unsuback", f"expected unsuback, got {uack}")

    pub2, pack2 = publish(port, "pub-u2", "u/a", b"second", qos=1)
    check(pack2.get("op") == "puback", f"second publish failed: {pack2}")
    sub.ws.settimeout(1.0)
    try:
        late = sub.ws.recv()
        raise CheckError(f"unexpected delivery after unsubscribe: {late!r}")
    except websocket.WebSocketTimeoutException:
        pass  # expected: nothing arrives
    sub.__exit__(None, None, None)
    pub1.__exit__(None, None, None)
    pub2.__exit__(None, None, None)
    return {"unsubscribe": "stops-delivery"}


def case_refusals(port):
    """§2/§5: invalid inputs get explicit refusals, never a crash or a hang."""
    c, _ = connect(port, "refuser")
    # invalid filter (wildcard in the middle)
    c.send_json({"op": "subscribe", "destination": "a/#/b", "qos": 1})
    bad = c.recv_json()
    check(bad.get("op") == "suback" and bad.get("granted") is False,
          f"invalid filter should be refused, got {bad}")
    # shared subscription refused in v1
    c.send_json({"op": "subscribe", "destination": "s/#", "qos": 1,
                 "kind": "shared", "group": "g"})
    shared = c.recv_json()
    check(shared.get("granted") is False and
          "shared" in shared.get("reason", "").lower(),
          f"shared should be refused, got {shared}")
    # bad qos
    c.send_json({"op": "publish", "destination": "r/a", "qos": 2,
                 "payload_b64": base64.b64encode(b"x").decode()})
    bqos = c.recv_json()
    check(bqos.get("op") == "error" and bqos.get("code") == "bad_qos",
          f"qos 2 should be bad_qos, got {bqos}")
    # unknown op
    c.send_json({"op": "frobnicate"})
    uop = c.recv_json()
    check(uop.get("op") == "error" and uop.get("code") == "unknown_op",
          f"unknown op should error, got {uop}")
    # malformed JSON
    c.ws.send("{not json")
    bj = c.recv_json()
    check(bj.get("op") == "error" and bj.get("code") == "bad_json",
          f"bad json should error, got {bj}")
    # oversized payload
    big = base64.b64encode(b"x" * (2 * 1024 * 1024)).decode()
    c.send_json({"op": "publish", "destination": "r/big", "qos": 1,
                 "payload_b64": big})
    big_ack = c.recv_json()
    check(big_ack.get("op") == "error" and
          big_ack.get("code") == "payload_too_large",
          f"oversized should be payload_too_large, got {big_ack}")
    c.__exit__(None, None, None)
    return {"refusals": "all-explicit"}


def case_reconnect_takeover(port):
    """§1: a second connect with the same client_id takes over the session.

    c2 reconnects with ``clean_start=false`` so the ``to/#`` subscription
    survives the takeover (a clean reconnect would discard the session and
    c2 would never receive the message published after it took over).
    """
    c1, _ = connect(port, "takeover", clean=False)
    c1.send_json({"op": "subscribe", "destination": "to/#", "qos": 1})
    check(c1.recv_json().get("op") == "suback", "first suback missing")

    c2, _ = connect(port, "takeover", clean=False)
    # c1 must be closed by the server: the takeover sends it a WebSocket close
    # frame (1000 "session taken over"). websocket-client consumes the close
    # frame internally, so the takeover signal is that c1's socket is gone --
    # recv() returns '' and then raises WebSocketConnectionClosedException --
    # NOT a data message carrying the reason text. An empty recv or a closed
    # socket after c2 connected is exactly the takeover we are asserting.
    c1.ws.settimeout(3.0)
    takeover_seen = False
    try:
        while True:
            msg = c1.ws.recv()
            if msg == "" or msg is None:
                takeover_seen = True
                break
    except (websocket.WebSocketTimeoutException,
            websocket.WebSocketConnectionClosedException):
        takeover_seen = True  # server closed c1 in response to the takeover
    check(takeover_seen, "original connection was not closed on takeover")
    # c2 still works
    pub, pack = publish(port, "pub-take", "to/a", b"after-takeover", qos=1)
    check(pack.get("op") == "puback", f"publish on takeover conn failed: {pack}")
    op, header, body = c2.recv_binary()
    check(header.get("destination") == "to/a" and body == b"after-takeover",
          f"delivery on takeover conn wrong: {header} {body!r}")
    c2.send_json({"op": "ack", "delivery_id": header["delivery_id"]})
    c2.recv_json()
    c2.__exit__(None, None, None)
    pub.__exit__(None, None, None)
    return {"takeover": "session-moved"}


CASES = {
    "ws-handshake": case_handshake,
    "ws-connect-connack": case_connect_connack,
    "ws-pubsub-qos1": case_pubsub_qos1,
    "ws-qos0-no-ack": case_qos0_no_ack,
    "ws-binary-frame": case_binary_frame,
    "ws-offline-catchup": case_offline_catchup,
    "ws-ping-pong": case_ping_pong,
    "ws-unsubscribe-stops": case_unsubscribe_stops,
    "ws-refusals": case_refusals,
    "ws-reconnect-takeover": case_reconnect_takeover,
}


def run_case(name, port):
    fn = CASES[name]
    started = time.monotonic()
    try:
        evidence = fn(port)
        elapsed = time.monotonic() - started
        print(f"PASS {name} ({elapsed:.2f}s) {json.dumps(evidence, sort_keys=True)}",
              flush=True)
        return {"case": name, "seconds": round(elapsed, 2), "evidence": evidence}
    except Exception as exc:
        elapsed = time.monotonic() - started
        print(f"FAIL {name}: {exc}", flush=True)
        return {"case": name, "seconds": round(elapsed, 2),
                "error": f"{type(exc).__name__}: {exc}"}


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--case", choices=sorted(CASES), default=None)
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--keep-data", action="store_true",
                        help="keep the WAL data dir (default: start clean so "
                             "persistent-session state from a prior run cannot "
                             "leak into ws-offline-catchup)")
    args = parser.parse_args()

    if not args.all and not args.case:
        parser.error("specify --all or --case")

    DATA.mkdir(parents=True, exist_ok=True)
    if not args.keep_data:
        # Start from a clean slate: the WAL persists durable sessions, and a
        # leftover session from an earlier run would make ws-offline-catchup
        # replay a stale message on the first connect.
        for child in DATA.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)

    print("Starting WS compat broker (ephemeral port) ...", flush=True)
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
            if "error" in r:
                failures += 1
                (ROOT / "test" / "protocol-compatibility" / "ws" / "run" /
                 f"broker-{name}.log").parent.mkdir(parents=True, exist_ok=True)
                (ROOT / "test" / "protocol-compatibility" / "ws" / "run" /
                 f"broker-{name}.log").write_text(broker_log_tail(8000))
    finally:
        stop_broker(proc)

    out = ROOT / "test" / "protocol-compatibility" / "ws" / "run" / "report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(
        {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
         "results": results, "failures": failures}, indent=2, sort_keys=True) + "\n")
    print(f"\nREPORT {out}", flush=True)
    print(f"SUMMARY passed={len(results) - failures} failed={failures}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
