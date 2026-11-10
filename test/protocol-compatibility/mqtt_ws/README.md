# S8-B MQTT over WebSocket — real-client compatibility

Evidence for the Gate S8 item "通过真实第三方客户端集成测试，无自编客户端互测"
(`docs/roadmap.md` §S8 Gate, `docs/protocol-compatibility.md` §4.13).

The suite drives a **real `erl -noshell -s mqtt_ws_broker_boot run` broker node**
(ranch 临时端口 `port => 0`、WAL 后端、ext4 数据目录) and connects a real
**`paho-mqtt` 2.1.0** client with `transport="websockets"` — the MQTT client named
in the S0.4 client matrix. Everything MQTT crosses WebSocket **binary frames
handled by paho's own RFC 6455 implementation**; no code from this repository
(`ws_frame` / `mqtt_frame`) is used as the client.

| Client | Version | Where it runs |
|---|---|---|
| paho-mqtt (Python) | 2.1.0 | host, `transport="websockets"`, `ws_set_options(path="/mqtt")` |

The boot node starts `broker_proto_mqtt_ws` **without** `broker_proto_mqtt`'s own
TCP listener, so a passing case cannot have accidentally gone over plain TCP.
`mqtt_ws_broker_boot.erl` mirrors `mqtt_ws_conn_SUITE init_per_testcase` exactly:
same env, same ephemeral binding, same `ranch:get_port` readback.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# whole suite (starts one broker on an ephemeral port, runs all 9 cases)
/usr/bin/python3 test/protocol-compatibility/mqtt_ws/mqtt_ws_compat.py --all

# one case
/usr/bin/python3 test/protocol-compatibility/mqtt_ws/mqtt_ws_compat.py --case ws-pubsub-qos1
```

Reports land in `run/report.json`; the broker's own log (including any crash) in
`run/broker.log`, and per-case tails in `run/broker-<case>.log` (`run/`, `data/`
and `_build/` are gitignored). `--keep-data` keeps the WAL data dir between runs
(default is a clean slate so a leftover persistent session cannot leak into a
case).

## Cases → contract

| Case | Client | What it proves |
|---|---|---|
| `ws-connect` | paho-mqtt | CONNECT over WebSocket → CONNACK `reason_code = 0` |
| `ws-pubsub-qos0` | paho-mqtt | QoS0 publish reaches a subscriber, no ack involved |
| `ws-pubsub-qos1` | paho-mqtt | QoS1 round trip; PUBACK on both sides (publish completes) |
| `ws-pubsub-qos2` | paho-mqtt | QoS2 round trip; PUBREC/PUBREL/PUBCOMP on both sides |
| `ws-retain` | paho-mqtt | retained publish is replayed to a subscriber that arrives later, with `retain = 1` |
| `ws-will` | paho-mqtt | a socket closed **without** DISCONNECT publishes the Will |
| `ws-mqtt5` | paho-mqtt | MQTT 5.0 CONNECT/SUBSCRIBE/PUBLISH over the same transport |
| `ws-path` | paho-mqtt | a non-root URL path (`/mqtt`) is accepted |
| `ws-raw-framing` | **raw socket probe** | see below — **not** third-party evidence |

## `ws-raw-framing` is a transport probe, not client evidence

Two properties of the WebSocket transport are invisible through any MQTT client
library, because the library hides the framing:

1. an MQTT packet split across two WebSocket binary frames is reassembled
   (frame/packet alignment is never assumed);
2. a WebSocket **text** frame is a protocol error, answered with close `1003`.

Those are probed here with a hand-written socket (`struct` + `os.urandom` mask),
so the report entry carries `"third_party": false` and `client:
"raw-socket-probe"`. `docs/protocol-compatibility.md` §4.13 quotes it only as a
transport probe; the CT suite (`mqtt_ws_conn_SUITE`) pins the same two
properties independently.

## Known limits

* **No `permessage-deflate`** — the compression extension is out of scope by
  design (`docs/design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md` §2).
  paho never offers it, so this suite cannot observe the negotiation; a client
  that offers the extension gets the plain, uncompressed connection.
* **Subprotocol is mandatory** — a handshake without
  `Sec-WebSocket-Protocol: mqtt` is refused with HTTP 400. paho-mqtt always
  sends it, so that refusal is only covered by `mqtt_ws_conn_SUITE`.
* **wss** is covered by `mqtt_ws_conn_SUITE` (self-signed certificate) and by
  `docs/operations.md`; this harness deliberately runs plain `ws` so a TLS
  configuration mistake cannot mask a transport bug.
