# S5 WebSocket — real-client compatibility

Evidence for the Gate S5 item "通过真实第三方客户端集成测试，无自编客户端互测"
(`docs/roadmap.md` §7 Gate S5, `docs/protocol-compatibility.md` §4.5).

The suite drives a **real `erl -noshell -s ws_broker_boot run` broker node** (ranch
临时端口 `port=>0`、WAL 后端、ext4 数据目录) and connects real
**Python `websocket-client` 1.7.0** sockets to it — the WebSocket client named in
the S0.4 client matrix. The application protocol is the broker's own
JSON/binary frame API; the transport framing is standard RFC 6455, which
`websocket-client` implements independently of this project.

| Client | Version | Where it runs |
|---|---|---|
| websocket-client (Python) | 1.7.0 | host |

The script **never pokes `ws_conn` internals** — it uses the same WS handshake
and frame API a browser or a Node `ws` client would use. The boot path
(`ws_broker_boot.erl`) mirrors `ws_conn_SUITE init_per_testcase` exactly: same
env, same ephemeral binding, same `ranch:get_port` readback.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# whole suite (starts one broker on an ephemeral port, runs all 10 cases)
python3 test/protocol-compatibility/ws/ws_compat.py

# one case
python3 test/protocol-compatibility/ws/ws_compat.py --case ws-pubsub-qos1
```

Useful flags: `--keep-data` keeps the WAL data dir between runs (by default the
suite starts from a clean slate so a leftover persistent session from an earlier
run cannot leak into `ws-offline-catchup`). Reports land in `run/report.json`;
the broker's own log (including any crash) in `run/broker.log` (`run/` and
`data/` are gitignored).

## Application frame format

* A **text frame** carries one JSON object with an `op` field.
* A **binary frame** carries `<<Op:8, HeaderLen:32, Header/binary, Payload/binary>>`
  where `Header` is the same JSON object and `Payload` is the raw message body.

Operations: `connect` / `subscribe` / `unsubscribe` / `publish` / `ack` /
`ping` / `disconnect`. Every message is routed through the same
`broker_router` / `broker_queue` (fsync) / `broker_subscription` facades as
MQTT — there is no protocol-specific storage path.

## Cases → contract

| Case | What it proves |
|---|---|
| `ws-handshake` | RFC 6455 handshake completes; `Sec-WebSocket-Accept` validated by websocket-client |
| `ws-connect-connack` | `connect` → `connack` reports `protocol=unifiedmq-ws/1` + `session_present`; a second `connect` on the same connection is refused (`already_connected`) |
| `ws-pubsub-qos1` | subscribe → publish(qos1) → binary `message` → `ack` → `ackack`; `puback` echoes `offsets` |
| `ws-qos0-no-ack` | qos0 delivers with `delivery_id=null`; no ack required |
| `ws-binary-frame` | binary publish frame (op=1) round-trips; body bytes exact |
| `ws-offline-catchup` | `clean_start=false` reconnect replays the message published while away |
| `ws-ping-pong` | application-level `ping` → `pong` |
| `ws-unsubscribe-stops` | after `unsubscribe` no further delivery arrives |
| `ws-refusals` | invalid filter / shared / qos2 / unknown op / bad JSON / oversized payload each get an explicit refusal, never a crash or a hang |
| `ws-reconnect-takeover` | a second connect with the same `client_id` takes over; the old connection is closed, the new one keeps receiving |

## Known limits (WebSocket v1)

* **QoS 0/1 only** — no QoS2 handshake; `qos=2` ⇒ `bad_qos`.
* **shared subscription refused** — `kind=shared` ⇒ `suback` `granted=false`.
* **no retained / will** — those live on the MQTT side; WS v1 does not copy them.
