# MQTT over QUIC — third-party compatibility run

MQTT over QUIC (OASIS MQTT TC, *MQTT over QUIC — Single Stream Mode*, Committee
Note Draft 3) drives a real broker instance with a **third-party** client:
EMQX's [`emqtt`](https://github.com/emqx/emqtt) 1.16.1. Its `emqtt_quic` module
speaks MQTT over QUIC through [`quicer`](https://github.com/emqx/quic) with ALPN
`mqtt` on one client-initiated bidirectional stream — the same shape this
broker implements. No code from this repository runs in the client.

The CT suite (`apps/broker_proto_mqtt_quic/test/`) proves the transport's
contract and the listener's refusals; this directory proves that an independent
client can actually use the thing.

## Prerequisites

- `erl` / `rebar3` on `PATH` (override the OTP bin dir with `OTP_BIN=...`).
- `openssl` on `PATH` — QUIC has no cleartext mode, so the run needs a
  certificate; one is generated per checkout into `data/tls/`.
- A C toolchain with `cmake`/`ninja` for the first `quicer` build
  (`msquic` + `quictls`). The first run takes roughly 10–15 minutes and then
  caches in `_build/`.

## Run

```sh
./run.sh                        # all cases
COMPAT_CASES=connect,qos1 ./run.sh
```

The script compiles the broker with `rebar3 as quic`, builds the client project
under `client/` (into the repo-level `_build/quic_client/`), boots a node whose
**only** listener is QUIC on an ephemeral UDP port, reads the port back from the
node, and runs the cases against it. No TCP MQTT listener and no WebSocket
listener exist on that node, so a passing case cannot have gone over another
protocol.

## Cases

| Case | What it proves |
|---|---|
| `connect` | TLS 1.3 handshake with ALPN `mqtt` + a live MQTT session (PINGREQ/PINGRESP) on the first stream |
| `qos0` | down and up at QoS 0 — no acknowledgements, so a receive means the delivery path worked |
| `qos1` | PUBACK on the publisher's stream, `qos => 1` delivery on the subscriber's |
| `qos2` | PUBREC/PUBREL/PUBCOMP with exactly one delivery |
| `retain` | retained state is broker state: the publisher is gone when the subscriber subscribes, and the delivery carries `retain => true` |
| `will` | the client disappears without a DISCONNECT; the Will is published |
| `mqtt5` | MQTT 5.0 properties survive the round trip on the same single stream |
| `reconnect` | a persistent session (`clean_start => false`, same client id) is resumed by a second connection — the message arrives without a re-subscribe |

## What this run does *not* cover, and why

- **0-RTT early data** — optional in the CN draft; not implemented here.
- **Multi-stream mode** — an EMQX private extension; not implemented here.
- **Closing one stream and CONNECTing on another stream of the *same* QUIC
  connection** — `emqtt`'s public API has no stream-scoped close (its `close/1`
  shuts the stream down *and* closes the connection). That half of "stream close
  is not connection close" is asserted in
  `apps/broker_proto_mqtt_quic/test/mqtt_quic_listener_SUITE.erl`
  (`a_new_stream_on_the_same_connection_is_a_new_session`), where the client
  side is raw `quicer` and the MQTT side is this broker's own engine.
- **QoS 2 state across a reconnect** — `emqtt`'s QUIC path carries the same
  engine-level guarantees as TCP, but this harness only subscribes after the
  reconnect, so it does not claim more than it checks.

`emqtt`'s QUIC support is itself an experimental EMQX feature (the client asks
for `peer_bidi_stream_count => 3`, `active => 1` and sends asynchronously), so
the honest claim is: *an independent client's experimental QUIC path works
against this broker for the eight cases above* — not "MQTT over QUIC is
interoperable with every client".

## Files

| File | Role |
|---|---|
| `run.sh` | builds both sides, generates the certificate, boots the broker, runs the cases, tears down |
| `mqtt_quic_broker_boot.erl` | the broker node: QUIC listener on port 0, prints `PORT <n>`, then blocks |
| `mqtt_quic_compat.erl` | the cases, driven through `emqtt` |
| `client/rebar.config` | the client project: `emqtt` 1.16.1 + `quicer` 0.4.12, with `base_dir` pointing at the repo-level `_build/quic_client` |
| `data/`, `_build/` | generated per checkout, git-ignored |

The client's dependency tree lives at `<repo>/_build/quic_client/`, not under
`client/`, and that is deliberate: `rebar3 ct` copies the whole top-level
`test/` tree into `_build/*/extras` and compiles every `.erl` there, so a
vendored `quicer` checkout under `test/` would abort every CT run of the
project. Keep `client/` source-only.
