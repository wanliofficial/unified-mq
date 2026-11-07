# T15 — real-client compatibility (MQTT 3.1.1)

Evidence for the Gate item "真实 MQTT 客户端完成 publish → subscribe → ack 全链路"
(`docs/design/s1-mqtt-vertical-slice.md` §9 T15, `docs/protocol-compatibility.md` §4).

The suite drives the **T14 release** (`_build/t14/rel/unified_mq`) inside an
`erlang:28.3.1.0` container and puts a transparent TCP **tee** between the real
clients and the broker. Both client families are real:

| Client | Version | Where it runs |
|---|---|---|
| paho-mqtt (Python) | 2.x | host |
| mosquitto_pub / mosquitto_sub | 2.1.2 | `eclipse-mosquitto:2`, `--network host` |

Ports: broker `127.0.0.1:18831` (container 1883), tee `127.0.0.1:18832`.
Data (`data/`) lives on the host ext4 and is bind-mounted to
`/var/lib/unified-mq`, so `wal_writer`'s filesystem guard is satisfied for the
same reason as T14.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"

# whole suite (assembles the release if missing, then starts one broker)
python3 test/protocol-compatibility/mqtt/t15_compat.py

# one case, reusing the existing release/container
python3 test/protocol-compatibility/mqtt/t15_compat.py --case paho311-full --skip-build
```

Useful flags: `--keep-data` leaves the broker container running (inspect with
`docker logs unified-mq-t15-broker`), `--keep-going` runs every case even after
a failure. Reports land in `run/report-<cases>.json`; per-connection captures in
`run/captures/<case>-conn<N>-{c2s,s2c}.bin` (`run/` and `data/` are gitignored).

## Byte-level method

`t15_capture.erl` reads a capture file and decodes it with the **production**
`mqtt_frame` module, frame by frame:

```
FRAME <n> <type> roundtrip=ok|mismatch <raw hex> <semantic fields>
ERROR <reason> <remaining hex>
PARTIAL <remaining hex>
DONE <frame count>
```

For every decoded frame the analyzer re-encodes the record and compares the
result with the exact bytes the client sent. `roundtrip=ok` therefore means:
*our encoder reproduces a real client's encoding of a frame we just accepted*.
T8's golden vectors are hand-written, so they cannot catch this class of drift
(design §13 M3); this suite can, and every check asserts `roundtrip=ok` on both
directions.

## Cases → contract

| Case | What it proves |
|---|---|
| `paho311-full` | CONNECT (clean, keepalive=1, credentials), SUBSCRIBE, QoS1 PUBLISH, PUBACK in **both** directions, PINGREQ/PINGRESP (keepalive timer), DISCONNECT; all frames byte-identical; CONNACK `20020000`, SUBACK `9003000101` |
| `paho-v5-refused` | MQTT 5 CONNECT is rejected at parse time; real paho reports reason **135 = 0x87** ("Not authorized") and the broker closes. Wire bytes `2003008700` (v5 framing, zero properties) |
| `paho-qos2` | **S2-C**: full QoS2 both ways. Inbound: paho drives PUBLISH→PUBREC→PUBREL→PUBCOMP and returns only on PUBCOMP. Outbound: the subscriber receives at QoS2, so paho runs the receiver half (PUBREC / PUBREL / PUBCOMP) against us. Every frame byte-identical |
| `paho-retain` | **S2-C**: stored on publish, replayed to a later subscriber with `retain=1`, forwarded live with `retain=0`, and an empty-payload `retain=1` publish clears it (a fresh subscriber then gets nothing) |
| `paho-will` | **S2-C**: Will published after an abnormal TCP close, at its configured QoS, with the configured payload |
| `paho-will-skipped` | **S2-C**: the same Will is **not** published when the client sends DISCONNECT (§3.14) |
| `paho-bad-filter` | a filter containing U+0000 passes paho's own validation, reaches the broker, and is rejected in the codec (`embedded_null`): close with **no SUBACK** |
| `mosq-pubsub` | mosquitto 2.1.2 QoS1 round trip; SUBSCRIBE/SUBACK and PUBLISH/PUBACK bytes match, SUBACK `9003000101` |
| `mosq-qos2` | **S2-C**: mosquitto 2.1.2 QoS2 round trip; PUBREC/PUBREL/PUBCOMP present on both sides of the tee |
| `mosq-v5-refused` | mosquitto MQTT5 reads the same `2003008700` refusal (`Connection error: Not authorized`) |

## Deviations / known limits

* **M13 (T14)** still applies: the Python driver runs on the host so it can use
  paho directly; the broker is in the container. The tee is the only in-path
  observer, so no `tcpdump` is needed (the host has none).
* **Empty client ids are refused** by design (T9: the session table is keyed by
  ClientID). `mosquitto_sub`/`mosquitto_pub` default to an empty id under
  MQTT 3.1.1, hence the explicit `-i` in both commands.
* **MQTT 5 CONNACK framing (M16)**: the refusal is now v5-framed. A
  `20 02 00 87` 3.1.1 CONNACK made paho raise inside its properties decoder and
  mosquitto report a network protocol error instead of the reason code. Fixed in
  `mqtt_conn_fsm`; 3.1.1 responses are unchanged. The reason code stays `0x87`
  per design §4.5, although v5's spec-correct "Unsupported Protocol Version" is
  `0x84` — MQTT 5 is S2 scope.
* **QoS 2 / Retain / Will** were refusal or strip-and-drop postures in S1
  (design §4.5). S2-C replaced them with real support (ADR-018): the old
  `paho-qos2-refused` and `paho-will-ignored` cases were deleted, not skipped —
  their assertions are now the *negative* half of `paho-qos2`/`paho-will`
  (nothing to assert about a refusal that no longer exists).
* **`Client.publish(qos=2)`** waits for PUBCOMP (paho reports it through the
  same `on_publish` callback as PUBACK), so a broken four-step handshake fails
  the case instead of silently returning.
* **QoS2 is not exactly-once** across a broker crash (ADR-012 §12.1): the
  handshake is complete and duplicate-free across a reconnect, but the crash
  case is at-least-once. See `tools/docker/s1_recovery.py` R8.
