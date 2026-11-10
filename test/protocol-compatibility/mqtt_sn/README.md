# S8-C MQTT-SN — real-client compatibility

Evidence for the Gate S8 item "通过真实第三方客户端集成测试，无自编客户端互测"
(`docs/roadmap.md` §S8 Gate, `docs/protocol-compatibility.md` §4.14).

The suite drives a **real `erl -noshell -s mqtt_sn_broker_boot run` broker node**
(ephemeral UDP port `port => 0`, WAL backend, ext4 data directory) and puts
**`mqtt-sn-tools` 0.0.7** on the other end — a standalone C implementation of
MQTT-SN 1.2 (`njh/mqtt-sn-tools`) that this repository does not control. Our own
`mqtt_sn_codec` is never used as a client on this side of the socket.

| Client | Version | Where it runs |
|---|---|---|
| mqtt-sn-tools (`mqtt-sn-pub`, `mqtt-sn-sub`) | 0.0.7, upstream commit `a1dd870` | host, plain UDP to `127.0.0.1` |

The boot node starts **only** `broker_proto_mqtt_sn`: there is no MQTT TCP
listener, no MQTT-over-WS listener and no broker WS API, so a passing case
cannot have accidentally gone over another protocol. `mqtt_sn_broker_boot.erl`
mirrors `mqtt_sn_conn_SUITE init_per_testcase` exactly — same env values, same
`port => 0` ephemeral binding, same `mqtt_sn_listener:port/1` readback, same
`predefined_topics => #{1 => <<"sn/pre">>}`.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /home/wanli/workspace/unified-mq
rebar3 compile                     # the harness reuses _build/default/lib

# build the C tools once from a checkout at the pinned commit
git clone https://github.com/njh/mqtt-sn-tools
cd mqtt-sn-tools && git checkout a1dd870 && make && cd ..

python3 test/protocol-compatibility/mqtt_sn/mqtt_sn_compat.py \
  --build-tools ./mqtt-sn-tools --all
```

| Flag | Meaning |
|---|---|
| `--all` | every case (the default when `--case` is absent) |
| `--case <name>` | one case, repeatable |
| `--build-tools <src>` | `make -C <src>` and copy the two binaries into `_build/mqtt-sn-tools/bin` |
| `--keep-going` | continue after a failure instead of stopping at the first |
| `--keep-data` | reuse the WAL data dir from a previous run |

Tools are looked up in this order: `--build-tools`, `$MQTT_SN_TOOLS_DIR`,
`_build/mqtt-sn-tools/bin`, `PATH`. Each run starts **one** broker node,
`run/broker.log` holds its output and `run/report.json` the machine-readable
result (`client`, `third_party`, `evidence`, `not_covered`). `data/`, `run/` and
`_build/` are gitignored.

## Cases → contract

| Case | Client | What it proves |
|---|---|---|
| `sn-connect` | `mqtt-sn-pub -q 1` | CONNECT → CONNACK `rc=0`, PUBACK for the uplink publish, DISCONNECT → DISCONNECT |
| `sn-qos0` | `mqtt-sn-sub`/`mqtt-sn-pub` | QoS0 publish reaches a subscriber; no ack is involved on either side |
| `sn-qos1` | same, `-q 1` | uplink PUBACK observed in the publisher's own log, and the subscriber prints the payload only after answering the downlink PUBLISH |
| `sn-register` | `mqtt-sn-pub -q 1` | client REGISTER → REGACK `rc=0` **with a topic id**, then PUBLISH naming only that id |
| `sn-register-downlink` | `mqtt-sn-sub -t 'sn/down/#'` | the gateway REGISTERs the concrete topic (`sn/down/x`) with a wildcard subscriber *before* the PUBLISH that names its id; the client answers REGACK |
| `sn-short-topic` | topic `s1` | a two-character topic travels as itself, with no id allocation |
| `sn-predefined-topic` | `-T 1` | id `1` resolves through the gateway's configured `predefined_topics`; no REGISTER is sent |
| `sn-retain` | `mqtt-sn-pub -r -q 1` | a retained publish is replayed to a subscriber that arrives **after** the publisher is gone |
| `sn-sleep-disconnect` | `mqtt-sn-pub -e 5` | DISCONNECT with a duration is answered with the granted duration (see below) |
| `sn-qos-minus1` | `mqtt-sn-pub -q -1` | QoS −1 publish reaches a subscriber and the tool opens **no** session at all |

### Reading the `-e` (sleep) evidence

MQTT-SN 1.2 §6.14 says a gateway that accepts a sleep request answers
DISCONNECT **with the granted duration**, so that a client which asked for more
than the gateway allows learns the real value. `mqtt-sn-tools` treats any such
field as suspicious and prints:

```
WARN  DISCONNECT warning. Gateway returned duration in disconnect packet: 0x500
```

That warning *is* the evidence: the plain-disconnect case (no `-e`) receives a
two-byte DISCONNECT and prints no warning, so the warning appearing only for
`-e` distinguishes "sleep accepted" from "plain disconnect".

## Known limits — what this client cannot cover

These are quoted in `docs/protocol-compatibility.md` §4.14 rather than hidden,
and each is covered by `mqtt_sn_conn_SUITE` inside the repository:

* **QoS 2** — `mqtt-sn-tools` 0.0.7 has no QoS 2 code path (`-q` accepts
  `0`, `1`, `-1` only). PUBREC/PUBREL/PUBCOMP is covered in both directions by
  `mqtt_sn_conn_SUITE`.
* **Will** — the tool has no Will option and never sends
  WILLTOPIC/WILLMSG/WILLTOPICUPD. Will vs. takeover is covered by
  `mqtt_sn_conn_SUITE`.
* **Sleep wake** — the tool's `PINGREQ` is the bare two-byte packet with no
  client id, so it can never wake a sleeping session by name. The wake path
  (`PINGREQ(client_id)` from a new endpoint → rebind → backlog) is covered by
  `mqtt_sn_conn_SUITE`. Only the sleep *request* half is covered here.
* **DTLS** — out of scope for S8-C (`docs/design/s8-mqtt-sn-mqtt-over-quic-and-mqtt-over-ws.md`
  §7); this harness runs plain UDP, so a TLS configuration mistake cannot mask
  a gateway bug.
* **SEARCHGW/ADVERTISE** — the tool has no discovery packet, so gateway
  discovery is covered by `mqtt_sn_conn_SUITE` only.

## One bug this suite found

The first run failed with `Failed to connect to MQTT-SN gateway` for every case:
`mqtt_sn_codec` decoded CONNECT's Will and CleanSession bits at positions 7 and
6 (the MQTT 3.1.1 CONNECT layout) instead of 3 and 2, which is what MQTT-SN 1.2
§5.4.4 defines. `mqtt-sn-tools` sends `0x04` for a clean session, so the codec
rejected every conforming client with `{reserved_bits, 0x04}`. Fixed in
`mqtt_sn_codec` (decoder and encoder), with two new cases in
`mqtt_sn_codec_SUITE` pinning the real flag byte from the tool.
