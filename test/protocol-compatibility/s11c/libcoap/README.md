# S11-C CoAP over DTLS — real-client compatibility

Evidence for the Gate S11 item "通过真实第三方客户端集成测试，无自编客户端互测"
(`docs/roadmap.md` §13.2, `docs/protocol-compatibility.md` §4.24).

The suite drives a **real `erl -noshell -s coap_dtls_broker_boot run` broker node**
(ephemeral UDP port `port => 0`, DTLS-PSK listener, WAL backend, ext4 data
directory) and puts **libcoap `coap-client` v4.3.5** on the other end — a
standalone C implementation of CoAP (`obgm/libcoap`) that this repository does
not control. Our own `coap_codec` is never used as a client on this side of the
socket.

| Client | Version | Where it runs |
|---|---|---|
| libcoap `coap-client` | v4.3.5, upstream tag `v4.3.5` | host, DTLS-PSK to `127.0.0.1` |

The boot node starts **only** `broker_proto_coap` with `transport => dtls`: there
is no plain UDP listener, no MQTT TCP listener and no broker HTTP API, so a
passing case cannot have accidentally gone over another protocol.
`coap_dtls_broker_boot.erl` mirrors `coap_dtls_SUITE init_per_testcase` exactly —
same env values, same `port => 0` ephemeral binding, same `coap_listener:port/1`
readback.

## The key is derived, not passed

libcoap's PSK mode takes raw bytes (`-k 0x<hex>`). The boot node does **not**
configure a PSK: it configures a password and derives the key with the shipped
`broker_security:psk_for_identity/1` (`sha256("unifiedmq-coaps-psk" ‖ 0 ‖
identity ‖ 0 ‖ realm ‖ 0 ‖ password)`), then prints the hex once. The script
hands those exact bytes to `coap-client`. So the key path under test is the real
KDF a device author must reimplement, not a test shortcut.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
cd /home/wanli/workspace/unified-mq
rebar3 compile                     # the harness reuses _build/default/lib

# build libcoap's coap-client once from a checkout at the pinned tag
git clone --depth 1 --branch v4.3.5 https://github.com/obgm/libcoap
cmake -S libcoap -B libcoap/build -DENABLE_DTLS=ON -DENABLE_TCP=OFF \
      -DENABLE_DOCS=OFF -DENABLE_EXAMPLES=ON -DENABLE_TESTS=OFF \
      -DBUILD_SHARED_LIBS=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build libcoap/build --target coap-client -j"$(nproc)"

python3 test/protocol-compatibility/s11c/libcoap/libcoap_compat.py \
  --build-tools ./libcoap --all
```

| Flag | Meaning |
|---|---|
| `--all` | every case (the default when `--case` is absent) |
| `--case <name>` | one case, repeatable |
| `--build-tools <src>` | configure + build `coap-client` from a libcoap source tree into `_build/libcoap/bin` |
| `--keep-going` | continue after a failure instead of stopping at the first |
| `--keep-data` | reuse the WAL data dir from a previous run |

`coap-client` is looked up in this order: `--build-tools`, `$COAP_CLIENT`,
`_build/libcoap/bin`, `PATH`. Each run starts **one** broker node, `run/broker.log`
holds its output and `run/report.json` the machine-readable result (`client`,
`build`, `transport`, `cases`, `not_covered`). `data/`, `run/` and `_build/` are
gitignored.

## Cases → contract

| Case | Client | What it proves |
|---|---|---|
| `coaps-well-known` | `coap-client -m get` | DTLS-PSK handshake completes and `GET /.well-known/core` returns 2.05 link-format |
| `coaps-put-get` | `-m put` / `-m get` | a small body round-trips byte-exact |
| `coaps-con-non` | `-N` / default | CON and NON GETs are both answered on the same session |
| `coaps-block1-upload` | `-f file -b 256/512/1024` | a 1000-byte body sent block-wise is reassembled and read back byte-exact (RFC 7959 Block1) |
| `coaps-delete` | `-m delete` | DELETE clears a retained resource; the next GET is 4.04 with an empty body |
| `coaps-wrong-psk` | `-k 0x00…` | a wrong PSK never produces a response |
| `coaps-unknown-identity` | `-u nobody` | an unknown identity is refused the same way as a wrong PSK |
| `coaps-plaintext-rejected` | `coap://` | a plain datagram to the DTLS port is dropped |

## What this client cannot cover — and why

These are quoted in `docs/protocol-compatibility.md` §4.24.4 rather than hidden:

* **Block2 (block-wise responses)** — the server does not implement it. A
  response larger than libcoap's receive PDU is dropped by the client
  (`coap_pdu_resize: pdu too big`), so a retained payload above ~1.1 KB is
  stored but not retrievable. `coaps-block1-upload` therefore uses a 1000-byte
  body: below the response ceiling, above several blocks.
* **Observe notifications** — `coap-client -s` can subscribe, but the S11-C gate
  is the DTLS transport and the request path; Observe is covered by
  `coap_observe_SUITE` with a byte-level client.
* **Certificates / mTLS** — libcoap built here supports PSK and PKI, but this
  broker only offers PSK over DTLS (ADR-031 §D5).

## Two Block1 bugs this suite found

The first Block1 run failed differently at each SZX — `4.08` at 256/512, and a
**2.04 that stored nothing** (GET returned `4.04`) at 1024. Both were real
implementation defects that the in-repo byte-level client had hidden, because
that client keeps the token fixed and expects the *next* block number:

1. **`2.31 Continue` echoed `Num + 1`** instead of the received `Num`. RFC 7959
   §3.2 Figure 7 answers request `1:0/1/128` with `1:0/1/128`; `NUM`
   acknowledges the block just stored.
2. **Reassembly was keyed on the token**, but libcoap changes it between blocks
   (block 0 empty, block 1 eight bytes) while keeping `Request-Tag` (RFC 9175
   §3.1, option 292) stable. Keyed on the token alone the blocks never joined.

Fixed in `coap_listener` (`continue_reply/3`, `block1_stream/5` + `block1_tag/1`).
`coap_listener_SUITE` now pins both: a changed token with a stable `Request-Tag`
reassembles, and the `2.31` carries the received block number. Removing either
fix turns the suite red.
