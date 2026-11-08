# S5 NATS — real-client compatibility

Evidence for the Gate S5 item "通过真实第三方客户端集成测试，无自编客户端互测"
(`docs/roadmap.md` §7 Gate S5, `docs/protocol-compatibility.md` §4.6).

The suite drives a **real `erl -noshell -s nats_broker_boot run` broker node**
(ranch ephemeral port `port => 0`, WAL backend, ext4 data dir, token auth on)
and connects the official **Go `nats.go` v1.48.0** client to it — the NATS client
named in the S0.4 client matrix. The wire protocol is core NATS, which `nats.go`
implements independently of this project. The Go program never speaks the text
protocol by hand: it uses `nats.Connect`, `SubscribeSync`,
`QueueSubscribeSync`, `Publish`, `Request` and `Flush`.

| Client | Version | Where it runs |
|---|---|---|
| nats.go (Go) | v1.48.0 | host, Go 1.26.10 |

The boot path (`nats_broker_boot.erl`) mirrors `nats_conn_SUITE
init_per_testcase` exactly: same env, same ephemeral binding, same
`ranch:get_port` readback, same token.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
export PATH="/home/wanli/Development/go1.26.10/bin:$PATH"

# whole suite (starts one broker, runs all 11 cases)
python3 test/protocol-compatibility/nats/nats_compat.py --all

# one case
python3 test/protocol-compatibility/nats/nats_compat.py --case nats-queue-group

# reuse the already-built Go binary
python3 test/protocol-compatibility/nats/nats_compat.py --all --skip-build
```

Offline build notes: `go.mod` pins `nats.go v1.48.0` plus the higher cached
`golang.org/x/crypto v0.39.0` / `golang.org/x/sys v0.33.0` (the versions
available in `GOMODCACHE`); the driver sets `GOPROXY=off`. Reports land in
`run/report.json`; the broker log in `run/broker.log` (`run/`, `data/` and
`_build/` are gitignored). The suite starts from a clean data dir so a leftover
WAL cannot leak into `nats-no-history`.

## Cases → contract

| Case | What it proves |
|---|---|
| `nats-connect-info` | real `INFO` → `CONNECT` → `PING`/`PONG` exchange; connection reaches `CONNECTED` |
| `nats-pubsub` | `SubscribeSync` → `Publish` → `MSG` payload exact |
| `nats-wildcard` | `*` matches exactly one token; `>` matches the rest; negative case really gets nothing |
| `nats-queue-group` | two members of one queue group receive 10 messages **exactly once each** |
| `nats-request-reply` | `Request` uses an inbox and receives `Respond`'s reply |
| `nats-no-history` | a subscription created after a publish does not replay it; the next publish does arrive |
| `nats-unsubscribe` | delivery stops after `Unsubscribe` |
| `nats-auto-unsubscribe` | `AutoUnsubscribe(1)` → exactly one delivery |
| `nats-echo-false` | `NoEcho()` client does not receive its own publish; another client does |
| `nats-ping-flush` | 20 concurrent `Flush()` calls all complete (PING/PONG under load) |
| `nats-auth-failure` | a wrong `CONNECT` token is refused with an authorization error |

## Scope / known limits (NATS v1)

* **core NATS only** — no `HPUB`/`HMSG` headers (2.2 extension) and no
  JetStream. Those commands are refused explicitly, not silently ignored.
* **at-most-once** — core NATS has no ack handshake. The adapter acks its own
  cursor right after writing the `MSG`; the broker has already fsynced the
  message, so it is strictly stronger than NATS core, but a crash between write
  and cursor advance can lose one delivery to that connection.
* **`>` parent-level difference** — broker `#` matches the parent subject,
  NATS `>` does not. Recorded as a conversion difference in §4.6.
* **anonymous per-connection client id** — NATS has no persistent session or
  client identity, so reconnect is a new `ClientId`; no MQTT-style takeover.
* **single tenant** (`t0`) until S6.
