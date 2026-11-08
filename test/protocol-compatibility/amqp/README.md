# S5 AMQP — real-client compatibility

Evidence for the Gate S5 item "通过真实第三方客户端集成测试，无自编客户端互测"
(`docs/roadmap.md` §7 Gate S5, `docs/protocol-compatibility.md` §4.7).

The suite drives a **real `erl -noshell -s amqp_broker_boot run` broker node**
(ranch ephemeral port `port => 0`, WAL backend, ext4 data dir) and connects the
official **`github.com/rabbitmq/amqp091-go` v1.10.0** client to it — the AMQP
0-9-1 client named in the S0.4 client matrix. The wire protocol is AMQP 0-9-1,
implemented independently by `amqp091-go`. The Go program never speaks the wire
protocol by hand: it uses `amqp.Dial`, `Channel`, `ExchangeDeclare`,
`QueueDeclare`, `QueueBind`, `Consume`, `Publish`, `Ack`, `Nack` and `Qos`.

| Client | Version | Where it runs |
|---|---|---|
| amqp091-go (Go) | v1.10.0 | host, Go 1.26.10 |

The boot path (`amqp_broker_boot.erl`) mirrors `amqp_conn_SUITE
init_per_testcase` exactly: same env, same ephemeral binding, same
`ranch:get_port` readback.

## Run

```sh
export PATH="/home/wanli/Development/otp-28.3.1/bin:$PATH"
export PATH="/home/wanli/Development/go1.26.10/bin:$PATH"

# whole suite (starts one broker, runs all 13 cases)
python3 test/protocol-compatibility/amqp/amqp_compat.py --all

# one case
python3 test/protocol-compatibility/amqp/amqp_compat.py --case amqp-topic

# reuse the already-built Go binary
python3 test/protocol-compatibility/amqp/amqp_compat.py --all --skip-build
```

Offline build notes: `go.mod` pins `amqp091-go v1.10.0`; the driver sets
`GOPROXY=off`. Reports land in `run/report.json`; the broker log in
`run/broker.log` (`run/`, `data/` and `_build/` are gitignored). The suite starts
from a clean data dir so a previous run cannot leak into a case.

## Cases → contract

| Case | What it proves |
|---|---|
| `amqp-connect` | protocol header → start/start-ok/tune/tune-ok/open/open-ok, then `channel.open` |
| `amqp-pubsub` | direct exchange + queue + bind → publish → `basic.deliver` body exact → `basic.ack` |
| `amqp-direct` | routing-key match delivers; a non-matching binding receives nothing |
| `amqp-fanout` | three bound queues all receive the same message |
| `amqp-topic` | `stock.*` matches one word; `stock.#` matches the rest; `*` never matches two words |
| `amqp-competing-consumers` | two consumers on one queue receive 10 messages **exactly once each** |
| `amqp-manual-ack` | an acked message is not redelivered |
| `amqp-nack-requeue` | `nack(requeue=true)` puts the message back and it is redelivered |
| `amqp-unbind` | delivery stops after `queue.unbind` |
| `amqp-queue-delete` | `queue.delete` removes the queue; a later passive declare fails |
| `amqp-passive-missing` | passive declare of a missing exchange/queue closes the channel with 404 |
| `amqp-prefetch-qos` | `basic.qos(1)` allows one unacked delivery at a time |
| `amqp-multi-channel` | two channels on one connection stay isolated |

## Scope / known limits (AMQP v1)

* **AMQP 0-9-1 subset** — no `basic.get` (answers `basic.get-empty`), no
  `mandatory` `basic.return`, no headers-exchange routing, no publisher
  confirms or transactions.
* **transient topology** — exchanges/queues/bindings live in ETS and are lost on
  broker restart; message bodies are still fsynced in `broker_queue`.
* **consumer-join anchor** — a shared cursor is anchored at the partition tail
  when a consumer joins, so messages published before the first consumer
  attaches are not replayed. v1 targets the "consumer online, then publish"
  flow; documented in §4.7.
* **single tenant** (`t0`) until S6.
