# UnifiedMQ

UnifiedMQ is a multi-protocol message broker on Erlang/OTP 28. MQTT, WebSocket,
NATS, AMQP 0-9-1, Kafka and Pulsar clients share one message kernel, with
CloudEvents 1.0 as an optional event layer and a Raft/Mnesia control plane.

The authoritative, up-to-date documentation is in Chinese:

- [README.md](README.md) — goals, architecture, acceptance criteria and roadmap.
- [docs/roadmap.md](docs/roadmap.md) — stage status and gate evidence.
- [docs/architecture.md](docs/architecture.md) — applications, processes and the
  publish/subscribe data path.
- [docs/operations.md](docs/operations.md) — deployment, admin API, backup and
  upgrade.
- [docs/protocol-compatibility.md](docs/protocol-compatibility.md) — per-protocol
  capability matrix and real third-party client evidence.
- [docs/storage-and-recovery.md](docs/storage-and-recovery.md) — WAL format,
  sessions and recovery.

Status: S0–S6 are implemented and tested (631 CT cases plus six real third-party
client suites). Deliberately unimplemented items are listed in
[docs/security.md](docs/security.md) §12.
