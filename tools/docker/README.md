# T14 — S1 crash-recovery harness

The Gate S1 crash matrix (design §8.2) run against a **real release node in a
container** with the data directory on the host's ext4 filesystem.  `kill -9`
is injected from the host (`docker kill -s KILL`); assertions are made at the
MQTT protocol edge plus a one-shot Erlang probe for the values the protocol
cannot expose (WAL high watermark, flushed cursor, replay time).

## What runs

| Piece | Role |
|---|---|
| `docker-compose.s1.yml` | declarative description of the same broker service (data bind mount, port `18830`, OTP image); kept as the §8.1 reference |
| `entrypoint.sh` | starts epmd, creates the four partitions out of band (`broker_queue:create_partition/1`, H8), then execs the release in `foreground` mode |
| `s1_recovery.py` | renders `sys.config`, drives paho-mqtt clients, injects kill -9, runs R1a/R1b/R2–R7, writes `run/report-*.json` |
| `../../_build/t14/rel/unified_mq` | release built by `rebar3 as t14 release`: `dev_mode=false` (apps are copied, not symlinked) and `include_erts=false` |

The container image is `erlang:28.3.1.0` — the exact OTP/ERTS version the
project pins (28.3.1 / 16.2).  Therefore the release is built on the host but
executes against the same runtime, with no host symlinks or host-linked ERTS
inside the container.

## Usage

```sh
# Full matrix: R1a..R7 in both cursor tiers.
python3 tools/docker/s1_recovery.py

# A single scenario while developing.
python3 tools/docker/s1_recovery.py --scenario r3 --tier zero

# Keep a failing data directory for inspection (container is left down).
python3 tools/docker/s1_recovery.py --scenario r4 --tier default --keep-data
```

The script rebuilds the release (`rebar3 as t14 release`) unless
`--skip-build` is given, and deletes `tools/docker/data/` between scenarios so
no assertion can pass on a previous run's state.  Requires `docker-compose`
1.29 (`docker compose` is not installed on the reference host) and Python
`paho-mqtt` 2.x.

## Tier semantics

`render_config` reads the shipped `config/sys.config.src` and substitutes only
the values below, asserting each replacement — the T14 run cannot silently use
a stale copy of the config.

| Tier | `cursor_max_regress` | `cursor_flush_interval_ms` | `inflight_flush_interval_ms` |
|---|---|---|---|
| `default` | 64 | 100 | 100 |
| `zero` | 1 | 0 | 0 |

R3 and R7 are run in both tiers.  The zero tier must show `flushed_cursor ==
N+1` and **no** duplicate delivery after the crash; the default tier only has
to prove the no-loss side (duplicates are allowed inside the regress window).

## Scenario → assertion map

| # | Injection | Assertion |
|---|---|---|
| R1a | publish QoS1, wait PUBACK, kill -9 | exact payload is replayed after restart; probe reports one WAL record |
| R1b | subscribe (clean=false), kill -9 | `session_present=1`, restored subscription delivers **without** a re-SUBSCRIBE; `.subs` survives |
| R2 | 10 acked + 190 racing QoS1 publishes, kill -9 | recovered set is a gap-free prefix of the publish order; the acked prefix is not lost; no torn/partial payload |
| R3 | 200 QoS1 delivered+acked, kill -9 (socket left open) | read `.cursors`; every offset **after** the flushed cursor is replayed, never lost. Zero tier: cursor at N+1, zero duplicates |
| R4 | 20 acked records, kill -9, append `55 01` to the last segment | recovery truncates back to the pre-tear size; all 20 records replay; no phantom record |
| R5 | 10 QoS0 publishes, kill -9 | probe sees 0 stored records and the restarted broker delivers nothing |
| R6 | 20,000 QoS1 records, kill -9 | throwaway VM recovers all four partitions and reports replay time < 500 ms (measured apart from VM/container startup) |
| R7 | QoS1 delivered, **not** acked, kill -9 | the message is delivered again after restart (`dup=1` or equivalent cursor catch-up); no loss |

## Notes / deviations

* **M13** — the paho driver runs on the host, not in a `mqtt-client` compose
  service as §8.1 sketched.  The scenario needs one process to own both the MQTT
  socket and the instant of `kill -9`; doing that across two containers would
  add a control channel without testing anything more.  The broker side is
  still fully containerised, and the data dir is a real host mount.
* **M14** — R6 measures `broker_storage_wal:recover/2` for all partitions in a
  one-shot VM, not boot-to-port wall time.  The design budget (500 ms) is about
  record replay; including container/VM startup would make the number depend on
  the host's Docker daemon and hide the storage property being gated.
* **M15** — on the reference host `docker-compose` 1.29.2 cannot talk to the
  daemon (`requests 2.32 / urllib3 2.5` reject the `http+docker` scheme used by
  docker-py 6.1.3).  The driver therefore issues the equivalent `docker run` /
  `docker kill` / `docker start` commands directly; `docker-compose.s1.yml`
  remains the declarative spec and works unchanged where compose is healthy.
  The container flags (user, mounts, env, port, stop timeout) are identical.
* `kill -9` still cannot model the `os_buffer` power-loss window (§8.3); that
  remains S2's dm-flakey/VM work, as recorded in the design and
  `docs/protocol-compatibility.md`.
