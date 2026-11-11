# 协议服务端符合性测试验证方案

- **文档版本**：v1.0（2026-11-10 起草，待代码落地）
- **状态**：**设计 / 落地规划**——本文件描述「如何借助第三方测试资源验证自研 Broker 六个协议的服务端实现」，本身不是已完成的测试套件，后续由 `test/conformance/` 下的代码实现（S8 的三个新协议不在本文范围，见 §0 补注）
- **读者**：负责协议层、CI 与质量保障的工程师；需要判断「某个协议该用什么外部资源做符合性验证」的评审人
- **对应**：`docs/protocol-compatibility.md`（六协议能力矩阵与已实现证据）、`docs/architecture.md` §1、`docs/roadmap.md` §9、`docs/security.md` §2.1/§5.1
- **证据口径**：本文中的「已实测能力」引用自 `docs/protocol-compatibility.md` 的基线（起草时为 **673/673、43 套件**；**当前为 823/823、52 套件**，S8-D 收尾，2026-11-11）；「外部资源」为本文开头汇总的公开仓库，未经本仓库本地验证，**不可直接当作官方标准认证**

> **最重要的前置声明（贯穿全文）**：以下整理的所有外部资源，**都不是「官方标准符合性认证套件」**。其中 Kafka 的第三方套件最接近本 Broker 的验证场景，其余多为「可借鉴思路」或「客户端/内部工具」。任何对外表述都不得把它们说成标准认证；它们提供的是**结构化反馈、互操作证据、异常路径思路**，而非合规背书。

---

## 0. 背景与目标

本 Broker（`unified-mq`）是一个支持 **MQTT 3.1.1/5.0、WebSocket、NATS、AMQP 0-9-1、Kafka、Pulsar** 六协议的 Erlang/OTP 实现，协议适配层为 `apps/broker_proto_*`（`broker_proto_mqtt` / `broker_proto_ws` / `broker_proto_nats` / `broker_proto_amqp` / `broker_proto_kafka` / `broker_proto_pulsar`）。

> **范围（2026-11-11 补注）**：本文只规划上面这六个 S5 协议的**外部** conformance 资源接入。S8 新增的 MQTT-SN、MQTT over QUIC、MQTT over WebSocket 不在本文范围内——它们的第三方客户端证据（`mqtt-sn-tools`、EMQX `emqtt`、paho websocket）已在 `docs/protocol-compatibility.md` §4.11~§4.13 记录，外部 conformance suite 的规划同样未立项。

S5 已通过「真实第三方客户端集成测试」建立协议兼容基线（见 `docs/protocol-compatibility.md` §0 客户端矩阵），但那是**自编断言 + 真实客户端**，缺少以下两类外部验证：

1. **第三方 conformance suite 的结构化反馈**：别人写好的、针对「兼容该协议的 Broker」的断言集。
2. **厂商中立互操作套件**：如 Eclipse Paho 的互操作测试，验证的是「我的 Broker 是否能被标准客户端正确驱动」。

本文把能找到的外部测试资源按协议分类，给出**可落地的接入方案、harness 接口约定、验收口径与边界声明**，目标是后续用代码把这些资源接进 `test/conformance/` 并纳入 CI。

---

## 1. 资源总览与优先级

| 协议 | 外部资源 | 类型 | 官方性 | 与本 Broker 的契合度 | 落地优先级 |
|---|---|---|---|---|---|
| Kafka | `github.com/faustbrian/golib/pkg/kafka/kafkatest` | 第三方 **conformance suite**（Go） | 非官方，但明确用于测试兼容 Kafka 的 Broker | **高**——唯一结构化反馈 | **P0** |
| Kafka（备选参考） | `github.com/aranw/kafkatest` | Docker 启动 Redpanda 做集成测试的库 | 非官方，偏客户端测试 | 低，仅参考 | — |
| MQTT | Eclipse Paho `interoperability/client_test.py` / `client_test5.py` | 厂商中立官方互操作套件 | **官方中立**（针对 3.1.1/5.0） | **高**——标准互操作验证 | **P1** |
| MQTT | `fss-mqtt-broker` `crates/mqttd/tests/wire.rs` | 线格式（字节级畸形包）测试思路 | 第三方内部思路 | 中——异常路径借鉴 | **P1（补充）** |
| AMQP / RabbitMQ | Pika / Bunny / Java client 交叉测试 | 官方客户端库 | 官方客户端，但**无服务端符合性套件** | 中——核心场景交叉 | **P2** |
| Pulsar | `ruststream-pulsar` `conformance_pulsar.rs` | 客户端库**内部**测试 | 非官方，验证客户端自身 | 低——非服务端 | **P3（仅参考）** |
| NATS | `@nats-io/nst` | 官方 nats.js 内部测试工具 | 官方声明「**not a supported public API**」 | 低——不稳定 | **P3（仅参考）** |
| NATS | 官方 `nats-server` `test/test.go` | 官方服务端集成测试工具包 | 官方但**不对外**作符合性套件 | 低——参考改写 | **P3（仅参考）** |
| MQTT-SN | `com.mobius-software.mqttsn:mqttsn-testsuite` | 7 年前 Maven 性能包 | 非官方，无服务端验证证据 | **不建议** | 不投入 |
| MQTT-QUIC | （无公开套件） | — | — | — | 不投入（本 Broker 未实现） |

**实操优先级（与上文一致）**：

1. **Kafka**：实现 `BrokerHarness`，跑 `kafkatest` 的 Conformance 函数——唯一能给结构化反馈的现成代码。
2. **MQTT**：把 Paho 的 `client_test.py` / `client_test5.py` 指向本 Broker；参考 `wire.rs` 自建畸形包测试，专攻异常路径。
3. **RabbitMQ/AMQP**：用 Pika、Bunny、Java client 交叉跑核心场景，因为没有官方符合性套件。
4. **其余（Pulsar / NATS）**：只能参考对应官方 Broker 的测试代码自己改写，或扩展本 Broker 已有的官方客户端实测路径。

---

## 2. 总体验证方法论与落地结构

### 2.1 四层验证体系

本 Broker 的协议验证分四层，本文聚焦第 2、3 层（外部资源），第 1、4 层已有基础：

| 层 | 验证对象 | 工具 | 现状 | 本文增量 |
|---|---|---|---|---|
| L1 帧/单元层 | 编解码、状态机 | Erlang CT（`apps/broker_proto_*/test/`） | 已建（823/823 含帧层） | 不变 |
| L2 真实客户端 e2e | 端到端语义 | `test/protocol-compatibility/`（paho / nats.go / kafka-clients / pulsar-client 等） | 已建（S5 基线） | 不变，作为对照基准 |
| **L3 外部 conformance** | 第三方断言集 | `kafkatest` / Paho 互操作 / 客户端交叉 | **未建** | **本文核心** |
| L4 异常/混沌路径 | 畸形包、故障注入 | `tools/docker/`（T14/S2）、`wire.rs` 思路 | MQTT 已有 T15 字节级 | 扩展畸形包专项 |

### 2.2 通用 Harness 接口约定（所有协议共享的抽象）

无论哪个协议的 conformance 套件，本 Broker 都需要提供一个「被测试对象」的适配器，统一满足以下契约，才能被外部测试驱动：

```erlang
%% 概念接口（各协议具体形态见 §3~§7）
-callback conformance_setup() -> #{broker_host => string(),
                                   broker_port => pos_integer(),
                                   bootstrap :: fun(() -> ok),
                                   teardown :: fun(() -> ok)}.
```

各协议 harness 的最小能力要求：

- **地址注入**：外部套件必须能通过环境变量 / 参数拿到本 Broker 的监听地址（如 `BROKER_HOST=127.0.0.1` `BROKER_PORT=9092`）。
- **Topic/资源准备**：套件建 topic、清 topic 的能力要映射到本 Broker 的「带外分区创建」语义（见 `docs/architecture.md` §1：分区创建是带外动作，`create_partition/1` 是唯一创建者）。
- **身份**：使用 `broker_security` 统一身份表（`docs/security.md` §2.1）——`guest/guest→t0`、`alice/pw→t1`、token `tok`。harness 不得信任客户端自带的 `tenant`。
- **隔离**：每个 conformance 套件跑在独立租户 / 独立 topic 前缀，避免相互污染；用 `broker_security:default_tenant/0` 决定匿名档。

### 2.3 与现有 `test/` 目录的关系

```
test/
├── integration/                 # 已有（s1_e2e_SUITE）
├── protocol-compatibility/      # 已有（mqtt/ws/nats/amqp/kafka/pulsar，L2 真实客户端）
└── conformance/                 # 本文新增（L3 外部套件接入）
    ├── kafka/                   # §3 kafkatest 接入
    ├── mqtt/                    # §4 Paho 互操作 + wire.rs 思路
    ├── amqp/                    # §5 客户端交叉
    ├── pulsar/                  # §6 扩展官方 Java Client
    └── nats/                    # §7 扩展 nats.go + raw 测试
```

> Kafka / MQTT 的目录会包含「适配器代码 + 调外部套件的驱动脚本」；AMQP / Pulsar / NATS 的目录以「扩展本 Broker 既有客户端实测脚本」为主。

---

## 3. Kafka 验证方案（P0 — 唯一结构化反馈）

### 3.1 资源

- **主选**：`github.com/faustbrian/golib/pkg/kafka/kafkatest`（Go）
  - 提供公开的 conformance suite，明确用于测试**兼容 Kafka 的 Broker**。
  - 核心用法：实现它的 `BrokerHarness`（提供 Broker 地址、建 Topic、读记录、查 offset），然后调用：
    - `RunProducerConformance` —— 生产端一致性
    - `RunConsumerConformance` —— 消费端一致性
    - `RunTransactionConformance` —— 事务一致性
- **备选（仅参考）**：`github.com/aranw/kafkatest` —— 用 Docker 启动 Redpanda 做集成测试，主要方便测试**客户端**，不是用来验证 Broker 实现，不纳入主方案。

### 3.2 本 Broker Kafka 已实测能力（对照 `docs/protocol-compatibility.md` §4.8 / §2）

S5 已用 `kafka-clients 3.7.1` 实测 12/12 用例，已实现：

- Produce 8、Fetch 11、ListOffsets 5、Metadata 8、OffsetCommit 7、OffsetFetch 5、FindCoordinator 2、JoinGroup 5、Heartbeat 3、LeaveGroup 3、SyncGroup 3、InitProducerId 0
- RecordBatch v2（**无压缩**）；经典消费组；单节点协调器；offset 内存态

**已知限制（直接决定 §3.3 的取舍）**：无压缩、无事务、无幂等、无 Admin API。

### 3.3 落地方案

#### 3.3.1 BrokerHarness 适配器（Go 侧）

本 Broker 需提供一个 Go 适配器实现 `kafkatest.BrokerHarness`：

```go
// test/conformance/kafka/harness.go（示意）
type UnifiedMQHarness struct {
    host string
    port int
}

func (h *UnifiedMQHarness) Address() string { return fmt.Sprintf("%s:%d", h.host, h.port) }

// CreateTopic: 映射到本 Broker 的带外分区创建语义
func (h *UnifiedMQHarness) CreateTopic(ctx context.Context, name string, partitions int) error {
    // 调用本 Broker 的管理面 / 受控 API 建分区（见 ADR-011 §11.2：boot 只 recover，create 带外）
    // 不支持的分区数/配置需返回明确错误，让套件记为 SKIP 而非 FAIL
}

func (h *UnifiedMQHarness) ReadRecords(ctx context.Context, topic string, partition int32, offset int64) ([]Record, error) { /* Fetch API */ }
func (h *UnifiedMQHarness) LatestOffset(ctx context.Context, topic string, partition int32) (int64, error) { /* ListOffsets */ }
```

约束：
- 地址来自环境变量 `UMQ_KAFKA_HOST` / `UMQ_KAFKA_PORT`（默认 `127.0.0.1:9092`）。
- `CreateTopic` 必须落在「带外分区创建」路径上，不能依赖 Kafka 的自动建 Topic 语义（本 Broker 不实现）。
- 身份用 SASL/PLAIN（`broker_security` 统一身份，见 `docs/security.md` §2.1），harness 配置 `guest/guest`。

#### 3.3.2 三个 Conformance 函数的取舍

| 函数 | 本 Broker 支持度 | 处置 |
|---|---|---|
| `RunProducerConformance` | 高（Produce/Fetch/RecordBatch v2 已实现） | **全跑**，作为 Kafka 验证主证据 |
| `RunConsumerConformance` | 高（消费组 / offset 已实现，内存态） | **全跑** |
| `RunTransactionConformance` | **不支持**（无事务、无幂等、无 InitProducerId 实质语义） | **显式 SKIP**，在报告中标注「本 Broker Kafka 不支持事务语义，未实现」；不得伪造通过 |

#### 3.3.3 验收口径

- `RunProducerConformance` + `RunConsumerConformance` 全绿 ⇒ Kafka L3 验证通过。
- 报告须含：跑过的断言数、SKIP 项及理由、`guest/guest` 身份下的行为。
- 与 `test/protocol-compatibility/kafka/` 的 L2 结果交叉核对，差异即缺陷。

---

## 4. MQTT 验证方案（P1 — 标准互操作 + 异常路径）

### 4.1 资源

- **主选：Eclipse Paho 互操作性测试套件**（厂商中立官方测试集，针对 MQTT 3.1.1 与 5.0）
  - 用法（参考 Opifex 项目的 CI 接入）：
    ```bash
    git clone https://github.com/eclipse-paho/paho.mqtt.testing /tmp/paho
    python3 /tmp/paho/interoperability/client_test.py   # 3.1.1
    python3 /tmp/paho/interoperability/client_test5.py  # 5.0
    ```
  - 验证目标：本 Broker 是否能被**标准客户端**正确驱动（连接、订阅、发布、QoS、保留、遗嘱等）。
- **补充：fss-mqtt-broker 的线格式测试 `wire.rs`**（`crates/mqttd/tests/wire.rs`，链接见 `docs` 开头汇总）
  - 思路：手工构造畸形字节流，断言服务端是「静默关闭」「拒绝」还是「接受」，专门抓非法 topic、畸形包等深层缺陷。
  - 这是**思路借鉴**，不是直接拿来跑（Rust 代码，本 Broker 是 Erlang）。把它翻译成 Erlang CT 用例或 Python 字节注入用例。

### 4.2 本 Broker MQTT 已实测能力（对照 `docs/protocol-compatibility.md` §4/§4.1/§4.2）

- MQTT 3.1.1：QoS0/1/2、Retain、Will、clean session、keep_alive、通配符、per-session 订阅 + cursor + in-flight 持久化（崩溃恢复 T14 双档 8/8）、真实 paho 2.x + mosquitto 2.1.2 逐帧字节一致（T15，10/10、46/46 帧 roundtrip）。
- MQTT 5.0（子集，S2.5/ADR-017）：CONNECT properties、PUBLISH properties、v5 reason codes、`$share` 复用 Shared 投递、session expiry 落盘。
- 逐条对外可见限制：`mqtt_max_qos` 超限断连、非法 topic filter 的 SUBSCRIBE 直接断连（无 SUBACK）、QoS2 跨崩溃允许重复（非 exactly-once）、无背压只有停投。

### 4.3 落地方案

#### 4.3.1 Paho 互操作接入 CI

- 在 `test/conformance/mqtt/` 下放驱动脚本（Bash/Python），`git clone` Paho 测试集到临时目录（建议 pinned commit，避免上游漂移），把 `client_test*.py` 的 Broker 地址指向本 Broker 的 MQTT 端口（默认约定 `18831`，与 T15 一致）。
- 身份：Paho 用 `guest/guest`（统一身份表）。
- 需要本 Broker 暴露两种监听：3.1.1 与 5.0（同一端口按 Protocol Level 协商即可）。
- 断言口径：Paho 脚本原生的 PASS/FAIL 输出即为证据；把不支持的项（如 AUTH 扩展认证、topic alias）标记为预期 SKIP，并在报告注明对应 `docs/protocol-compatibility.md` §4 的限制条目。

#### 4.3.2 wire.rs 风格的畸形包专项（异常路径）

在 `test/conformance/mqtt/wire/` 下用 Python 手工拼字节（参考 §2.2，复用 T15 的透明 TCP tee 思路），覆盖：

- 非法 topic filter（含 NUL、通配符错位）→ 期望**断连无 SUBACK**（对应 §4.2 限制 3）。
- 畸形固定头 / 超长剩余长度 / 截断包 → 期望**静默关闭**或拒绝。
- 超限 QoS 的 PUBLISH → 期望**断连零存储零投递**（对应限制 1）。
- 非法 v5 帧型 → 期望用 v5 帧型回 reason code（对应 §4.2 限制 2 / T15 M16）。

每条用例断言「服务端行为属于哪一类（静默关闭 / 拒绝 / 接受）」，与 `docs/protocol-compatibility.md` §4 的对外限制逐条对账。

#### 4.3.3 验收口径

- Paho `client_test.py` + `client_test5.py` 全绿（除文档声明的 SKIP）。
- `wire/` 畸形包用例全绿，且与 §4.2 限制清单一一对应。
- 与 `test/protocol-compatibility/mqtt/`（L2 T15）交叉核对。

---

## 5. RabbitMQ / AMQP 验证方案（P2 — 无官方套件，客户端交叉）

### 5.1 资源与约束

- **不存在**任何 AMQP / RabbitMQ 官方服务端符合性套件。
- 可行策略：用官方客户端库 **Pika（Python）、Bunny（Ruby）、Java client（amqp 或 spring-amqp）** 交叉跑核心场景，互相印证本 Broker 对 AMQP 0-9-1 子集的实现。

### 5.2 本 Broker AMQP 已实测能力（对照 `docs/protocol-compatibility.md` §4.7 / §2）

S5 用 `amqp091-go v1.10.0` 实测 13/13 用例，已实现 AMQP 0-9-1 子集：`direct/fanout/topic` exchange、queue declare/bind/consume、publish、ack/nack、basic properties、SASL/PLAIN 认证（走统一身份表）。

**已知限制**：无事务、无 publisher confirm 全档、无流控 tuning、无 AMQP 1.0。

### 5.3 落地方案

在 `test/conformance/amqp/` 下，用三种官方客户端各写一份核心场景脚本，断言一致：

| 场景 | 覆盖点 | 交叉验证意义 |
|---|---|---|
| 直连 / 扇出 / 主题路由 | exchange + binding | 三种客户端路由结果一致 |
| 队列声明 / 绑定 / 删除生命周期 | queue.declare/bind/delete | 生命周期语义一致 |
| 发布 / 消费 / ack / nack | basic.publish/consume/ack/nack | 投递与 ACK 契约一致 |
| SASL/PLAIN 认证失败路径 | connection.start-ok | 统一身份表行为一致（见 §2.2） |

约束：
- 身份统一用 `guest/guest`（避免各客户端默认凭据差异）。
- 不支持的场景（事务、confirm）显式 SKIP 并标注。
- 三种客户端结果必须一致，否则即缺陷。

---

## 6. Pulsar 验证方案（P3 — 仅参考，扩展官方客户端）

### 6.1 资源与边界

- `ruststream-pulsar` 的 `conformance_pulsar.rs` 是 **Rust 客户端库的内部测试**，用于验证该客户端自身的生命周期与功能，**并非**用来验证任意 Pulsar Broker 是否符合协议。
- 因此该资源**不作为**本 Broker 的符合性验证手段，仅作接口/语义参考。

### 6.2 本 Broker Pulsar 已实测能力（对照 `docs/protocol-compatibility.md` §4.9 / §2）

S5 用**官方 Pulsar Java Client 3.3.1** 实测 8/8 用例，已实现：Connect、Lookup、PartitionedMetadata、Producer、Send（单条+batch）、Subscribe、Flow、Ack、Seek、Unsubscribe、Ping、GetLastMessageId；properties/key/event_time/sequence_id；CONNECT 凭据（token / SASL PLAIN，走统一身份表）。

**已知限制**：无 BookKeeper/ledger（ADR-006）、无事务/压缩/Key_Shared/schema；订阅游标内存态。

### 6.3 落地方案

Pulsar 不构成独立 conformance 套件接入。落地动作是**扩展 `test/protocol-compatibility/pulsar/` 的官方 Java Client 实测脚本**，覆盖更多场景（更多订阅类型、batch 边界、seek 精度等），并以 `conformance_pulsar.rs` 为语义对照参考，不引入该 Rust 依赖。

---

## 7. NATS 验证方案（P3 — 仅参考，扩展 nats.go + raw）

### 7.1 资源与边界

- `@nats-io/nst`：官方 nats.js 的内部测试工具，官方明确声明「**not a supported public API**」，**不能**作为稳定符合性测试方案。
- `ruststream` 的 NATS 测试：同样是其客户端库自身的 conformance 检查。
- 官方 `nats-server` 的 `test/test.go`：官方服务端自己的集成测试工具包（提供 raw TCP 原语、正则匹配器），**不对外**作为符合性套件。
- 三者都只能作为**思路参考**，不可作为稳定断言来源。

### 7.2 本 Broker NATS 已实测能力（对照 `docs/protocol-compatibility.md` §4.6 / §2）

S5 用 `nats.go v1.48.0` 实测 11/11 用例，已实现 core NATS：PUB/SUB/UNSUB、subject wildcard（`*`/`>`）、Queue Group、Request/Reply、INFO/CONNECT/PING/PONG、auth、HPUB/HMSG headers（`ce-*`）。

**已知限制**：无 JetStream、无集群路由。

### 7.3 落地方案

- **主**：扩展 `test/protocol-compatibility/nats/` 的 `nats.go` 实测脚本，增加更多 core NATS 场景（含 `HPUB/HMSG` 的 `ce-*` header 透传，对应 CloudEvents Binding 验证）。
- **补充（借鉴 `test/test.go` 思路）**：在 `test/conformance/nats/` 下用 raw TCP 原语构造 NATS 协议帧，断言 `INFO/CONNECT/PING/PONG` 协商与 auth 失败路径行为（对应统一身份表 §2.2）。
- 不引入 `@nats-io/nst` 或 `ruststream` 依赖。

---

## 8. MQTT-SN / MQTT-QUIC（不投入）

- **MQTT-SN**：仅找到一个近 7 年前的 Maven 包 `com.mobius-software.mqttsn:mqttsn-testsuite`，标注为「Performance Testsuite」，且无证据表明可用于服务端协议验证。**不建议使用**，且本 Broker 未实现 MQTT-SN。
- **MQTT-QUIC**：没有找到任何公开测试套件；本 Broker 未实现。
- 结论：这两项仅记录于本文，不进入 `test/conformance/`，不与本 Broker 现状混淆。

---

## 9. 实操落地计划（按代码实现）

### 9.1 目录与交付物

```
test/conformance/
├── kafka/      harness.go + run_kafkatest.sh   # §3：实现 BrokerHarness，跑 Producer/Consumer Conformance
├── mqtt/       run_paho.sh + wire/*.py          # §4：Paho 互操作 + 畸形包专项
├── amqp/       pika_test.py + bunny_test.rb + java_test.java  # §5：三客户端交叉
├── pulsar/     extend_java_client/              # §6：扩展官方 Java Client 实测
└── nats/       extend_natsgo/ + raw_tcp/         # §7：扩展 nats.go + raw 测试
```

### 9.2 CI 集成

接入 `.github/workflows/ci.yml` 的 `protocol-compatibility` 层（与现有 L2 并列新增 L3 阶段），建议矩阵：

- **Kafka conformance**：构建 Kafka 监听的 release（`rebar3 as t14 release` 同形态），起 Broker，跑 `RunProducerConformance` + `RunConsumerConformance`，产物 `conformance-kafka.json` 归档。
- **MQTT 互操作**：起 MQTT 监听，clone pinned Paho，跑 `client_test.py`/`client_test5.py`；再跑 `wire/` 畸形包。
- **AMQP 交叉**：三客户端脚本串行（环境需装 Python/Ruby/JDK）。
- **Pulsar / NATS**：扩展脚本并入现有 L2 套件，不必单独 stage。

每一 stage 失败须阻断合并；SKIP 项需显式列出理由（见各 § 的限制条目）。

### 9.3 验收标准

| 协议 | 通过条件 | 已知 SKIP |
|---|---|---|
| Kafka | Producer + Consumer Conformance 全绿 | TransactionConformance（无事务） |
| MQTT | Paho 3.1.1 + 5.0 全绿；wire 畸形包全绿 | AUTH 扩展认证、topic alias |
| AMQP | Pika/Bunny/Java 三客户端核心场景一致 | 事务、publisher confirm、AMQP 1.0 |
| Pulsar | 官方 Java Client 扩展场景全绿 | 事务、压缩、Key_Shared、schema |
| NATS | nats.go 扩展 + raw 协商全绿 | JetStream、集群路由 |

### 9.4 风险与边界声明（对外表述红线）

1. 所有外部套件**都不是官方标准认证**，任何文档/汇报不得声称「通过标准认证」。
2. 对不支持的能力一律 **SKIP + 明示**，不得伪造通过（尤其 Kafka 事务、AMQP 事务/confirm）。
3. 本 Broker 的「带外分区创建」「统一身份表（不信任客户端 tenant）」「崩溃至少一次」等语义，是 conformance 适配器的硬约束，必须在 harness 里体现，不能让套件以为「自动建 Topic / 匿名可指定租户 / exactly-once」。
4. Pulsar/NATS 的外部资源仅作参考，落地以「扩展本 Broker 既有官方客户端实测」为主。

---

## 10. 附录：资源链接汇总

- **Kafka 主选 conformance**：`github.com/faustbrian/golib/pkg/kafka/kafkatest`（`BrokerHarness`、`RunProducerConformance`、`RunConsumerConformance`、`RunTransactionConformance`）
- **Kafka 备选参考**：`github.com/aranw/kafkatest`（Docker + Redpanda，偏客户端）
- **MQTT Paho 互操作**：`https://github.com/eclipse-paho/paho.mqtt.testing`（`interoperability/client_test.py`、`client_test5.py`）
- **MQTT 线格式思路**：`https://github.com/mbilling/fss-mqtt-broker/blob/d28b28fbb6b7650fdcf957cab8600d689a99191c/docs/TEST-PLAN.md`（`crates/mqttd/tests/wire.rs`）
- **Pulsar 客户端内部测试（参考）**：`https://github.com/powersemmi/ruststream-pulsar/blob/c6d58b9e41cf2e4214360cb17d4783fee0c794de/crates/ruststream-pulsar/tests/conformance_pulsar.rs`
- **NATS nst（不稳定，参考）**：`@nats-io/nst`（官方声明 not a supported public API）
- **NATS 官方服务端测试（参考改写）**：`nats-server` 仓库 `test/test.go`
- **MQTT-SN（不建议）**：`com.mobius-software.mqttsn:mqttsn-testsuite`（7 年前 Maven 性能包）
- **MQTT-QUIC**：无公开套件

> 所有链接在落地前需再次确认存活与版本；涉及 `git clone` 的资源一律 **pinned commit**，避免上游漂移导致 CI 不稳定。
