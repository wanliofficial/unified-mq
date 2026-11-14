# RabbitMQ Compatibility and Feature Gap Requirement Specification

## 1. Overview
This document outlines the architectural and functional feature gaps identified between `unified-mq` (an Erlang-based unified messaging/compatibility layer) and the official production-grade `rabbitmq-server`. It translates these gaps into actionable engineering requirements to help evaluate or upgrade the system's compatibility with standard AMQP 0-9-1 protocols and enterprise-grade operational features.

---

## 2. Requirement Categories

### Epic 1: AMQP 0-9-1 Protocol Completeness
**Goal:** Move beyond basic pub/sub subsets to achieve comprehensive compliance with the AMQP 0-9-1 specification, ensuring seamless integration with standard enterprise RabbitMQ client SDKs.

*   **REQ-AMQP-01: Full Protocol State Machine Support**
    *   *Description:* Implement complete lifecycle state machines for Connections and Channels.
    *   *Acceptance Criteria:* Support advanced control and negotiation frames including `Connection.Tune`, `Connection.Blocked`, `Channel.Flow`, and `Basic.Recover`.
*   **REQ-AMQP-02: Advanced Exchange Types & Routing**
    *   *Description:* Expand routing logic beyond basic Topic and Direct exchanges.
    *   *Acceptance Criteria:* Implement support for `Headers` exchange (matching headers via `x-match` arguments) and `Alternate Exchanges` (`alternate-exchange`) for unroutable messages.
*   **REQ-AMQP-03: Publisher Confirms and Transactions**
    *   *Description:* Ensure reliable message delivery tracking from publisher to broker.
    *   *Acceptance Criteria:* Fully support `Confirm.Select` mode with asynchronous `Basic.Ack`/`Basic.Nack` callbacks, as well as AMQP transactional (`Tx`) support.

### Epic 2: Distributed Consistency & Metadata Management
**Goal:** Enhance cluster coordination and metadata replication to match the resilience and consistency standards of enterprise messaging infrastructure.

*   **REQ-DIST-01: Distributed Consensus & Replication (Quorum Queues)**
    *   *Description:* Implement robust multi-node data replication.
    *   *Acceptance Criteria:* Support Raft-based consensus (or equivalent high-availability queue replication) to prevent message loss during node failures.
*   **REQ-DIST-02: Network Partition Handling (Split-Brain Recovery)**
    *   *Description:* Provide automated strategies for handling cluster network partitions.
    *   *Acceptance Criteria:* Implement configurable partition handling modes (e.g., `ignore`, `pause_minority`, `autoheal`).
*   **REQ-DIST-03: Dynamic Virtual Host & Permission Isolation**
    *   *Description:* Enforce fine-grained security and multi-tenancy.
    *   *Acceptance Criteria:* Support dynamic vhost creation, user management, and fine-grained resource permissions (configure, read, write) aligned with RabbitMQ access control models.

### Epic 3: Production-Grade Flow Control & Resource Management
**Goal:** Protect the broker against memory exhaustion, high-pressure queue backlogs, and runaway producers.

*   **REQ-RES-01: Memory High Watermark & Disk Alarm Integration**
    *   *Description:* Monitor node resource thresholds proactively.
    *   *Acceptance Criteria:* Implement automatic monitoring of system memory and available disk space, triggering alarms and blocking publishing connections when thresholds (e.g., 40% RAM usage) are exceeded.
*   **REQ-RES-02: Credit-Based Flow Control**
    *   *Description:* Prevent fast producers from overwhelming slow consumers or internal buffers.
    *   *Acceptance Criteria:* Implement credit-based backpressure mechanisms between connections, channels, and internal worker processes.
*   **REQ-RES-03: Paging & Lazy Queues**
    *   *Description:* Handle massive message backlogs without triggering Out-Of-Memory (OOM) crashes.
    *   *Acceptance Criteria:* Automatically page messages off to disk when memory pressure rises, or support lazy queues where messages are immediately written to disk.

### Epic 4: Ecosystem & Extensibility
**Goal:** Provide operational visibility and administrative tools expected by operators managing AMQP brokers.

*   **REQ-ECO-01: Management HTTP API Compatibility**
    *   *Description:* Expose management endpoints for monitoring and automation.
    *   *Acceptance Criteria:* Provide an HTTP REST API compatible with the standard RabbitMQ Management Plugin (e.g., querying queues, exchanges, connections, and message rates).
*   **REQ-ECO-02: Plugin Extension Hook Framework**
    *   *Description:* Allow modular feature additions.
    *   *Acceptance Criteria:* Establish a stable callback/plugin architecture enabling custom auth backends, routing plugins, or telemetry exporters.