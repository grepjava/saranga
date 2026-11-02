<p align="center">
  <img src="assets/saranga.svg" alt="Saranga: a pure-Rust message broker" width="600">
</p>

# Saranga

Saranga is a message broker written in Rust, inspired by [Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) 2.57.0, and built to work interchangeably with it. It speaks the same wire protocols as Artemis: Core, AMQP 1.0, OpenWire, STOMP, MQTT 3.1.1 and 5, and WebSockets. It reads and writes the same journal, bindings, paging and large-message files, and takes the same `broker.xml`.

It is in Phase 11 (hardening and 1.0) of the plan in [PHASES.md](https://github.com/grepjava/saranga/blob/main/PHASES.md). [COMPAT.md](COMPAT.md) has the pinned version and the deliberate differences. The [user guide](docs/guide/README.md) covers installing, configuring and running it, and migrating from Artemis.

## Why Saranga?

Saranga is a high-performance, memory-safe message broker built in Rust, designed to work as Apache ActiveMQ Artemis works, with the efficiency, reliability and operational simplicity of a native application.

- **Seamless Artemis Compatibility.** Saranga targets full compatibility with Apache ActiveMQ Artemis 2.57.0, preserving wire protocols, `broker.xml` configuration, journal, paging and large-message formats, management interfaces and command-line tooling. Existing Artemis clients, tools, configurations and data files are intended to work unchanged; [COMPAT.md](COMPAT.md) lists where Saranga still differs. Saranga's compatibility roadmap tracks upstream Artemis releases to maintain interoperability as the platform evolves.
- **High Availability and Clustering Across Both Brokers.** Saranga is designed to operate interchangeably with Artemis in high-availability configurations, serving as either primary or backup through shared storage, replication or pluggable quorum mechanisms. Mixed Artemis–Saranga clusters support message load balancing, redistribution, grouping, federation, bridges, broker connections and bidirectional mirroring. This enables incremental, node-by-node migration without planned downtime, while preserving failover and failback capabilities.
- **Native Performance, Without JVM Overhead.** Compiled into a standalone native executable, Saranga eliminates JVM startup, warm-up and tuning requirements. Benchmarks show startup times of approximately 140 ms versus 2.7 seconds for Artemis—about 19 times faster—with throughput matching or exceeding Artemis across all tested scenarios.
- **Memory Safety Without Garbage Collection.** Built in Rust, Saranga uses compile-time ownership, borrowing and type-safety guarantees to prevent common memory-safety defects and data races in safe code. With no garbage collector, it also avoids garbage-collection pauses. The limited use of `unsafe` is isolated to operating-system integrations, including file locking, disk-space queries, local-time handling and terminal operations.
- **Greater Resource Efficiency.** Saranga uses less broker CPU per message in all 13 benchmark scenarios, with CPU reductions of 41–58% for persistent and large-message workloads. Its initial memory footprint is approximately 42 MiB compared with Artemis's 209 MiB, while OpenWire, STOMP and MQTT workloads consume only 5–20% of Artemis's memory in the measured scenarios. This translates into greater workload density and more efficient infrastructure utilization.
- **Engineered for Edge Deployments.** With a compact memory footprint, rapid startup and no JVM dependency, Saranga extends enterprise-grade messaging to edge gateways, resource-constrained devices and distributed environments where conventional JVM-based brokers may be impractical.
- **Lower Cloud Infrastructure Costs.** Reduced CPU and memory consumption enables smaller compute instances, higher broker density per host and more efficient container deployments. For equivalent messaging workloads, these improvements create opportunities to reduce infrastructure spending without sacrificing protocol compatibility or throughput.

Saranga combines the established messaging capabilities of Apache ActiveMQ Artemis with Rust's memory safety, native execution and resource efficiency—providing a practical path to modernize messaging infrastructure without abandoning existing Artemis investments.

## Building

    cargo build --release -p saranga-cli

The binary is `target/release/saranga`. `saranga create <dir>` makes an instance as `artemis create` does, and `<dir>/bin/artemis run` starts it.

## The Saranga console

Saranga has a web console of its own at `http://localhost:8161/saranga/`, built into the binary, so it needs no `console.war`, no JVM and no Node.js at run time. It takes the same users, console roles and permissions as Artemis's console, and every call goes through the same guard and audit as Jolokia.

- **What Artemis's console does.** The overview, the connections, sessions, consumers, producers, addresses and queues, with closes, creates and deletes. Browsing a queue's messages and moving, copying, retrying, resending or deleting them. Sending a message. The explorer of every management resource, the diagram and the preferences.
- **Live data.** The pages follow the broker over one server-sent-events stream instead of polling, the charts open on the last hour of the broker's own history, and the events page shows the management notifications as they come.
- **A typed API** at `/saranga/api/v1`, described as OpenAPI 3.1 at `/saranga/api/v1/openapi.json`.

- **The cluster and the broker's administration.** Every node of the cluster, Artemis or Saranga, with high availability and bridges; address settings, diverts, acceptors, users and security settings, XA transactions and a dead-letter workbench.
- **Accessible, and at a phone's width.** Every page is checked against WCAG 2.1 AA.

It is in Saranga 1.1.0 and later; [docs/console-design.md](docs/console-design.md) records how it was built. To include it in a build from source, run `npm ci && npm run build` in `console/` (Node.js is needed only for this step) before `cargo build`; without that, `/saranga/` serves a page that says how. [Management](docs/guide/management.md#the-saranga-console) in the guide describes it in full. Artemis's own console can still be served alongside it.

## Performance against Artemis

Both brokers ran on the same machine, with the same clients, in instances made by their own `create`, each on an empty NIO journal with `journal-datasync` on. Messages are 1 KiB except in the large-message rows, which are 1 MiB.

The benchmark machine:

|                                |                                                   |
| ------------------------------ | ------------------------------------------------- |
| CPU                            | Intel Core i5-8250U, 4 cores, 8 threads           |
| CPUs for the broker            | 0, 1, 4 and 5 (two cores and their hyperthreads)  |
| CPUs for the client            | 2, 3, 6 and 7                                     |
| Memory                         | 3.3 GiB                                           |
| Storage                        | SATA SSD, ext4                                    |
| Operating system               | Linux, kernel 7.3, dedicated to the run           |
| Java (Artemis and the clients) | OpenJDK 25.0.4.1, Artemis with a 2 GiB heap       |
| Versions                       | Artemis 2.57.0, Saranga at `40329ba` (2026-11-02) |

|                     | Artemis  | Saranga |
| ------------------- | -------- | ------- |
| Startup             | 2,706 ms | 143 ms  |
| Memory when started | 209 MiB  | 42 MiB  |

**Throughput (messages a second) and peak memory** with an unbounded producer, over 60 s after a 15 s warmup:

| Scenario               | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Artemis peak RSS | Saranga peak RSS |
| ---------------------- | ------------- | ------------- | --------------- | ---------------- | ---------------- |
| Core                   | 74,607        | 80,232        | 1.08            | 605 MiB          | 663 MiB          |
| Core, persistent       | 65,077        | 73,589        | 1.13            | 625 MiB          | 403 MiB          |
| Core, 1 MiB persistent | 60            | 78            | 1.30            | 562 MiB          | 88 MiB           |
| AMQP                   | 31,344        | 31,682        | 1.01            | 2,233 MiB        | 1,617 MiB        |
| AMQP, persistent       | 27,067        | 30,919        | 1.14            | 586 MiB          | 1,156 MiB        |
| AMQP, 1 MiB persistent | 99            | 106           | 1.07            | 813 MiB          | 191 MiB          |
| OpenWire               | 49,977        | 51,716        | 1.03            | 628 MiB          | 71 MiB           |
| OpenWire, persistent   | 285           | 642           | 2.25            | 588 MiB          | 53 MiB           |
| STOMP                  | 88,556        | 106,239       | 1.20            | 688 MiB          | 64 MiB           |
| STOMP, persistent      | 68,863        | 83,307        | 1.21            | 629 MiB          | 124 MiB          |
| MQTT, QoS 0            | 91,019        | 118,634       | 1.30            | 581 MiB          | 60 MiB           |
| MQTT, QoS 1            | 44,531        | 94,725        | 2.13            | 2,343 MiB        | 114 MiB          |

**Broker CPU a message and latency (p50 / p99, send to receipt)** with both brokers at the same rate, half the lower of their two throughputs:

| Scenario                     | Rate, msg/s | Artemis CPU a message | Saranga CPU a message | Artemis p50 / p99 | Saranga p50 / p99 |
| ---------------------------- | ----------- | --------------------- | --------------------- | ----------------- | ----------------- |
| Core                         | 37,303      | 64 µs                 | 43 µs                 | 141 µs / 607 µs   | 138 µs / 295 µs   |
| Core, persistent             | 32,538      | 76 µs                 | 41 µs                 | 4.1 ms / 11.6 ms  | 1.2 ms / 9.9 ms   |
| Core, 1 MiB persistent       | 30          | 19.5 ms               | 10.3 ms               | 22.9 ms / 39.2 ms | 17.9 ms / 29.3 ms |
| Core, 100 queues, persistent | 10,000      | 237 µs                | 108 µs                | 1.1 ms / 4.5 ms   | 995 µs / 2.8 ms   |
| AMQP                         | 15,672      | 81 µs                 | 71 µs                 | 128 µs / 391 µs   | 129 µs / 295 µs   |
| AMQP, persistent             | 13,533      | 116 µs                | 64 µs                 | 3.6 ms / 26.9 ms  | 1.6 ms / 34.0 ms  |
| AMQP, 1 MiB persistent       | 49          | 16.4 ms               | 9.2 ms                | 23.3 ms / 37.6 ms | 20.0 ms / 33.8 ms |
| OpenWire                     | 24,988      | 112 µs                | 89 µs                 | 188 µs / 1.4 ms   | 244 µs / 2.7 ms   |
| OpenWire, persistent         | 142         | 3.6 ms                | 1.9 ms                | 4.5 ms / 6.6 ms   | 2.5 ms / 2.8 ms   |
| STOMP                        | 44,278      | 44 µs                 | 37 µs                 | 82 µs / 551 µs    | 117 µs / 786 µs   |
| STOMP, persistent            | 34,431      | 64 µs                 | 38 µs                 | 4.1 ms / 12.4 ms  | 1.3 ms / 10.6 ms  |
| MQTT, QoS 0                  | 45,509      | 41 µs                 | 37 µs                 | 85 µs / 393 µs    | 110 µs / 238 µs   |
| MQTT, QoS 1                  | 22,265      | 112 µs                | 47 µs                 | 2.9 ms / 12.4 ms  | 862 µs / 7.0 ms   |

Saranga is behind on latency in some scenarios without persistence: p50 is 25 to 56 µs higher on OpenWire, STOMP and MQTT (level on Core and AMQP), and p99 is higher on OpenWire and STOMP. Persistent AMQP's p99 is higher in this run too (34.0 against 26.9 ms); it moves a lot from run to run, and another run the same morning gave Saranga 6.9 ms and Artemis 36.6 ms.

Its peak memory is higher on non-persistent Core (663 against 605 MiB) and persistent AMQP (1,156 against 586 MiB). This is not memory per message, which is about 2 KB in both. It is a backlog that the benchmark's client leaves behind.

- **The client is the limit.** Its producer and consumer share the client's four CPUs, and in these runs the client used all of them.
- **Saranga takes sends faster than that client can receive them.** The producer then gets more of the client's CPU, its consumer falls behind, and the difference waits in the broker.
- **In these two scenarios, Artemis takes sends no faster than the client receives them**, so nothing accumulates.

Saranga still delivers more messages a second than Artemis in each of these runs:

| Scenario         | Broker  | Sent/s | Received/s | Backlog grows by |
| ---------------- | ------- | ------ | ---------- | ---------------- |
| Core             | Artemis | 74,608 | 74,607     | 0/s              |
| Core             | Saranga | 84,941 | 80,232     | 4,709/s          |
| AMQP, persistent | Artemis | 27,068 | 27,067     | 0/s              |
| AMQP, persistent | Saranga | 36,997 | 30,919     | 6,078/s          |

Over a 75 s run, that comes to about 350,000 to 460,000 messages held at the peak.

Runs on this machine vary by up to 5%. The method, the machine and how to run [scripts/bench.sh](https://github.com/grepjava/saranga/blob/main/scripts/bench.sh) yourself are in [docs/benchmarks.md](docs/benchmarks.md).

## Licence

Apache License 2.0; see [LICENSE](LICENSE).
