<p align="center">
  <img src="assets/saranga.svg" alt="Saranga: a pure-Rust message broker" width="600">
</p>

# Saranga

Saranga is a message broker written in Rust, inspired by [Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) 2.57.0, and built to work interchangeably with it. It speaks the same wire protocols as Artemis: Core, AMQP 1.0, OpenWire, STOMP, MQTT 3.1.1 and 5, and WebSockets. It reads and writes the same journal, bindings, paging and large-message files, and takes the same `broker.xml`.

It is in Phase 11 (hardening and 1.0) of the plan in [PHASES.md](https://github.com/grepjava/saranga/blob/main/PHASES.md). [COMPAT.md](COMPAT.md) has the pinned version and the deliberate differences. The [user guide](docs/guide/README.md) covers installing, configuring and running it, and migrating from Artemis.

## Why Saranga?

Saranga is a high-performance, memory-safe message broker built in Rust, designed to deliver full Apache ActiveMQ Artemis compatibility with the efficiency, reliability and operational simplicity of a native application.

- **Seamless Artemis Compatibility.** Saranga targets full compatibility with Apache ActiveMQ Artemis 2.57.0, preserving wire protocols, `broker.xml` configuration, journal, paging and large-message formats, management interfaces and command-line tooling. Existing Artemis clients, tools, configurations and data files are intended to work unchanged. Saranga's compatibility roadmap tracks upstream Artemis releases to maintain interoperability as the platform evolves.
- **High Availability and Clustering Across Both Brokers.** Saranga is designed to operate interchangeably with Artemis in high-availability configurations, serving as either primary or backup through shared storage, replication or pluggable quorum mechanisms. Mixed Artemis–Saranga clusters support message load balancing, redistribution, grouping, federation, bridges, broker connections and bidirectional mirroring. This enables incremental, node-by-node migration without planned downtime, while preserving failover and failback capabilities.
- **Native Performance, Without JVM Overhead.** Compiled into a standalone native executable, Saranga eliminates JVM startup, warm-up and tuning requirements. Benchmarks show startup times of approximately 150 ms versus 2.7 seconds for Artemis—an 18× improvement—with throughput matching or exceeding Artemis across all tested scenarios.
- **Memory Safety Without Garbage Collection.** Built in Rust, Saranga uses compile-time ownership, borrowing and type-safety guarantees to prevent common memory-safety defects and data races in safe code. With no garbage collector, it also avoids garbage-collection pauses. The limited use of `unsafe` is isolated to operating-system integrations, including file locking, disk-space queries, local-time handling and terminal operations.
- **Greater Resource Efficiency.** Saranga uses less broker CPU per message in 12 of 13 benchmark scenarios, with CPU reductions of 40–57% for persistent and large-message workloads. Its initial memory footprint is approximately 36 MiB compared with Artemis's 209 MiB, while OpenWire, STOMP and MQTT workloads consume only 5–20% of Artemis's memory in the measured scenarios. This translates into greater workload density and more efficient infrastructure utilization.
- **Engineered for Edge Deployments.** With a compact memory footprint, rapid startup and no JVM dependency, Saranga extends enterprise-grade messaging to edge gateways, resource-constrained devices and distributed environments where conventional JVM-based brokers may be impractical.
- **Lower Cloud Infrastructure Costs.** Reduced CPU and memory consumption enables smaller compute instances, higher broker density per host and more efficient container deployments. For equivalent messaging workloads, these improvements create opportunities to reduce infrastructure spending without sacrificing protocol compatibility or throughput.

Saranga combines the established messaging capabilities of Apache ActiveMQ Artemis with Rust's memory safety, native execution and resource efficiency—providing a practical path to modernize messaging infrastructure without abandoning existing Artemis investments.

## Building

    cargo build --release -p saranga-cli

The binary is `target/release/saranga`. `saranga create <dir>` makes an instance as `artemis create` does, and `<dir>/bin/artemis run` starts it.

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
| Operating system               | Linux, kernel 7.0, dedicated to the run           |
| Java (Artemis and the clients) | OpenJDK 25.0.4.1, Artemis with a 2 GiB heap       |
| Versions                       | Artemis 2.57.0, Saranga at `bc9efdd` (2026-10-17) |

|                     | Artemis  | Saranga |
| ------------------- | -------- | ------- |
| Startup             | 2,717 ms | 145 ms  |
| Memory when started | 209 MiB  | 36 MiB  |

**Throughput (messages a second) and peak memory** with an unbounded producer, over 60 s after a 15 s warmup:

| Scenario               | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Artemis peak RSS | Saranga peak RSS |
| ---------------------- | ------------- | ------------- | --------------- | ---------------- | ---------------- |
| Core                   | 74,878        | 75,891        | 1.01            | 601 MiB          | 881 MiB          |
| Core, persistent       | 64,674        | 72,621        | 1.12            | 576 MiB          | 288 MiB          |
| Core, 1 MiB persistent | 61            | 78            | 1.28            | 576 MiB          | 81 MiB           |
| AMQP                   | 30,961        | 33,155        | 1.07            | 2,238 MiB        | 1,577 MiB        |
| AMQP, persistent       | 27,307        | 31,645        | 1.16            | 591 MiB          | 1,116 MiB        |
| AMQP, 1 MiB persistent | 95            | 104           | 1.09            | 809 MiB          | 197 MiB          |
| OpenWire               | 49,575        | 52,375        | 1.06            | 719 MiB          | 65 MiB           |
| OpenWire, persistent   | 283           | 634           | 2.24            | 586 MiB          | 45 MiB           |
| STOMP                  | 87,287        | 99,570        | 1.14            | 619 MiB          | 50 MiB           |
| STOMP, persistent      | 70,023        | 78,592        | 1.12            | 651 MiB          | 118 MiB          |
| MQTT, QoS 0            | 91,683        | 121,195       | 1.32            | 583 MiB          | 51 MiB           |
| MQTT, QoS 1            | 43,183        | 90,555        | 2.10            | 2,340 MiB        | 105 MiB          |

**Broker CPU a message and latency (p50 / p99, send to receipt)** with both brokers at the same rate, half the lower of their two throughputs:

| Scenario                     | Rate, msg/s | Artemis CPU a message | Saranga CPU a message | Artemis p50 / p99 | Saranga p50 / p99 |
| ---------------------------- | ----------- | --------------------- | --------------------- | ----------------- | ----------------- |
| Core                         | 37,439      | 64 µs                 | 59 µs                 | 139 µs / 667 µs   | 156 µs / 343 µs   |
| Core, persistent             | 32,337      | 77 µs                 | 43 µs                 | 4.1 ms / 15.0 ms  | 1.2 ms / 9.1 ms   |
| Core, 1 MiB persistent       | 30          | 19.4 ms               | 10.5 ms               | 22.9 ms / 41.2 ms | 17.8 ms / 30.3 ms |
| Core, 100 queues, persistent | 10,000      | 188 µs                | 144 µs                | 3.2 ms / 7.8 ms   | 1.1 ms / 4.2 ms   |
| AMQP                         | 15,480      | 81 µs                 | 76 µs                 | 129 µs / 329 µs   | 133 µs / 281 µs   |
| AMQP, persistent             | 13,653      | 111 µs                | 64 µs                 | 3.6 ms / 41.2 ms  | 1.6 ms / 36.6 ms  |
| AMQP, 1 MiB persistent       | 47          | 16.2 ms               | 9.3 ms                | 22.1 ms / 40.2 ms | 20.2 ms / 32.8 ms |
| OpenWire                     | 24,787      | 117 µs                | 92 µs                 | 182 µs / 1.6 ms   | 245 µs / 2.6 ms   |
| OpenWire, persistent         | 141         | 3.6 ms                | 1.9 ms                | 4.5 ms / 6.7 ms   | 2.5 ms / 2.8 ms   |
| STOMP                        | 43,643      | 43 µs                 | 38 µs                 | 80 µs / 738 µs    | 124 µs / 1.0 ms   |
| STOMP, persistent            | 35,011      | 65 µs                 | 39 µs                 | 4.2 ms / 13.2 ms  | 1.3 ms / 11.7 ms  |
| MQTT, QoS 0                  | 45,841      | 40 µs                 | 39 µs                 | 83 µs / 631 µs    | 116 µs / 289 µs   |
| MQTT, QoS 1                  | 21,591      | 115 µs                | 49 µs                 | 2.9 ms / 18.4 ms  | 855 µs / 6.6 ms   |

Saranga is behind on latency without persistence: p50 is 4 to 63 µs higher, and p99 is higher on OpenWire and STOMP.

Its peak memory is higher on non-persistent Core (881 against 601 MiB) and persistent AMQP (1,116 against 591 MiB). This is not memory per message, which is about 2 KB in both. It is a backlog that the benchmark's client leaves behind.

- **The client is the limit.** Its producer and consumer share the client's four CPUs, and in these runs the client used all of them.
- **Saranga takes sends faster than that client can receive them.** The producer then gets more of the client's CPU, its consumer falls behind, and the difference waits in the broker.
- **Artemis takes sends no faster than the client receives**, so nothing accumulates.
- **The client does the same work for either broker:** 74 µs of client CPU a message with Artemis and 75 with Saranga, at 40,000 msg/s.

Saranga still delivers more messages a second than Artemis in each of these runs:

| Scenario         | Broker  | Sent/s | Received/s | Backlog grows by |
| ---------------- | ------- | ------ | ---------- | ---------------- |
| Core             | Artemis | 74,879 | 74,878     | 0/s              |
| Core             | Saranga | 82,280 | 75,891     | 6,389/s          |
| AMQP, persistent | Artemis | 27,307 | 27,307     | 0/s              |
| AMQP, persistent | Saranga | 37,655 | 31,645     | 6,009/s          |

Over a 75 s run, that comes to about 430,000 messages held at the peak.

Runs on this machine vary by up to 5%. The method, the machine and how to run [scripts/bench.sh](https://github.com/grepjava/saranga/blob/main/scripts/bench.sh) yourself are in [docs/benchmarks.md](docs/benchmarks.md).

## Licence

Apache License 2.0; see [LICENSE](LICENSE).
