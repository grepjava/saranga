<p align="center">
  <img src="assets/saranga.png" alt="Saranga: a pure-Rust message broker" width="600">
</p>

# Saranga

Saranga is a message broker written in Rust, inspired by [Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) 2.57.0. It is a thought experiment with the goals below. It speaks the same wire protocols as Artemis: Core, AMQP 1.0, OpenWire, STOMP, MQTT 3.1.1 and 5, and WebSockets. It reads and writes the same journal, bindings, paging and large-message files, and takes the same `broker.xml`.

It is in Phase 11 (hardening and 1.0) of the plan in [PHASES.md](PHASES.md). [COMPAT.md](COMPAT.md) has the pinned version and the deliberate differences.

The name: Śārṅga, the celestial bow of the Sustainer, keeper of the world’s balance. Across another sky, Artemis, the Greek huntress, draws her bow. Two mythologies, one enduring symbol of grace and power.

## Goals

- **Compatibility with Artemis.** The wire protocols, `broker.xml`, the journal, paging and large-message formats, management and the command line all behave as Artemis 2.57.0's do, so that its clients, tools and data files work against Saranga unchanged.
- **High availability and clustering alongside Artemis.** A Saranga broker can be an Artemis broker's backup, with a shared store, replication or a pluggable quorum, or its primary, and the two fail over and fail back between each other. Mixed clusters of Artemis and Saranga nodes load-balance, redistribute and group messages, and federation, bridges, broker connections and mirroring work in both directions. Nodes can therefore be moved from one broker to the other one at a time, with no downtime.
- **Native speed.** It compiles to a single native binary, with no JVM to start, warm up or tune. It starts in about 150 ms against Artemis's 2.7 s, and its throughput is the same or higher in every benchmark scenario.
- **Memory safety without a garbage collector.** It is written in Rust, so the compiler rules out use-after-free, data races and buffer overruns, with no collector pausing the broker. `unsafe` code is confined to a few operating-system calls: file locks, disk space, local time and the client's terminal.
- **Less CPU and memory for the same work.** At the same message rate it takes less broker CPU a message than Artemis in 12 of the 13 benchmark scenarios, and 40 to 57% less with persistent and large messages. It starts in 36 MiB rather than 209, and holds a twentieth to a fifth of Artemis's memory on OpenWire, STOMP and MQTT.
- **Small enough for the edge.** A small footprint and a fast start let it run on edge gateways, small devices and constrained networks where a JVM broker does not fit.
- **Savings in the cloud.** Less CPU and memory a message means smaller instances, more brokers a node and lower bills for the same traffic.

## Building

    cargo build --release -p saranga-cli

The binary is `target/release/saranga`. `saranga create <dir>` makes an instance as `artemis create` does, and `<dir>/bin/artemis run` starts it.

## Performance against Artemis

Both brokers ran on the same machine, with the same clients, in instances made by their own `create`, each on an empty NIO journal with `journal-datasync` on. The machine was an i5-8250U: the broker had two cores and their hyperthreads, and the client the other two. Messages are 1 KiB except in the large-message rows, which are 1 MiB.

| | Artemis | Saranga |
|---|---|---|
| Startup | 2,717 ms | 145 ms |
| Memory when started | 209 MiB | 36 MiB |

**Throughput (messages a second) and peak memory** with an unbounded producer, over 60 s after a 15 s warmup:

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|
| Core | 74,878 | 75,891 | 1.01 | 601 MiB | 881 MiB |
| Core, persistent | 64,674 | 72,621 | 1.12 | 576 MiB | 288 MiB |
| Core, 1 MiB persistent | 61 | 78 | 1.28 | 576 MiB | 81 MiB |
| AMQP | 30,961 | 33,155 | 1.07 | 2,238 MiB | 1,577 MiB |
| AMQP, persistent | 27,307 | 31,645 | 1.16 | 591 MiB | 1,116 MiB |
| AMQP, 1 MiB persistent | 95 | 104 | 1.09 | 809 MiB | 197 MiB |
| OpenWire | 49,575 | 52,375 | 1.06 | 719 MiB | 65 MiB |
| OpenWire, persistent | 283 | 634 | 2.24 | 586 MiB | 45 MiB |
| STOMP | 87,287 | 99,570 | 1.14 | 619 MiB | 50 MiB |
| STOMP, persistent | 70,023 | 78,592 | 1.12 | 651 MiB | 118 MiB |
| MQTT, QoS 0 | 91,683 | 121,195 | 1.32 | 583 MiB | 51 MiB |
| MQTT, QoS 1 | 43,183 | 90,555 | 2.10 | 2,340 MiB | 105 MiB |

**Broker CPU a message and latency (p50 / p99, send to receipt)** with both brokers at the same rate, half the lower of their two throughputs:

| Scenario | Rate, msg/s | Artemis CPU a message | Saranga CPU a message | Artemis p50 / p99 | Saranga p50 / p99 |
|---|---|---|---|---|---|
| Core | 37,439 | 64 µs | 59 µs | 139 µs / 667 µs | 156 µs / 343 µs |
| Core, persistent | 32,337 | 77 µs | 43 µs | 4.1 ms / 15.0 ms | 1.2 ms / 9.1 ms |
| Core, 1 MiB persistent | 30 | 19.4 ms | 10.5 ms | 22.9 ms / 41.2 ms | 17.8 ms / 30.3 ms |
| Core, 100 queues, persistent | 10,000 | 188 µs | 144 µs | 3.2 ms / 7.8 ms | 1.1 ms / 4.2 ms |
| AMQP | 15,480 | 81 µs | 76 µs | 129 µs / 329 µs | 133 µs / 281 µs |
| AMQP, persistent | 13,653 | 111 µs | 64 µs | 3.6 ms / 41.2 ms | 1.6 ms / 36.6 ms |
| AMQP, 1 MiB persistent | 47 | 16.2 ms | 9.3 ms | 22.1 ms / 40.2 ms | 20.2 ms / 32.8 ms |
| OpenWire | 24,787 | 117 µs | 92 µs | 182 µs / 1.6 ms | 245 µs / 2.6 ms |
| OpenWire, persistent | 141 | 3.6 ms | 1.9 ms | 4.5 ms / 6.7 ms | 2.5 ms / 2.8 ms |
| STOMP | 43,643 | 43 µs | 38 µs | 80 µs / 738 µs | 124 µs / 1.0 ms |
| STOMP, persistent | 35,011 | 65 µs | 39 µs | 4.2 ms / 13.2 ms | 1.3 ms / 11.7 ms |
| MQTT, QoS 0 | 45,841 | 40 µs | 39 µs | 83 µs / 631 µs | 116 µs / 289 µs |
| MQTT, QoS 1 | 21,591 | 115 µs | 49 µs | 2.9 ms / 18.4 ms | 855 µs / 6.6 ms |

Saranga is behind on latency without persistence: p50 is 4 to 63 µs higher, and p99 is higher on OpenWire and STOMP.

Its peak memory is higher on non-persistent Core (881 against 601 MiB) and persistent AMQP (1,116 against 591 MiB). This is not memory per message, which is about 2 KB in both. It is a backlog that the benchmark's client leaves behind.
- **The client is the limit.** Its producer and consumer share the client's four CPUs, and in these runs the client used all of them.
- **Saranga takes sends faster than that client can receive them.** The producer then gets more of the client's CPU, its consumer falls behind, and the difference waits in the broker.
- **Artemis takes sends no faster than the client receives**, so nothing accumulates.
- **The client does the same work for either broker:** 74 µs of client CPU a message with Artemis and 75 with Saranga, at 40,000 msg/s.

Saranga still delivers more messages a second than Artemis in each of these runs:

| Scenario | Broker | Sent/s | Received/s | Backlog grows by |
|---|---|---|---|---|
| Core | Artemis | 74,879 | 74,878 | 0/s |
| Core | Saranga | 82,280 | 75,891 | 6,389/s |
| AMQP, persistent | Artemis | 27,307 | 27,307 | 0/s |
| AMQP, persistent | Saranga | 37,655 | 31,645 | 6,009/s |

Over a 75 s run, that comes to about 430,000 messages held at the peak.

Runs on this machine vary by up to 5%. The method, the machine and how to run [scripts/bench.sh](scripts/bench.sh) yourself are in [docs/benchmarks.md](docs/benchmarks.md).

## Licence

Apache License 2.0; see [LICENSE](LICENSE).
