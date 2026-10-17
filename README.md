# Saranga

Saranga is a message broker written in Rust that aims to be a drop-in replacement for [Apache ActiveMQ Artemis](https://activemq.apache.org/components/artemis/) 2.57.0. It speaks the same wire protocols: Core, AMQP 1.0, OpenWire, STOMP, MQTT 3.1.1 and 5, and WebSockets. It reads and writes the same journal, bindings, paging and large-message files, and takes the same `broker.xml`, so that Artemis's clients, management and command line work against it unchanged.

It is in Phase 11 (hardening and 1.0) of the plan in [PHASES.md](PHASES.md). [COMPAT.md](COMPAT.md) has the pinned version and the deliberate differences.

## Building

    cargo build --release -p saranga-cli

The binary is `target/release/saranga`. `saranga create <dir>` makes an instance as `artemis create` does, and `<dir>/bin/artemis run` starts it.

## Performance against Artemis

Both brokers ran on the same machine, with the same clients, in instances made by their own `create`, each on an empty NIO journal with `journal-datasync` on. The machine was an i5-8250U: the broker had two cores and their hyperthreads, and the client the other two. Messages are 1 KiB except in the large-message rows, which are 1 MiB.

| | Artemis | Saranga |
|---|---|---|
| Startup | 2,670 ms | 169 ms |
| Memory when started | 207 MiB | 36 MiB |

**Throughput (messages a second) and peak memory** with an unbounded producer, over 60 s after a 15 s warmup:

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|
| Core | 72,064 | 71,061 | 0.99 | 578 MiB | 813 MiB |
| Core, persistent | 62,247 | 67,705 | 1.09 | 634 MiB | 135 MiB |
| Core, 1 MiB persistent | 58 | 78 | 1.34 | 579 MiB | 79 MiB |
| AMQP | 30,223 | 31,354 | 1.04 | 2,213 MiB | 1,579 MiB |
| AMQP, persistent | 26,245 | 28,748 | 1.10 | 574 MiB | 1,394 MiB |
| AMQP, 1 MiB persistent | 96 | 105 | 1.09 | 810 MiB | 199 MiB |
| OpenWire | 48,811 | 48,040 | 0.98 | 638 MiB | 62 MiB |
| OpenWire, persistent | 284 | 642 | 2.26 | 561 MiB | 45 MiB |
| STOMP | 91,408 | 77,015 | 0.84 | 747 MiB | 57 MiB |
| STOMP, persistent | 68,477 | 63,507 | 0.93 | 636 MiB | 115 MiB |
| MQTT, QoS 0 | 93,674 | 103,608 | 1.11 | 591 MiB | 51 MiB |
| MQTT, QoS 1 | 44,478 | 77,255 | 1.74 | 2,314 MiB | 106 MiB |

**Broker CPU a message and latency (p50 / p99, send to receipt)** with both brokers at the same rate, half the lower of their two throughputs:

| Scenario | Rate, msg/s | Artemis CPU a message | Saranga CPU a message | Artemis p50 / p99 | Saranga p50 / p99 |
|---|---|---|---|---|---|
| Core | 35,530 | 65 µs | 66 µs | 141 µs / 631 µs | 173 µs / 427 µs |
| Core, persistent | 31,123 | 79 µs | 53 µs | 4.1 ms / 13.4 ms | 1.2 ms / 13.6 ms |
| Core, 1 MiB persistent | 29 | 20.1 ms | 10.4 ms | 23.2 ms / 41.5 ms | 18.0 ms / 30.1 ms |
| Core, 100 queues, persistent | 10,000 | 191 µs | 152 µs | 3.2 ms / 7.3 ms | 1.1 ms / 2.5 ms |
| AMQP | 15,111 | 84 µs | 78 µs | 134 µs / 423 µs | 140 µs / 289 µs |
| AMQP, persistent | 13,122 | 115 µs | 70 µs | 3.6 ms / 18.7 ms | 1.6 ms / 36.9 ms |
| AMQP, 1 MiB persistent | 48 | 16.2 ms | 9.2 ms | 22.9 ms / 38.9 ms | 20.1 ms / 33.5 ms |
| OpenWire | 24,020 | 119 µs | 91 µs | 181 µs / 1.5 ms | 274 µs / 2.9 ms |
| OpenWire, persistent | 142 | 3.6 ms | 1.9 ms | 4.5 ms / 6.7 ms | 2.5 ms / 2.9 ms |
| STOMP | 38,507 | 42 µs | 57 µs | 75 µs / 501 µs | 130 µs / 1.3 ms |
| STOMP, persistent | 31,753 | 67 µs | 51 µs | 4.0 ms / 12.8 ms | 1.3 ms / 12.4 ms |
| MQTT, QoS 0 | 46,837 | 42 µs | 37 µs | 86 µs / 694 µs | 141 µs / 507 µs |
| MQTT, QoS 1 | 22,239 | 113 µs | 53 µs | 2.9 ms / 26.0 ms | 860 µs / 9.8 ms |

Saranga is behind in three places:
- **Non-persistent STOMP**, on throughput and CPU.
- **p50 without persistence**, by 6 to 93 µs.
- **Peak memory where a backlog built up during the run**: non-persistent Core and Saranga's persistent AMQP.

Runs on this machine vary by up to 5%. The method, the machine and how to run [scripts/bench.sh](scripts/bench.sh) yourself are in [docs/benchmarks.md](docs/benchmarks.md).

## Licence

Apache License 2.0; see [LICENSE](LICENSE).
