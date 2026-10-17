# Benchmarks

Saranga against Apache Artemis 2.57.0, on the same machine and in the same scenarios, made by [scripts/bench.sh](../scripts/bench.sh).

## Method

- **The same client for both.**
  - For Core and AMQP it is Artemis's own `artemis perf client`: the Core JMS client, or Qpid JMS for AMQP. A producer and a consumer on `queue://BENCH`, with up to 100 sends outstanding (`--max-pending 100`).
  - Artemis's perf client speaks no other protocol, so for OpenWire, STOMP and MQTT both brokers get [scripts/bench/Load.java](../scripts/bench/Load.java). It runs a producer and a consumer on one queue with 1 KiB messages, with at most 1,000 sent and not yet received.
    - OpenWire goes through the ActiveMQ 5 JMS client in Artemis's `lib`.
    - STOMP 1.2 and MQTT 3.1.1 go over a plain socket. The STOMP producer asks for a receipt every 100 sends, with up to 10 outstanding. The MQTT producer has up to 100 QoS 1 sends unacknowledged.
- **Instances made by each broker's own `create`**, with `--nio --no-autotune --no-web` and otherwise their defaults. Both use the NIO journal, which is the one Saranga writes, with `journal-datasync` on. Artemis has its default heap (`-Xms512M -Xmx2G`).
- **Separate cores.** The broker runs on half the CPUs and the client on the other half (`taskset`). Both brokers run from a copy on the same local file system, and every run starts on an empty data directory.
- **Throughput:** messages received a second with the producer unbounded. It is the mean of the per-second samples from the end of a 15 s warmup to the producer's last send, over 60 s of sending. The backlog a broker drains after the producer stops is not counted.
- **Latency:** both brokers run again at the same rate, half the lower of their two throughputs. The figure is the client's transfer time, from send to receipt, at p50 and p99.
- **CPU a message:** the broker's CPU time from three seconds after the warmup to the end of the run, divided by the messages received in that time. The table gives it from the latency run, where both brokers carry the same load.
- **Memory:** the broker's peak resident set (`VmHWM`) at the end of the throughput run.
- **Startup:** the time from launching `artemis run` until the acceptor takes a connection, the median of five starts. The resident memory is read once the broker is up.
- **Scenarios:**
  - `core`, `core-persistent`, `amqp` and `amqp-persistent` send 1 KiB messages.
  - `core-large` and `amqp-large` send persistent 1 MiB messages, which both brokers handle as large messages.
  - `many-queues` runs 100 persistent Core queues, each with its own producer and consumer, at 10,000 messages a second in all. Unbounded, 100 producers outrun their consumers and the run measures a backlog: Artemis stalled that way after a minute.
  - `openwire`, `stomp` and `mqtt` send non-persistent 1 KiB messages; for MQTT that means QoS 0.
  - `openwire-persistent`, `stomp-persistent` and `mqtt-persistent` send them persistent; for MQTT that means QoS 1. The OpenWire client sends a persistent message synchronously, so that scenario's rate is one journal sync's round trip.

## Results

Run on 2026-10-17. Core, AMQP, large messages, `many-queues` and OpenWire ran at Saranga commit `9333701`. STOMP and MQTT ran at `0b49448`, which changes only those two protocols, after the first run's STOMP scenarios failed against Artemis.
- **The STOMP failure:** the bench client's consumer sent nothing, so Artemis closed it at its 60 s TTL. The client now sends heart-beats.
- **The MQTT rerun:** Artemis's JVM aborted (SIGABRT) during the second run's `mqtt-persistent` throughput run, so that scenario ran a third time. Its figures match the first run's (Artemis 43,548 msg/s at 2,293 MiB peak, Saranga 76,957 at 110 MiB).

The machine was a dedicated Linux box (kernel 7.0) with an i5-8250U: four cores, eight threads. The broker had CPUs 0, 1, 4 and 5 (two cores and their hyperthreads), and the client the rest. It had 3.3 GiB of memory and ext4 on a SATA SSD. Both brokers and the clients ran on OpenJDK 25.0.4.1.

| Startup | Artemis | Saranga |
|---|---|---|
| Time to the acceptor (median of 5) | 2,670 ms | 169 ms |
| Resident memory when started | 207 MiB | 36 MiB |

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Latency at msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis CPU a message | Saranga CPU a message | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|---|---|---|---|---|
| core | 72,064 | 71,061 | 0.99 | 35,530 | 141 us / 631 us | 173 us / 427 us | 65 us | 66 us | 578 MiB | 813 MiB |
| core-persistent | 62,247 | 67,705 | 1.09 | 31,123 | 4.1 ms / 13.4 ms | 1.2 ms / 13.6 ms | 79 us | 53 us | 634 MiB | 135 MiB |
| amqp | 30,223 | 31,354 | 1.04 | 15,111 | 134 us / 423 us | 140 us / 289 us | 84 us | 78 us | 2,213 MiB | 1,579 MiB |
| amqp-persistent | 26,245 | 28,748 | 1.10 | 13,122 | 3.6 ms / 18.7 ms | 1.6 ms / 36.9 ms | 115 us | 70 us | 574 MiB | 1,394 MiB |
| core-large | 58 | 78 | 1.34 | 29 | 23.2 ms / 41.5 ms | 18.0 ms / 30.1 ms | 20.1 ms | 10.4 ms | 579 MiB | 79 MiB |
| amqp-large | 96 | 105 | 1.09 | 48 | 22.9 ms / 38.9 ms | 20.1 ms / 33.5 ms | 16.2 ms | 9.2 ms | 810 MiB | 199 MiB |
| many-queues | 9,995 | 9,999 | 1.00 | 10,000 | 3.2 ms / 7.3 ms | 1.1 ms / 2.5 ms | 191 us | 152 us | 603 MiB | 133 MiB |
| openwire | 48,811 | 48,040 | 0.98 | 24,020 | 181 us / 1.5 ms | 274 us / 2.9 ms | 119 us | 91 us | 638 MiB | 62 MiB |
| openwire-persistent | 284 | 642 | 2.26 | 142 | 4.5 ms / 6.7 ms | 2.5 ms / 2.9 ms | 3.6 ms | 1.9 ms | 561 MiB | 45 MiB |
| stomp | 91,408 | 77,015 | 0.84 | 38,507 | 75 us / 501 us | 130 us / 1.3 ms | 42 us | 57 us | 747 MiB | 57 MiB |
| stomp-persistent | 68,477 | 63,507 | 0.93 | 31,753 | 4.0 ms / 12.8 ms | 1.3 ms / 12.4 ms | 67 us | 51 us | 636 MiB | 115 MiB |
| mqtt | 93,674 | 103,608 | 1.11 | 46,837 | 86 us / 694 us | 141 us / 507 us | 42 us | 37 us | 591 MiB | 51 MiB |
| mqtt-persistent | 44,478 | 77,255 | 1.74 | 22,239 | 2.9 ms / 26.0 ms | 860 us / 9.8 ms | 113 us | 53 us | 2,314 MiB | 106 MiB |

In `mqtt-persistent`'s latency run, neither broker quite kept the rate: Artemis received 21,763 msg/s and Saranga 21,930.

## Reading them

- **Throughput.**
  - It is higher for Saranga in 8 of the 12 unbounded scenarios: by 4 to 10% on AMQP and persistent Core, 9 to 34% with large messages, 11% on MQTT, 74% on persistent MQTT and 2.3 times on persistent OpenWire. Both keep up with 100 queues at 10,000 messages a second.
  - On Core and OpenWire without persistence it is level, within this machine's noise.
  - On STOMP it is lower: 84% of Artemis's without persistence and 93% with it.
- **Startup** is about 16 times faster, at a sixth of the memory.
- **CPU a message at the same rate.**
  - It is lower for Saranga in 11 of the 13 scenarios: by a fifth to a half with persistent and large messages, a quarter on OpenWire, and 7 to 12% on non-persistent AMQP and MQTT.
  - On non-persistent Core it is the same.
  - On non-persistent STOMP it is a third higher: 57 against 42 microseconds.
- **Latency at the same rate.**
  - With persistent 1 KiB messages, p50 is a third to a half of Artemis's, and a little over half on persistent OpenWire. With large messages it is 10 to 20% lower.
  - Without persistence, p50 is 6 to 93 microseconds higher.
  - p99 is the same or lower in most scenarios. It is higher on non-persistent OpenWire and STOMP, and on `amqp-persistent`, a figure that moves a lot from run to run: runs on the same day have given 8.8 to 41 ms.
- **Peak memory** is a twentieth to a fifth of Artemis's on OpenWire, STOMP and MQTT, and a seventh to a quarter with persistent Core, large messages and many queues. It is higher where the producer outran the consumer during the run, so that a backlog of messages was held in memory:
  - non-persistent Core, for both brokers;
  - non-persistent AMQP, for both brokers;
  - Saranga's persistent AMQP.

  Neither broker pages these backlogs: both page only beyond `global-max-size`, which is 1 GiB for each here (half of Artemis's 2 GiB heap, and Saranga's default).
- **The noise.** Runs on this machine moved by up to 5% between runs on the same day. Its CPU also throttles once warm: both brokers lose about a tenth of their rate after the first 30 s, which the 15 s warmup only partly excludes.

## Running it

    CARGO_TARGET_DIR=~/saranga-target WORK=~/saranga-bench scripts/bench.sh [SCENARIO...]

The script runs on Linux. It needs the Artemis distribution that `scripts/build-artemis.sh` builds, and Java 17 or later. `WORK` should be on a local file system: in WSL, not under `/mnt`. `DURATION`, `WARMUP`, `STARTS`, `NUM_QUEUES`, `MANY_RATE`, `MAX_PENDING`, `LOAD_INFLIGHT`, `BROKER_CPUS` and `CLIENT_CPUS` change the runs. On a machine without Rust, `SARANGA_BIN` names a `saranga` binary built elsewhere, and `ARTEMIS_DIST` the Artemis distribution. The results are written to `$WORK/results.md` and `$WORK/results.tsv`, and each run's client output and report to `$WORK/runs`.
