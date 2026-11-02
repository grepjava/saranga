# Benchmarks

Saranga against Apache Artemis 2.57.0, on the same machine and in the same scenarios, made by [scripts/bench.sh](https://github.com/grepjava/saranga/blob/main/scripts/bench.sh).

## Method

- **The same client for both.**
  - For Core and AMQP it is Artemis's own `artemis perf client`: the Core JMS client, or Qpid JMS for AMQP. A producer and a consumer on `queue://BENCH`, with up to 100 sends outstanding (`--max-pending 100`).
  - Artemis's perf client speaks no other protocol, so for OpenWire, STOMP and MQTT both brokers get [scripts/bench/Load.java](https://github.com/grepjava/saranga/blob/main/scripts/bench/Load.java). It runs a producer and a consumer on one queue with 1 KiB messages, with at most 1,000 sent and not yet received.
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

Run on 2026-11-02 at Saranga commit `40329ba`, every scenario in one run, with no failures.

The machine was a dedicated Linux box (Ubuntu 26.10, kernel 7.3) with an i5-8250U: four cores, eight threads. The broker had CPUs 0, 1, 4 and 5 (two cores and their hyperthreads), and the client the rest. It had 3.3 GiB of memory and ext4 on a SATA SSD. Artemis and the clients ran on OpenJDK 25.0.4.1.

| Startup | Artemis | Saranga |
|---|---|---|
| Time to the acceptor (median of 5) | 2,706 ms | 143 ms |
| Resident memory when started | 209 MiB | 42 MiB |

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Latency at msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis CPU a message | Saranga CPU a message | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|---|---|---|---|---|
| core | 74,607 | 80,232 | 1.08 | 37,303 | 141 us / 607 us | 138 us / 295 us | 64 us | 43 us | 605 MiB | 663 MiB |
| core-persistent | 65,077 | 73,589 | 1.13 | 32,538 | 4.1 ms / 11.6 ms | 1.2 ms / 9.9 ms | 76 us | 41 us | 625 MiB | 403 MiB |
| amqp | 31,344 | 31,682 | 1.01 | 15,672 | 128 us / 391 us | 129 us / 295 us | 81 us | 71 us | 2,233 MiB | 1,617 MiB |
| amqp-persistent | 27,067 | 30,919 | 1.14 | 13,533 | 3.6 ms / 26.9 ms | 1.6 ms / 34.0 ms | 116 us | 64 us | 586 MiB | 1,156 MiB |
| core-large | 60 | 78 | 1.30 | 30 | 22.9 ms / 39.2 ms | 17.9 ms / 29.3 ms | 19.5 ms | 10.3 ms | 562 MiB | 88 MiB |
| amqp-large | 99 | 106 | 1.07 | 49 | 23.3 ms / 37.6 ms | 20.0 ms / 33.8 ms | 16.4 ms | 9.2 ms | 813 MiB | 191 MiB |
| many-queues | 9,999 | 9,999 | 1.00 | 10,000 | 1.1 ms / 4.5 ms | 995 us / 2.8 ms | 237 us | 108 us | 621 MiB | 120 MiB |
| openwire | 49,977 | 51,716 | 1.03 | 24,988 | 188 us / 1.4 ms | 244 us / 2.7 ms | 112 us | 89 us | 628 MiB | 71 MiB |
| openwire-persistent | 285 | 642 | 2.25 | 142 | 4.5 ms / 6.6 ms | 2.5 ms / 2.8 ms | 3.6 ms | 1.9 ms | 588 MiB | 53 MiB |
| stomp | 88,556 | 106,239 | 1.20 | 44,278 | 82 us / 551 us | 117 us / 786 us | 44 us | 37 us | 688 MiB | 64 MiB |
| stomp-persistent | 68,863 | 83,307 | 1.21 | 34,431 | 4.1 ms / 12.4 ms | 1.3 ms / 10.6 ms | 64 us | 38 us | 629 MiB | 124 MiB |
| mqtt | 91,019 | 118,634 | 1.30 | 45,509 | 85 us / 393 us | 110 us / 238 us | 41 us | 37 us | 581 MiB | 60 MiB |
| mqtt-persistent | 44,531 | 94,725 | 2.13 | 22,265 | 2.9 ms / 12.4 ms | 862 us / 7.0 ms | 112 us | 47 us | 2,343 MiB | 114 MiB |

In `mqtt-persistent`'s latency run, neither broker quite kept the rate: Artemis received 21,805 msg/s and Saranga 21,995.

## Reading them

- **Throughput.**
  - It is higher for Saranga in 10 of the 12 unbounded scenarios: 8 to 21% on Core, STOMP and persistent Core and AMQP, 7 and 30% with large messages, 30% on MQTT, and 2.1 and 2.25 times on persistent MQTT and OpenWire. Both keep up with 100 queues at 10,000 messages a second.
  - On non-persistent AMQP and OpenWire (1.01 and 1.03) the difference is within this machine's noise.
- **Startup** is about 19 times faster, at a fifth of the memory.
- **CPU a message at the same rate.**
  - It is lower for Saranga in all 13 scenarios: by 41 to 58% with persistent and large messages, 54% with 100 queues, a third on non-persistent Core, a fifth on OpenWire, and 10 to 16% on non-persistent AMQP, STOMP and MQTT.
- **Latency at the same rate.**
  - With persistent 1 KiB messages, p50 is 30 to 43% of Artemis's, and a little over half on persistent OpenWire. With large messages it is 14 to 22% lower, and with 100 queues 6% lower.
  - Without persistence, p50 is level on Core and AMQP and 25 to 56 microseconds higher on OpenWire, STOMP and MQTT.
  - p99 is lower in every scenario but non-persistent OpenWire and STOMP and persistent AMQP. `amqp-persistent`'s p99 moves a lot from run to run: in another run the same morning, Artemis's was 36.6 ms and Saranga's 6.9 ms.
- **Peak memory** is a twentieth to a fifth of Artemis's on OpenWire, STOMP and MQTT, a sixth to a quarter with large messages and many queues, and two thirds with persistent Core. Where the producer outran the consumer during the run, a backlog of messages was held in memory, and the peaks are high:
  - non-persistent Core and AMQP, for both brokers (Saranga's higher on Core, lower on AMQP);
  - Saranga's persistent AMQP.

  Neither broker pages these backlogs: both page only beyond `global-max-size`, which is 1 GiB for each here (half of Artemis's 2 GiB heap, and Saranga's default).
- **The noise.** Runs on this machine move by up to 5% between runs on the same day. Its CPU also throttles once warm: both brokers lose about a tenth of their rate after the first 30 s, which the 15 s warmup only partly excludes.

## Running it

    CARGO_TARGET_DIR=~/saranga-target WORK=~/saranga-bench scripts/bench.sh [SCENARIO...]

The script runs on Linux. It needs the Artemis distribution that `scripts/build-artemis.sh` builds, and Java 17 or later. `WORK` should be on a local file system: in WSL, not under `/mnt`. `DURATION`, `WARMUP`, `STARTS`, `NUM_QUEUES`, `MANY_RATE`, `MAX_PENDING`, `LOAD_INFLIGHT`, `BROKER_CPUS` and `CLIENT_CPUS` change the runs. On a machine without Rust, `SARANGA_BIN` names a `saranga` binary built elsewhere, and `ARTEMIS_DIST` the Artemis distribution. The results are written to `$WORK/results.md` and `$WORK/results.tsv`, and each run's client output and report to `$WORK/runs`.
