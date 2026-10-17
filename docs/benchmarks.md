# Benchmarks

Saranga against Apache Artemis 2.57.0, on the same machine and in the same scenarios, made by [scripts/bench.sh](../scripts/bench.sh).

## Method

- **One client for both.** Artemis's own `artemis perf client`: the Core JMS client, or Qpid JMS for AMQP. A producer and a consumer on `queue://BENCH`, with up to 100 sends outstanding (`--max-pending 100`).
- **Instances made by each broker's own `create`**, with `--nio --no-autotune --no-web` and otherwise their defaults. Both use the NIO journal, which is the one Saranga writes, with `journal-datasync` on. Artemis has its default heap (`-Xms512M -Xmx2G`).
- **Separate cores.** The broker runs on half the CPUs and the client on the other half (`taskset`). Both brokers run from a copy on the same local file system, and every run starts on an empty data directory.
- **Throughput:** messages received a second with the producer unbounded, the mean of the per-second samples from the end of a 15 s warmup to the producer's last send, over 60 s of sending. The backlog a broker drains after the producer stops is not counted. The perf client counts its warmup within `--duration`, so the script asks for 75 s.
- **Latency:** both brokers run again at the same rate, half the lower of their two throughputs. The figure is the client's transfer time, from send to receipt, at p50 and p99.
- **CPU a message:** the broker's CPU time from three seconds after the warmup to the end of the run, divided by the messages received in that time.
- **Memory:** the broker's peak resident set (`VmHWM`) at the end of the throughput run.
- **Startup:** the time from launching `artemis run` until the acceptor takes a connection, the median of five starts. The resident memory is read once the broker is up.
- **Scenarios:**
  - `core`, `core-persistent`, `amqp` and `amqp-persistent` send 1 KiB messages.
  - `core-large` and `amqp-large` send persistent 1 MiB messages, which both brokers handle as large messages.
  - `many-queues` runs 100 persistent Core queues, each with its own producer and consumer, at 10,000 messages a second in all. Unbounded, 100 producers outrun their consumers and the run measures a backlog: Artemis stalled that way after a minute.

## Results

Run on 2026-10-17 at Saranga commit `2bf3e46`. The machine was a dedicated Linux box (kernel 7.0) with an i5-8250U: four cores, eight threads, of which the broker had CPUs 0, 1, 4 and 5 (two cores and their hyperthreads) and the client the rest. It had 3.3 GiB of memory and ext4 on a SATA SSD. Both brokers ran on OpenJDK 25.0.4.1, Artemis and the client alike.

| Startup | Artemis | Saranga |
|---|---|---|
| Time to the acceptor (median of 5) | 2,691 ms | 154 ms |
| Resident memory when started | 225 MiB | 36 MiB |

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Latency at msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis CPU a message | Saranga CPU a message | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|---|---|---|---|---|
| core | 74,056 | 76,816 | 1.04 | 37,028 | 141 us / 591 us | 170 us / 457 us | 64 us | 66 us | 580 MiB | 604 MiB |
| core-persistent | 65,198 | 67,876 | 1.04 | 32,599 | 4.1 ms / 79.9 ms | 1.2 ms / 18.8 ms | 76 us | 52 us | 599 MiB | 120 MiB |
| amqp | 30,984 | 34,102 | 1.10 | 15,492 | 126 us / 341 us | 148 us / 319 us | 81 us | 97 us | 2,215 MiB | 1,566 MiB |
| amqp-persistent | 27,526 | 32,313 | 1.17 | 13,763 | 3.7 ms / 26.6 ms | 1.6 ms / 35.1 ms | 116 us | 69 us | 575 MiB | 713 MiB |
| core-large | 63 | 83 | 1.32 | 31 | 23.0 ms / 42.5 ms | 18.2 ms / 34.3 ms | 19.6 ms | 11.3 ms | 568 MiB | 99 MiB |
| amqp-large | 95 | 106 | 1.12 | 47 | 22.3 ms / 62.2 ms | 20.2 ms / 35.6 ms | 16.2 ms | 9.2 ms | 811 MiB | 207 MiB |
| many-queues | 9,999 | 9,999 | 1.00 | 10,000 | 3.1 ms / 10.9 ms | 1.1 ms / 3.6 ms | 176 us | 151 us | 608 MiB | 131 MiB |

CPU a message is from the latency run, where both brokers carry the same load.

## Reading them

- **Throughput is higher for Saranga in every scenario.** The margin is 4% on Core with small messages, 10 to 17% on AMQP and 12 to 32% with large messages. Both keep up with 100 queues at 10,000 messages a second.
- **Startup** is about 17 times faster, at a sixth of the memory.
- **CPU a message at the same rate** is lower for Saranga wherever messages are persistent, by a third to a half. On non-persistent Core it is about the same, and on non-persistent AMQP it is about a fifth higher.
- **Latency at the same rate.**
  - With persistent messages, p50 is a third to a half of Artemis's.
  - Without, p50 is 20 to 30 microseconds higher and p99 lower.
  - p99 is lower in every scenario but `amqp-persistent`, where that figure moves a lot from run to run: three runs on the same day gave 8.8, 34 and 41 ms.
- **Peak memory** is a fifth or less of Artemis's with persistent Core and large messages. It is the same or more where the producer outran the consumer during the run, so that a backlog of messages was held in memory:
  - non-persistent Core, for both brokers;
  - non-persistent AMQP, for both brokers;
  - Saranga's persistent AMQP.

  Neither broker pages these backlogs: both page only beyond `global-max-size`, which is 1 GiB for each here (half of Artemis's 2 GiB heap, and Saranga's default).
- **The noise.** Runs on this machine moved by up to 5% between runs on the same day. Its CPU also throttles once warm: both brokers lose about a tenth of their rate after the first 30 s, which the 15 s warmup only partly excludes.

## Running it

    CARGO_TARGET_DIR=~/saranga-target WORK=~/saranga-bench scripts/bench.sh [SCENARIO...]

The script runs on Linux. It needs the Artemis distribution that `scripts/build-artemis.sh` builds, and Java 17 or later. `WORK` should be on a local file system: in WSL, not under `/mnt`. `DURATION`, `WARMUP`, `STARTS`, `NUM_QUEUES`, `MANY_RATE`, `MAX_PENDING`, `BROKER_CPUS` and `CLIENT_CPUS` change the runs. On a machine without Rust, `SARANGA_BIN` names a `saranga` binary built elsewhere, and `ARTEMIS_DIST` the Artemis distribution. The results are written to `$WORK/results.md` and `$WORK/results.tsv`, and each run's client output and JSON report to `$WORK/runs`.
