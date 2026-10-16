# Benchmarks

Saranga against Apache Artemis 2.57.0, on the same machine and in the same scenarios, made by [scripts/bench.sh](../scripts/bench.sh).

## Method

- **One client for both.** Artemis's own `artemis perf client`: the Core JMS client, or Qpid JMS for AMQP. A producer and a consumer on `queue://BENCH`, with up to 100 sends outstanding (`--max-pending 100`).
- **Instances made by each broker's own `create`**, with `--nio --no-autotune --no-web` and otherwise their defaults. Both use the NIO journal, which is the one Saranga writes, with `journal-datasync` on. Artemis has its default heap (`-Xms512M -Xmx2G`).
- **Separate cores.** The broker runs on two cores and the client on the other two (`taskset`). Both brokers run from a copy on the same local file system, and every run starts on an empty data directory.
- **Throughput:** messages received a second, the mean of the samples after a 15 s warmup, over 60 s, with the producer unbounded. A broker whose consumer falls behind is measured with its backlog growing.
- **Latency:** both brokers run again at the same rate, half the lower of their two throughputs. The figure is the client's transfer time, from send to receipt, at p50 and p99.
- **Memory:** the broker's peak resident set (`VmHWM`) at the end of the throughput run.
- **Startup:** the time from launching `artemis run` until the acceptor takes a connection, the median of five starts. The resident memory is read once the broker is up.
- **Scenarios:**
  - `core`, `core-persistent`, `amqp` and `amqp-persistent` send 1 KiB messages.
  - `core-large` and `amqp-large` send persistent 1 MiB messages, which both brokers handle as large messages.
  - `many-queues` runs 100 persistent Core queues, each with its own producer and consumer, at 10,000 messages a second in all. Unbounded, 100 producers outrun their consumers and the run measures a backlog: Artemis stalled that way after a minute.

## Results

Run on 2026-10-16 at Saranga commit `ed6c250`. The machine was WSL 2 (kernel 6.18) on an i9-12900KF, with 4 virtual CPUs, 31 GiB and ext4. Both brokers ran on OpenJDK 25.0.4.1, Artemis and the client alike.

| Startup | Artemis | Saranga |
|---|---|---|
| Time to the acceptor (median of 5) | 2,304 ms | 53 ms |
| Resident memory when started | 198 MiB | 19 MiB |

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Latency at msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|---|---|---|
| core | 52,985 | 45,844 | 0.87 | 22,922 | 319 us / 2.9 ms | 307 us / 2.7 ms | 539 MiB | 27 MiB |
| core-persistent | 41,908 | 39,136 | 0.93 | 19,568 | 3.8 ms / 7.9 ms | 1.6 ms / 5.1 ms | 524 MiB | 32 MiB |
| amqp | 19,181 | 14,402 | 0.75 | 7,201 | 643 us / 528 ms | 2.1 ms / 259 ms | 1,754 MiB | 2,809 MiB |
| amqp-persistent | 14,089 | 10,972 | 0.78 | 5,486 | 4.4 ms / 32.4 ms | 2.7 ms / 17.8 ms | 842 MiB | 65 MiB |
| core-large | 104 | 112 | 1.08 | 52 | 9.9 ms / 22.3 ms | 8.7 ms / 16.5 ms | 521 MiB | 24 MiB |
| amqp-large | 137 | 147 | 1.07 | 68 | 9.2 ms / 19.5 ms | 9.5 ms / 15.6 ms | 749 MiB | 79 MiB |
| many-queues | 9,999 | 9,999 | 1.00 | 10,000 | 4.3 ms / 11.4 ms | 2.4 ms / 15.5 ms | 521 MiB | 46 MiB |

## Reading them

- **Startup and memory are Saranga's clearest gains.** It starts about 40 times faster and holds a tenth or less of Artemis's memory, in every scenario but non-persistent AMQP.
- **Throughput.**
  - Saranga is within 13% of Artemis on Core with small messages.
  - It is 22 to 25% behind on AMQP with small messages.
  - It is slightly ahead with large messages.
  - Both keep up with 100 queues at 10,000 messages a second.
- **Latency at the same rate** is lower for Saranga at p99 in every scenario but `many-queues`. At p50 it is lower or close, except non-persistent AMQP.
- **Non-persistent AMQP** is the scenario where the producer outruns its consumer, so both brokers build a backlog. Artemis holds AMQP producers back as the address fills (it pages, and under `BLOCK` it withholds credit). Saranga keeps granting credit (COMPAT.md, AMQP), so its backlog, and its memory, grow further. Its p99 there measures the backlog, not the broker.
- **The noise is large.** WSL shares its cores with Windows, and the ratios moved between two full runs of the same binaries an hour apart: `core` went from 1.04 to 0.87, `core-persistent` from 0.74 to 0.93, and `amqp` from 0.85 to 0.75. Read differences under 15% as a tie. The first run's `many-queues` was unbounded, and so was not comparable.

## Running it

    CARGO_TARGET_DIR=~/saranga-target WORK=~/saranga-bench scripts/bench.sh [SCENARIO...]

The script runs on Linux. It needs the Artemis distribution that `scripts/build-artemis.sh` builds, and Java 17 or later. `WORK` should be on a local file system: in WSL, not under `/mnt`. `DURATION`, `WARMUP`, `STARTS`, `NUM_QUEUES`, `MANY_RATE`, `MAX_PENDING`, `BROKER_CPUS` and `CLIENT_CPUS` change the runs. The results are written to `$WORK/results.md` and `$WORK/results.tsv`, and each run's client output and JSON report to `$WORK/runs`.
