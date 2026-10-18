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

Run on 2026-10-17 at Saranga commit `bc9efdd`, every scenario in one run, with no failures.

The machine was a dedicated Linux box (kernel 7.0) with an i5-8250U: four cores, eight threads. The broker had CPUs 0, 1, 4 and 5 (two cores and their hyperthreads), and the client the rest. It had 3.3 GiB of memory and ext4 on a SATA SSD. Artemis and the clients ran on OpenJDK 25.0.4.1.

| Startup | Artemis | Saranga |
|---|---|---|
| Time to the acceptor (median of 5) | 2,717 ms | 145 ms |
| Resident memory when started | 209 MiB | 36 MiB |

| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Latency at msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis CPU a message | Saranga CPU a message | Artemis peak RSS | Saranga peak RSS |
|---|---|---|---|---|---|---|---|---|---|---|
| core | 74,878 | 75,891 | 1.01 | 37,439 | 139 us / 667 us | 156 us / 343 us | 64 us | 59 us | 601 MiB | 881 MiB |
| core-persistent | 64,674 | 72,621 | 1.12 | 32,337 | 4.1 ms / 15.0 ms | 1.2 ms / 9.1 ms | 77 us | 43 us | 576 MiB | 288 MiB |
| amqp | 30,961 | 33,155 | 1.07 | 15,480 | 129 us / 329 us | 133 us / 281 us | 81 us | 76 us | 2,238 MiB | 1,577 MiB |
| amqp-persistent | 27,307 | 31,645 | 1.16 | 13,653 | 3.6 ms / 41.2 ms | 1.6 ms / 36.6 ms | 111 us | 64 us | 591 MiB | 1,116 MiB |
| core-large | 61 | 78 | 1.28 | 30 | 22.9 ms / 41.2 ms | 17.8 ms / 30.3 ms | 19.4 ms | 10.5 ms | 576 MiB | 81 MiB |
| amqp-large | 95 | 104 | 1.09 | 47 | 22.1 ms / 40.2 ms | 20.2 ms / 32.8 ms | 16.2 ms | 9.3 ms | 809 MiB | 197 MiB |
| many-queues | 9,999 | 9,999 | 1.00 | 10,000 | 3.2 ms / 7.8 ms | 1.1 ms / 4.2 ms | 188 us | 144 us | 584 MiB | 118 MiB |
| openwire | 49,575 | 52,375 | 1.06 | 24,787 | 182 us / 1.6 ms | 245 us / 2.6 ms | 117 us | 92 us | 719 MiB | 65 MiB |
| openwire-persistent | 283 | 634 | 2.24 | 141 | 4.5 ms / 6.7 ms | 2.5 ms / 2.8 ms | 3.6 ms | 1.9 ms | 586 MiB | 45 MiB |
| stomp | 87,287 | 99,570 | 1.14 | 43,643 | 80 us / 738 us | 124 us / 1.0 ms | 43 us | 38 us | 619 MiB | 50 MiB |
| stomp-persistent | 70,023 | 78,592 | 1.12 | 35,011 | 4.2 ms / 13.2 ms | 1.3 ms / 11.7 ms | 65 us | 39 us | 651 MiB | 118 MiB |
| mqtt | 91,683 | 121,195 | 1.32 | 45,841 | 83 us / 631 us | 116 us / 289 us | 40 us | 39 us | 583 MiB | 51 MiB |
| mqtt-persistent | 43,183 | 90,555 | 2.10 | 21,591 | 2.9 ms / 18.4 ms | 855 us / 6.6 ms | 115 us | 49 us | 2,340 MiB | 105 MiB |

In `mqtt-persistent`'s latency run, neither broker quite kept the rate: Artemis received 21,140 msg/s and Saranga 21,334.

## Reading them

- **Throughput.**
  - It is higher for Saranga in 11 of the 12 unbounded scenarios and level on Core (1.01): 6 to 16% higher on AMQP, OpenWire, STOMP and persistent Core, 9 to 28% with large messages, 32% on MQTT, and 2.1 and 2.2 times on persistent MQTT and OpenWire. Both keep up with 100 queues at 10,000 messages a second.
  - On non-persistent Core the difference is within this machine's noise.
- **Startup** is about 19 times faster, at a sixth of the memory.
- **CPU a message at the same rate.**
  - It is lower for Saranga in 12 of the 13 scenarios: by 40 to 57% with persistent and large messages, a quarter with 100 queues, a fifth on OpenWire, an eighth on STOMP, and 6 to 8% on non-persistent Core and AMQP.
  - On non-persistent MQTT the two are level (39 against 40 microseconds).
- **Latency at the same rate.**
  - With persistent 1 KiB messages, p50 is 30 to 45% of Artemis's, and a little over half on persistent OpenWire. With large messages it is 9 to 22% lower.
  - Without persistence, p50 is 4 to 63 microseconds higher.
  - p99 is lower in every scenario but non-persistent OpenWire and STOMP. `amqp-persistent`'s p99 moves a lot from run to run: runs on the same day have given 8.8 to 41 ms.
- **Peak memory** is a twenty-second to a fifth of Artemis's on OpenWire, STOMP and MQTT, and a seventh to a half with persistent Core, large messages and many queues. Where the producer outran the consumer during the run, a backlog of messages was held in memory, and the peaks are high:
  - non-persistent Core and AMQP, for both brokers (Saranga's higher on Core, lower on AMQP);
  - Saranga's persistent AMQP.

  Neither broker pages these backlogs: both page only beyond `global-max-size`, which is 1 GiB for each here (half of Artemis's 2 GiB heap, and Saranga's default).
- **The noise.** Runs on this machine moved by up to 5% between runs on the same day. Its CPU also throttles once warm: both brokers lose about a tenth of their rate after the first 30 s, which the 15 s warmup only partly excludes.

## Running it

    CARGO_TARGET_DIR=~/saranga-target WORK=~/saranga-bench scripts/bench.sh [SCENARIO...]

The script runs on Linux. It needs the Artemis distribution that `scripts/build-artemis.sh` builds, and Java 17 or later. `WORK` should be on a local file system: in WSL, not under `/mnt`. `DURATION`, `WARMUP`, `STARTS`, `NUM_QUEUES`, `MANY_RATE`, `MAX_PENDING`, `LOAD_INFLIGHT`, `BROKER_CPUS` and `CLIENT_CPUS` change the runs. On a machine without Rust, `SARANGA_BIN` names a `saranga` binary built elsewhere, and `ARTEMIS_DIST` the Artemis distribution. The results are written to `$WORK/results.md` and `$WORK/results.tsv`, and each run's client output and report to `$WORK/runs`.
