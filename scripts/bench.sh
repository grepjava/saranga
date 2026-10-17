#!/usr/bin/env bash
# Benchmarks Artemis and Saranga on this machine, in the same scenarios and
# with the same client: Artemis's own "artemis perf client" (the Core JMS
# client, or Qpid JMS for AMQP), each broker an instance made by its own
# "create" with the NIO journal (the one Saranga writes) and no web server.
#
#   scripts/bench.sh [SCENARIO...]
#
# Each scenario runs each broker twice on an empty data directory: once
# with an unbounded rate for its throughput (messages received a second,
# the mean of the samples after the warmup), then both at the same rate,
# half the lower of the two throughputs, for their latency (the client's
# "transfer" time, from send to receipt, p50 and p99). The broker's peak
# resident memory (VmHWM) is read when each throughput run ends. Startup is
# the time from launching "artemis run" until the acceptor takes a
# connection, the median of STARTS starts. Linux only (taskset, /proc).
#
# Scenarios (default all): core, core-persistent, amqp, amqp-persistent,
# core-large, amqp-large (1 MiB messages, persistent) and many-queues (Core,
# persistent, NUM_QUEUES queues, a producer and a consumer on each, run at
# MANY_RATE messages a second in all, default 10000, rather than unbounded),
# then openwire, openwire-persistent, stomp, stomp-persistent, mqtt (QoS 0)
# and mqtt-persistent (QoS 1). The perf client speaks only Core and AMQP:
# for the last six both brokers get scripts/bench/Load.java, a producer and a
# consumer with 1 KiB messages and at most LOAD_INFLIGHT (default 1000) sent
# and not yet received, over the ActiveMQ 5 JMS client for OpenWire and
# plain STOMP 1.2 and MQTT 3.1.1 over a socket.
#
# DURATION (default 60) and WARMUP (default 15) are each run's seconds;
# STARTS (default 5); NUM_QUEUES (default 100); MAX_PENDING (default 100),
# the sends a producer has outstanding; BROKER_CPUS and CLIENT_CPUS (default
# the first and second half of the CPUs) the cores taskset gives each;
# WORK (default target/bench) where the brokers and results go, which
# should be on a local file system; ARTEMIS_DIST the distribution. SARANGA_BIN
# a saranga binary built elsewhere, for a machine without Rust. The
# results are written to $WORK/results.md and $WORK/results.tsv.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
WORK="${WORK:-$ROOT/target/bench}"
DURATION="${DURATION:-60}" WARMUP="${WARMUP:-15}" STARTS="${STARTS:-5}"
NUM_QUEUES="${NUM_QUEUES:-100}" MANY_RATE="${MANY_RATE:-10000}" MAX_PENDING="${MAX_PENDING:-100}"
LOAD_INFLIGHT="${LOAD_INFLIGHT:-1000}"
CPUS="$(nproc)"
BROKER_CPUS="${BROKER_CPUS:-0-$((CPUS / 2 - 1))}"
CLIENT_CPUS="${CLIENT_CPUS:-$((CPUS / 2))-$((CPUS - 1))}"
PORT=61616
ALL="core core-persistent amqp amqp-persistent core-large amqp-large many-queues openwire openwire-persistent stomp stomp-persistent mqtt mqtt-persistent"
SCENARIOS="${*:-$ALL}"
for s in $SCENARIOS; do
  [[ " $ALL " == *" $s "* ]] || { echo "unknown scenario: $s" >&2; exit 2; }
done
[[ "$(uname -s)" == Linux ]] || { echo "bench.sh runs on Linux" >&2; exit 2; }
[[ -d "$DIST/lib" ]] || { echo "no Artemis distribution at $DIST (scripts/build-artemis.sh)" >&2; exit 2; }

# SARANGA_BIN: a saranga built elsewhere (a machine without Rust).
if [[ -z "${SARANGA_BIN:-}" ]]; then
  cargo build -q --release -p saranga-cli --manifest-path "$ROOT/Cargo.toml" || exit 1
fi
# Both brokers run from the same file system, so neither loads its code
# from a slower one.
rm -rf "$WORK/brokers" "$WORK/runs"
mkdir -p "$WORK/brokers" "$WORK/runs"
cp -r "$DIST" "$WORK/brokers/dist"
mkdir -p "$WORK/brokers/bin" && cp "${SARANGA_BIN:-$TARGET/release/saranga}" "$WORK/brokers/bin/saranga"
ARTEMIS="$WORK/brokers/dist/bin/artemis"
SARANGA="$WORK/brokers/bin/saranga"
RESULTS="$WORK/results.tsv"
: > "$RESULTS"

make_instance() {
  local kind="$1" dir="$WORK/brokers/$1"
  local opts=(--silent --no-autotune --nio --user admin --password admin --require-login --no-web --host localhost)
  case "$kind" in
    saranga) "$SARANGA" create "$dir" "${opts[@]}" > /dev/null ;;
    artemis) "$ARTEMIS" create "$dir" "${opts[@]}" > /dev/null ;;
  esac
}

up() { (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; }
now_ms() { echo $(($(date +%s%N) / 1000000)); }
# The process listening on the acceptor.
broker_pid() { ss -ltnpH "sport = :$PORT" | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2; }

PID= STARTED_MS=
# Starts an instance on its broker cores and waits for its acceptor,
# setting STARTED_MS to the milliseconds it took and PID to its process.
start() {
  local dir="$WORK/brokers/$1" t0
  if up; then echo "port $PORT is taken before $1 starts" >&2; return 1; fi
  t0="$(now_ms)"
  (cd "$dir/bin" && exec setsid taskset -c "$BROKER_CPUS" ./artemis run > "$dir/run.out" 2>&1 &)
  for _ in $(seq 1 6000); do
    if up; then
      STARTED_MS=$(($(now_ms) - t0))
      PID="$(broker_pid)"
      return 0
    fi
    sleep 0.01
  done
  echo "$1 did not start:" >&2
  tail -20 "$dir/run.out" >&2
  return 1
}
stop() {
  local dir="$WORK/brokers/$1"
  (cd "$dir/bin" && ./artemis stop > /dev/null 2>&1)
  for _ in $(seq 1 600); do
    [[ -n "$PID" && -d "/proc/$PID" ]] || { PID=; break; }
    sleep 0.1
  done
  kill -9 "$PID" 2>/dev/null
  PID=
  for _ in $(seq 1 300); do up || return 0; sleep 0.1; done
  echo "port $PORT still taken after stopping $1" >&2
}
# An instance with empty data, journal, paging and large message
# directories.
reset() { rm -rf "$WORK/brokers/$1/data"; }
rss_kb() { awk -v k="$2:" '$1 == k { print $2 }' "/proc/$1/status" 2>/dev/null; }

record() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >> "$RESULTS"; }

# Runs the perf client: scenario, broker, run name, then its options.
perf() {
  local scenario="$1" kind="$2" run="$3"; shift 3
  case "$scenario" in
    openwire* | stomp* | mqtt*) load "$scenario" "$kind" "$run" "$@"; return ;;
  esac
  local protocol=core url="tcp://localhost:$PORT" size=1024 persistent=() dests=() spread=()
  case "$scenario" in
    amqp*) protocol=amqp url="amqp://localhost:$PORT" ;;
  esac
  case "$scenario" in
    *-large) size=1048576 persistent=(--persistent) ;;
    *-persistent) persistent=(--persistent) ;;
    many-queues) persistent=(--persistent) dests=(--num-destinations "$NUM_QUEUES") spread=(--threads 2) ;;
  esac
  local out="$WORK/runs/$scenario-$kind-$run"
  taskset -c "$CLIENT_CPUS" "$ARTEMIS" perf client --protocol "$protocol" --url "$url" \
    --user admin --password admin --message-size "$size" "${persistent[@]}" "${dests[@]}" "${spread[@]}" \
    --max-pending "$MAX_PENDING" --duration "$((WARMUP + DURATION))" --warmup "$WARMUP" \
    --json "$out.json" "$@" queue://BENCH > "$out.out" 2>&1 &
  local client=$!
  # The broker's CPU time from after the warmup (and the client's start) to
  # the end: its ticks and the milliseconds they cover.
  sleep $((WARMUP + 3))
  local t0 c0 t1 c1
  t0="$(now_ms)" c0="$(cpu_ticks "$PID")"
  wait "$client"
  t1="$(now_ms)" c1="$(cpu_ticks "$PID")"
  echo "$((c1 - c0)) $((t1 - t0))" > "$out.cpu"
}
cpu_ticks() { awk '{ print $14 + $15 }' "/proc/$1/stat" 2>/dev/null || echo 0; }

# Runs Load.java as perf runs the perf client (its options: --rate N or
# none), leaving its RESULT line in the run's .result file.
load() {
  local scenario="$1" kind="$2" run="$3" rate=0 proto persistent=false; shift 3
  [[ "${1:-}" == --rate ]] && rate="$2"
  case "$scenario" in
    openwire*) proto=openwire ;;
    stomp*) proto=stomp ;;
    mqtt-persistent) proto=mqtt1 ;;
    mqtt) proto=mqtt0 ;;
  esac
  [[ "$scenario" == *-persistent ]] && persistent=true
  local out="$WORK/runs/$scenario-$kind-$run"
  taskset -c "$CLIENT_CPUS" java -Dinflight="$LOAD_INFLIGHT" -Dpersistent="$persistent" \
    -cp "$WORK/brokers/dist/lib/*" "$ROOT/scripts/bench/Load.java" \
    "$proto" 127.0.0.1 "$PORT" "$((WARMUP + DURATION))" "$WARMUP" "$rate" > "$out.out" 2>&1 &
  local client=$!
  sleep $((WARMUP + 3))
  local t0 c0 t1 c1
  t0="$(now_ms)" c0="$(cpu_ticks "$PID")"
  wait "$client"
  t1="$(now_ms)" c1="$(cpu_ticks "$PID")"
  echo "$((c1 - c0)) $((t1 - t0))" > "$out.cpu"
  grep '^RESULT ' "$out.out" | cut -d' ' -f2- > "$out.result"
}
# The broker's CPU in a run, from its .cpu file and the messages received
# meanwhile: microseconds of CPU a message, and the cores it kept busy.
cpu_cost() {
  local ticks ms
  read -r ticks ms < "$1"
  perl -e 'my ($t, $ms, $n, $hz) = @ARGV; my $cpu = $t / $hz;
    printf "%d %.2f\n", $n ? $cpu * 1e6 / $n : 0, $ms ? $cpu * 1000 / $ms : 0' \
    "$ticks" "$ms" "$2" "$(getconf CLK_TCK)"
}

# From a perf client's JSON report: the mean received rate after the
# warmup until the producer's last send (not the backlog drained after it
# stops, which would credit a broker that fell behind; a stall before then
# counts), the summary's
# transfer p50 and p99 in microseconds, and the messages received in the
# broker CPU window (from three seconds after the warmup to the end).
report() {
  # Load.java's runs have it already.
  if [[ -s "${1%.json}.result" ]]; then cat "${1%.json}.result"; return; fi
  perl -MJSON::PP -e '
    local $/; open my $f, "<", $ARGV[0] or exit 1; my $s = decode_json(<$f>);
    my @i = grep { $_->{sampleType} eq "interval" && !$_->{warmup} } @$s;
    my ($last) = grep { $i[$_]{sent} > 0 } reverse 0 .. $#i;
    my @r = map { $_->{received} } @i[0 .. ($last // -1)];
    my ($sum, $n) = (0, 0); $sum += $_ for @r; $n += $_->{received} for @i[3 .. $#i];
    my ($summary) = grep { $_->{sampleType} eq "summary" } @$s;
    my $t = $summary->{totalTransferTime} // {};
    printf "%s %d %d %d %d\n", $summary->{result}, @r ? $sum / @r : 0, $t->{50} // 0, $t->{99} // 0, $n;
  ' "$1"
}

echo "Brokers on CPUs $BROKER_CPUS, client on $CLIENT_CPUS; ${DURATION}s runs after ${WARMUP}s warmup"
for kind in artemis saranga; do make_instance "$kind" || exit 1; done

# Startup, each broker started STARTS times on empty data.
for kind in artemis saranga; do
  times=()
  for _ in $(seq 1 "$STARTS"); do
    reset "$kind"
    start "$kind" || exit 1
    times+=("$STARTED_MS")
    idle="$(rss_kb "$PID" VmRSS)"
    stop "$kind"
  done
  median="$(printf '%s\n' "${times[@]}" | sort -n | sed -n "$(((STARTS + 1) / 2))p")"
  echo "startup $kind: ${median} ms (${times[*]}), ${idle} kB resident when started"
  record startup "$kind" ms "$median"
  record startup "$kind" idle_kb "$idle"
done

failed=0
for scenario in $SCENARIOS; do
  declare -A thr=()
  if [[ "$scenario" == many-queues ]]; then
    # Unbounded, a hundred producers outrun their consumers and the run
    # measures a backlog: these queues run at a set total rate, and the
    # rate received says whether the broker kept up.
    rate="$MANY_RATE"
  else
    for kind in artemis saranga; do
      reset "$kind"
      start "$kind" || exit 1
      perf "$scenario" "$kind" throughput
      read -r result received _ _ count < <(report "$WORK/runs/$scenario-$kind-throughput.json" || echo "fail 0 0 0 0")
      peak="$(rss_kb "$PID" VmHWM)"
      stop "$kind"
      read -r us cores < <(cpu_cost "$WORK/runs/$scenario-$kind-throughput.cpu" "$count")
      echo "$scenario $kind: $received msg/s ($result), peak ${peak} kB, $us us of CPU a message, $cores cores"
      record "$scenario" "$kind" thr_cpu_us "$us"
      [[ "$result" == success ]] || failed=$((failed + 1))
      thr[$kind]="$received"
      record "$scenario" "$kind" throughput "$received"
      record "$scenario" "$kind" peak_kb "$peak"
    done
    rate=$(( (thr[artemis] < thr[saranga] ? thr[artemis] : thr[saranga]) / 2 ))
    [[ "$rate" -ge 1 ]] || rate=1
  fi
  record "$scenario" both rate "$rate"
  for kind in artemis saranga; do
    reset "$kind"
    start "$kind" || exit 1
    perf "$scenario" "$kind" latency --rate "$rate"
    read -r result received p50 p99 count < <(report "$WORK/runs/$scenario-$kind-latency.json" || echo "fail 0 0 0 0")
    peak="$(rss_kb "$PID" VmHWM)"
    stop "$kind"
    read -r us cores < <(cpu_cost "$WORK/runs/$scenario-$kind-latency.cpu" "$count")
    echo "$scenario $kind at $rate msg/s: $received msg/s received, p50 ${p50} us, p99 ${p99} us, $us us of CPU a message, $cores cores ($result)"
    record "$scenario" "$kind" cpu_us "$us"
    record "$scenario" "$kind" cores "$cores"
    [[ "$result" == success ]] || failed=$((failed + 1))
    record "$scenario" "$kind" p50_us "$p50"
    record "$scenario" "$kind" p99_us "$p99"
    if [[ "$scenario" == many-queues ]]; then
      record "$scenario" "$kind" throughput "$received"
      record "$scenario" "$kind" peak_kb "$peak"
    fi
  done
  unset thr
done

# The tables.
perl -e '
  my %r; open my $f, "<", $ARGV[0] or die;
  while (<$f>) { chomp; my ($s, $k, $m, $v) = split /\t/; $r{$s}{$k}{$m} = $v; $o{$s} //= $n++ }
  sub mib { sprintf "%.0f MiB", $_[0] / 1024 }
  sub ratio { $_[1] ? sprintf("%.2f", $_[0] / $_[1]) : "-" }
  sub us { $_[0] >= 10000 ? sprintf("%.1f ms", $_[0] / 1000) : "$_[0] us" }
  print "| Startup | Artemis | Saranga |\n|---|---|---|\n";
  print "| Time to the acceptor (median) | $r{startup}{artemis}{ms} ms | $r{startup}{saranga}{ms} ms |\n";
  print "| Resident memory when started | ", mib($r{startup}{artemis}{idle_kb}), " | ", mib($r{startup}{saranga}{idle_kb}), " |\n\n";
  print "| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Same rate, msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis CPU a message | Saranga CPU a message | Artemis peak RSS | Saranga peak RSS |\n";
  print "|---|---|---|---|---|---|---|---|---|---|---|\n";
  for my $s (sort { $o{$a} <=> $o{$b} } grep { $_ ne "startup" } keys %r) {
    my ($a, $b) = ($r{$s}{artemis}, $r{$s}{saranga});
    printf "| %s | %d | %d | %s | %d | %s / %s | %s / %s | %d us | %d us | %s | %s |\n", $s,
      $a->{throughput}, $b->{throughput}, ratio($b->{throughput}, $a->{throughput}), $r{$s}{both}{rate},
      us($a->{p50_us}), us($a->{p99_us}), us($b->{p50_us}), us($b->{p99_us}),
      $a->{cpu_us}, $b->{cpu_us}, mib($a->{peak_kb}), mib($b->{peak_kb});
  }
' "$RESULTS" > "$WORK/results.md"
cat "$WORK/results.md"
[[ "$failed" -eq 0 ]] || { echo "FAILED: $failed runs did not succeed"; exit 1; }
