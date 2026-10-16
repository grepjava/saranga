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
# MANY_RATE messages a second in all, default 10000, rather than unbounded).
#
# DURATION (default 60) and WARMUP (default 15) are each run's seconds;
# STARTS (default 5); NUM_QUEUES (default 100); MAX_PENDING (default 100),
# the sends a producer has outstanding; BROKER_CPUS and CLIENT_CPUS (default
# the first and second half of the CPUs) the cores taskset gives each;
# WORK (default target/bench) where the brokers and results go, which
# should be on a local file system; ARTEMIS_DIST the distribution. The
# results are written to $WORK/results.md and $WORK/results.tsv.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
WORK="${WORK:-$ROOT/target/bench}"
DURATION="${DURATION:-60}" WARMUP="${WARMUP:-15}" STARTS="${STARTS:-5}"
NUM_QUEUES="${NUM_QUEUES:-100}" MANY_RATE="${MANY_RATE:-10000}" MAX_PENDING="${MAX_PENDING:-100}"
CPUS="$(nproc)"
BROKER_CPUS="${BROKER_CPUS:-0-$((CPUS / 2 - 1))}"
CLIENT_CPUS="${CLIENT_CPUS:-$((CPUS / 2))-$((CPUS - 1))}"
PORT=61616
ALL="core core-persistent amqp amqp-persistent core-large amqp-large many-queues"
SCENARIOS="${*:-$ALL}"
for s in $SCENARIOS; do
  [[ " $ALL " == *" $s "* ]] || { echo "unknown scenario: $s" >&2; exit 2; }
done
[[ "$(uname -s)" == Linux ]] || { echo "bench.sh runs on Linux" >&2; exit 2; }
[[ -d "$DIST/lib" ]] || { echo "no Artemis distribution at $DIST (scripts/build-artemis.sh)" >&2; exit 2; }

cargo build -q --release -p saranga-cli --manifest-path "$ROOT/Cargo.toml" || exit 1
# Both brokers run from the same file system, so neither loads its code
# from a slower one.
rm -rf "$WORK/brokers" "$WORK/runs"
mkdir -p "$WORK/brokers" "$WORK/runs"
cp -r "$DIST" "$WORK/brokers/dist"
mkdir -p "$WORK/brokers/bin" && cp "$TARGET/release/saranga" "$WORK/brokers/bin/saranga"
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
    --max-pending "$MAX_PENDING" --duration "$DURATION" --warmup "$WARMUP" \
    --json "$out.json" "$@" queue://BENCH > "$out.out" 2>&1
}

# From a perf client's JSON report: the mean received rate after the
# warmup, and the summary's transfer p50 and p99 in microseconds.
report() {
  perl -MJSON::PP -e '
    local $/; open my $f, "<", $ARGV[0] or exit 1; my $s = decode_json(<$f>);
    my @r = map { $_->{received} } grep { $_->{sampleType} eq "interval" && !$_->{warmup} } @$s;
    my ($sum) = (0); $sum += $_ for @r;
    my ($summary) = grep { $_->{sampleType} eq "summary" } @$s;
    my $t = $summary->{totalTransferTime} // {};
    printf "%s %d %d %d\n", $summary->{result}, @r ? $sum / @r : 0, $t->{50} // 0, $t->{99} // 0;
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
      read -r result received _ _ < <(report "$WORK/runs/$scenario-$kind-throughput.json" || echo "fail 0 0 0")
      peak="$(rss_kb "$PID" VmHWM)"
      stop "$kind"
      echo "$scenario $kind: $received msg/s ($result), peak ${peak} kB"
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
    read -r result received p50 p99 < <(report "$WORK/runs/$scenario-$kind-latency.json" || echo "fail 0 0 0")
    peak="$(rss_kb "$PID" VmHWM)"
    stop "$kind"
    echo "$scenario $kind at $rate msg/s: $received msg/s received, p50 ${p50} us, p99 ${p99} us ($result)"
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
  print "| Scenario | Artemis msg/s | Saranga msg/s | Saranga/Artemis | Latency at msg/s | Artemis p50 / p99 | Saranga p50 / p99 | Artemis peak RSS | Saranga peak RSS |\n";
  print "|---|---|---|---|---|---|---|---|---|\n";
  for my $s (sort { $o{$a} <=> $o{$b} } grep { $_ ne "startup" } keys %r) {
    my ($a, $b) = ($r{$s}{artemis}, $r{$s}{saranga});
    printf "| %s | %d | %d | %s | %d | %s / %s | %s / %s | %s | %s |\n", $s,
      $a->{throughput}, $b->{throughput}, ratio($b->{throughput}, $a->{throughput}), $r{$s}{both}{rate},
      us($a->{p50_us}), us($a->{p99_us}), us($b->{p50_us}), us($b->{p99_us}), mib($a->{peak_kb}), mib($b->{peak_kb});
  }
' "$RESULTS" > "$WORK/results.md"
cat "$WORK/results.md"
[[ "$failed" -eq 0 ]] || { echo "FAILED: $failed runs did not succeed"; exit 1; }
