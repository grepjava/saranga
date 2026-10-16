#!/usr/bin/env bash
# The linearizability runs, Jepsen style: a primary and its backup, made by
# "artemis create" and run through their instances' own bin/artemis, with
# connections going through the chaos proxy (tools/saranga-chaos), which
# delays, black-holes, cuts, refuses and drops them at random throughout.
# Clients (tests/interop/chaos/History.java) send and receive in
# transactions and record each operation with its outcome, while a nemesis
# kills the primary (the backup becomes active, then the primary is started
# again and takes over) and the backup (started again, it rejoins), and
# a supervisor starts again whichever broker stops by itself, as one that
# loses its lock or its quorum does. Then the proxy heals, the pair must
# become whole, the producers stop, the consumers drain the queue, and
# "saranga-chaos check" compares the history with the model of a queue:
# every send whose commit returned is received, nothing is received twice
# unless the client was told the outcome was unknown, nothing is received
# whose send failed or before it was sent. Only safety fails a run: a pair
# that the faults keep from taking over during the rounds is noted.
#
#   scripts/linearizability.sh <shared-store|replication|pluggable> [A] [B]
#   scripts/linearizability.sh all
#
# A, the primary, and B, the backup, are saranga or artemis (default
# saranga). "all" runs the three pairs in the four pairings.
#
#   shared-store  A and B on one set of data directories; B takes the lock
#                 when A is killed and gives it back when A starts again.
#                 The cluster connections go through the proxy too.
#   replication   B replicates A and wins the quorum vote when A is killed;
#                 A, started again (check-for-active-server), copies B's
#                 data and takes its node back (allow-failback). The proxy
#                 only delays, unless FAULTS says otherwise: replication
#                 follows the connector each node announces, so it goes
#                 through the proxy with the clients, and a lone pair's
#                 vote needs no votes ("ignoring quorum vote as max cluster
#                 size is 1"), so B becomes active whenever its replication
#                 connection breaks, leaving two active brokers, in Artemis
#                 as in Saranga. Artemis's answer is the pluggable quorum.
#   pluggable     the same with a lock manager, Curator's ZooKeeper on one
#                 server: replication and each broker's ZooKeeper session go
#                 through the proxy too, so the pair is partitioned from
#                 each other and from ZooKeeper.
#
# ROUNDS (default 4) is how many nodes the nemesis kills, the primary in odd
# rounds and the backup in even ones, PAUSE (default 5) the seconds between
# and while a node is down; FAULTS (default cut,delay,blackhole,refuse,drop,
# delay alone for replication) the proxy's faults; SEED the proxy's seed (logged when not given);
# PRODUCERS and CONSUMERS (default 3 each) the client processes. KEEP=1
# keeps the work directory, which keeps the history.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HERE="$ROOT/tests/interop/chaos"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
CHAOS="$ROOT/target/release/saranga-chaos"
ROUNDS="${ROUNDS:-4}" PAUSE="${PAUSE:-5}"
PRODUCERS="${PRODUCERS:-3}" CONSUMERS="${CONSUMERS:-3}"
SCENARIOS="shared-store replication pluggable"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }
sep() { windows && echo ';' || echo ':'; }

SCENARIO="${1:?usage: linearizability.sh <shared-store|replication|pluggable|all> [A] [B]}"
if [[ "$SCENARIO" == all ]]; then
  failed=0
  for scenario in $SCENARIOS; do
    for a in saranga artemis; do
      for b in saranga artemis; do
        "$0" "$scenario" "$a" "$b" || failed=$((failed + 1))
      done
    done
  done
  [[ "$failed" -eq 0 ]] && echo "ALL PASSED (every pair, every pairing)" || echo "FAILED: $failed runs"
  exit "$((failed > 0))"
fi
[[ " $SCENARIOS " == *" $SCENARIO "* ]] || { echo "unknown scenario: $SCENARIO" >&2; exit 2; }
KIND_A="${2:-saranga}" KIND_B="${3:-saranga}"
# A lone replicated pair only takes faults that break no connection (see
# replication above).
[[ "$SCENARIO" == replication ]] && FAULTS="${FAULTS:-delay}" || FAULTS="${FAULTS:-cut,delay,blackhole,refuse,drop}"

cargo build -q --release -p saranga-cli -p saranga-chaos --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/chaos"
WORK="$(mktemp -d -p "$ROOT/target/chaos")"
A="$WORK/a" B="$WORK/b" ZK="$WORK/zookeeper"
# The brokers' ports, the proxy's routes to them, ZooKeeper's port and the
# routes to it, and the proxy's control port.
PORTA=64616 PORTB=64716 PROXYA=64620 PROXYB=64720 ZKPORT=64681 ZKA=64682 ZKB=64683 CONTROL=64699
PROXY_PID= CLIENT_PID= ZK_PID= STOPPED=

# As the cluster suites make an instance.
make_instance() {
  local kind="$1" dir="$2" offset="$3"; shift 3
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --host localhost "$@")
  case "$kind" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac
}

# The queue, which a message rolled back however often stays on.
add_queue() {
  perl -0pi -e 's{<addresses>}{<addresses>\n         <address name="q"><anycast><queue name="q"/></anycast></address>}' "$1/etc/broker.xml"
  perl -0pi -e 's{<address-settings>}{<address-settings>\n         <address-setting match="q"><max-delivery-attempts>-1</max-delivery-attempts></address-setting>}' "$1/etc/broker.xml"
}
# Makes the connector an instance announces its proxy route.
announce() {
  perl -0pi -e "s{<connector name=\"artemis\">tcp://localhost:\\d+</connector>}{<connector name=\"artemis\">tcp://127.0.0.1:$2</connector>}" "$1/etc/broker.xml"
}

# The jars ZooKeeper's server needs besides those Artemis ships, as the
# no-loss suite finds them.
zk_jar() {
  local group="$1" artifact="$2" version="$3"
  local path="${group//.//}/$artifact/$version/$artifact-$version.jar"
  local local_jar="$HOME/.m2/repository/$path" cached="$ROOT/target/ha/lib/$artifact-$version.jar"
  if [[ -f "$local_jar" ]]; then echo "$local_jar"; return; fi
  if [[ ! -f "$cached" ]]; then
    mkdir -p "$ROOT/target/ha/lib"
    curl -sfL -o "$cached" "https://repo1.maven.org/maven2/$path" || return 1
  fi
  echo "$cached"
}
zookeeper() {
  local metrics snappy
  metrics="$(zk_jar io.dropwizard.metrics metrics-core 3.2.5)" || return 1
  snappy="$(zk_jar org.xerial.snappy snappy-java 1.1.10.4)" || return 1
  mkdir -p "$ZK/data"
  printf 'tickTime=1000\ndataDir=%s\nclientPort=%s\nadmin.enableServer=false\n' \
    "$(native "$ZK/data")" "$ZKPORT" > "$ZK/zoo.cfg"
  java -cp "$(native "$DIST/lib")/*$(sep)$(native "$metrics")$(sep)$(native "$snappy")" \
    org.apache.zookeeper.server.ZooKeeperServerMain "$(native "$ZK/zoo.cfg")" > "$ZK/out" 2>&1 &
  ZK_PID=$!
}
stop_zookeeper() {
  [[ -z "$ZK_PID" ]] && return
  if windows && [[ -r "/proc/$ZK_PID/winpid" ]]; then
    taskkill //F //PID "$(cat "/proc/$ZK_PID/winpid")" > /dev/null 2>&1
  fi
  kill "$ZK_PID" 2>/dev/null
  ZK_PID=
}

instance() {
  local dir="$1"; shift
  if windows; then
    (cd "$dir/bin" && timeout 300 cmd /c call "$(cygpath -w "$dir/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$dir/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi | grep --line-buffered -v -E "NO_ARTEMIS_OOME_DUMP|operable program or batch file|^The system cannot find the file specified" \
    | perl -ne 'BEGIN { $| = 1 } s/\r\n\z/\n/; if (/^WARNING: /) { $w = 1; next } $b = $w && /^$/; $w = 0; print unless $b'
  return "${PIPESTATUS[0]}"
}

count() { local n; n="$(grep -c -- "$2" "$1" 2>/dev/null)"; echo "${n:-0}"; }
# "artemis run --allow-kill" in the background: a line is added to the
# instance's launched file, and to its exited file when it ends (see
# wait_exit in the failback suite); its log's length so far is kept, so that
# its state is read from what this run logs.
launch() {
  local dir="$1"
  mkdir -p "$dir/log"
  count "$dir/log/artemis.log" "" > "$dir/mark"
  echo >> "$dir/launched"
  if windows; then
    (cd "$dir/bin" && { cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run --allow-kill >> "$dir/run.out" 2>&1; echo >> "$dir/exited"; } &)
  else
    (cd "$dir/bin" && { ./artemis run --allow-kill >> "$dir/run.out" 2>&1; echo >> "$dir/exited"; } &)
  fi
}
alive() { [[ "$(count "$1/launched" "")" -gt "$(count "$1/exited" "")" ]]; }
up() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_up() {
  for _ in $(seq 1 300); do up "$1" && return 0; sleep 0.2; done
  return 1
}
# Waits until each run of an instance has ended.
wait_dead() {
  for _ in $(seq 1 600); do alive "$1" || return 0; sleep 0.2; done
  return 1
}
# What an instance's current run last logged it was: active (AMQ221007, or
# AMQ221010 for a shared-store backup), backup (announced, AMQ221031),
# stopped (AMQ221002), or starting. A backup that fails back stops and
# starts again within its run.
state() {
  local last
  last="$(tail -n +"$(($(cat "$1/mark" 2>/dev/null || echo 0) + 1))" "$1/log/artemis.log" 2>/dev/null \
    | grep -o -E 'AMQ22(1000|1109|1007|1010|1031|1002)' | tail -1)"
  case "$last" in
    AMQ221007|AMQ221010) echo active ;;
    AMQ221031) echo backup ;;
    AMQ221002) echo stopped ;;
    *) echo starting ;;
  esac
}
wait_state() {
  for _ in $(seq 1 600); do [[ "$(state "$1")" == "$2" ]] && return 0; sleep 0.2; done
  return 1
}
kill_instance() {
  alive "$1" && instance "$1" kill > /dev/null
  wait_dead "$1"
}
# What a service manager does for the pair: an instance whose run ended (a
# broker that loses its lock, its quorum or its disk stops, and its JVM may
# end) is started again, and one stopped for ten seconds while its process
# lives on is killed and started again. Called every 0.2 s.
declare -A STOPPED_TICKS
supervise() {
  local i
  for i in "$A" "$B"; do
    if ! alive "$i"; then
      echo "supervisor: $(basename "$i") ended, started again"
      launch "$i"
      STOPPED_TICKS[$i]=0
    elif [[ "$(state "$i")" == stopped ]]; then
      STOPPED_TICKS[$i]=$((${STOPPED_TICKS[$i]:-0} + 1))
      if [[ "${STOPPED_TICKS[$i]}" -ge 50 ]]; then
        echo "supervisor: $(basename "$i") stopped, killed and started again"
        kill_instance "$i"
        launch "$i"
        STOPPED_TICKS[$i]=0
      fi
    else
      STOPPED_TICKS[$i]=0
    fi
  done
}
# Supervises the pair until A is active and B its backup, for up to the
# given seconds (default 180).
whole() {
  local ticks=$((${1:-180} * 5))
  for _ in $(seq 1 "$ticks"); do
    supervise
    [[ "$(state "$A")" == active && "$(state "$B")" == backup ]] && return 0
    sleep 0.2
  done
  echo "the pair is not whole: a is $(state "$A"), b is $(state "$B")"
  return 1
}

chaos() { "$CHAOS" ctl "127.0.0.1:$CONTROL" "$@" > /dev/null; }
stop_proxy() {
  [[ -z "$PROXY_PID" ]] && return
  kill "$PROXY_PID" 2>/dev/null
  wait "$PROXY_PID" 2>/dev/null
  PROXY_PID=
}
# Stops the pair, the backup first, before the proxy, whose end would
# partition them.
stop_pair() {
  local i
  for i in "$B" "$A"; do
    [[ -d "$i/bin" ]] && alive "$i" && instance "$i" stop > /dev/null 2>&1
    [[ -d "$i/bin" ]] && { wait_dead "$i" || return 1; }
  done
  return 0
}
cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  [[ -z "$STOPPED" ]] && stop_pair
  stop_proxy
  stop_zookeeper
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

step() { echo "=== $*"; }
fail() { echo "FAILED: $*"; FAILED=1; }
FAILED=0

ROUTES=()
prepare() {
  local a="tcp://127.0.0.1:$PROXYA" b="tcp://127.0.0.1:$PROXYB"
  ROUTES=("a=127.0.0.1:$PROXYA,127.0.0.1:$PORTA" "b=127.0.0.1:$PROXYB,127.0.0.1:$PORTB")
  case "$SCENARIO" in
    shared-store)
      make_instance "$KIND_A" "$A" 3000 --shared-store --static-cluster "$b" --cluster-user cu --cluster-password cpw
      make_instance "$KIND_B" "$B" 3100 --shared-store --backup --static-cluster "$a" --cluster-user cu --cluster-password cpw
      local shared
      shared="$(native "$WORK/shared")"
      sed -i -E -e "s#<(paging|bindings|journal|large-messages)-directory>[^<]*<#<\1-directory>$shared/\1<#" \
        "$A/etc/broker.xml" "$B/etc/broker.xml"
      ;;
    replication)
      make_instance "$KIND_A" "$A" 3000 --replicated --static-cluster "tcp://127.0.0.1:$PORTB" --cluster-user cu --cluster-password cpw
      make_instance "$KIND_B" "$B" 3100 --replicated --backup --static-cluster "tcp://127.0.0.1:$PORTA" --cluster-user cu --cluster-password cpw
      perl -0pi -e 's|(<vote-on-replication-failure>true</vote-on-replication-failure>)|$1<check-for-active-server>true</check-for-active-server>|' "$A/etc/broker.xml"
      perl -0pi -e 's|<backup/>|<backup><allow-failback>true</allow-failback></backup>|' "$B/etc/broker.xml"
      ;;
    pluggable)
      make_instance "$KIND_A" "$A" 3000 --replicated --static-cluster "$b" --cluster-user cu --cluster-password cpw
      make_instance "$KIND_B" "$B" 3100 --replicated --backup --static-cluster "$a" --cluster-user cu --cluster-password cpw
      local manager='<manager><properties><property key="connect-string" value="127.0.0.1:PORT"/><property key="session-ms" value="4000"/></properties></manager>'
      perl -0pi -e "s|<primary>\\s*<vote-on-replication-failure>true</vote-on-replication-failure>\\s*</primary>|<primary>${manager/PORT/$ZKA}</primary>|" "$A/etc/broker.xml"
      perl -0pi -e "s|<backup/>|<backup>${manager/PORT/$ZKB}<allow-failback>true</allow-failback></backup>|" "$B/etc/broker.xml"
      ROUTES+=("zka=127.0.0.1:$ZKA,127.0.0.1:$ZKPORT" "zkb=127.0.0.1:$ZKB,127.0.0.1:$ZKPORT")
      zookeeper && wait_up "$ZKPORT" || fail "zookeeper is not listening"
      ;;
  esac
  add_queue "$A"
  add_queue "$B"
  announce "$A" "$PROXYA"
  announce "$B" "$PROXYB"
}

# The nemesis's rounds: the primary killed and started again in odd ones,
# the backup in even ones, each waited for until the pair is whole again.
nemesis() {
  local round
  for round in $(seq 1 "$ROUNDS"); do
    sleep "$PAUSE"
    # Only what the history shows is checked: a pair the faults keep from
    # taking over or from becoming whole again is noted, and the rounds go
    # on.
    if ((round % 2)); then
      step "round $round: artemis kill (a, the primary)"
      kill_instance "$A" || { fail "a was not killed"; return; }
      if wait_state "$B" active; then
        echo "b: active"
      else
        echo "note: b did not become active (it is $(state "$B"))"
      fi
      sleep "$PAUSE"
      step "round $round: artemis run (a)"
    else
      step "round $round: artemis kill (b, the backup)"
      kill_instance "$B" || { fail "b was not killed"; return; }
      sleep "$PAUSE"
      step "round $round: artemis run (b)"
    fi
    if whole; then
      echo "a: active, b: its backup"
    else
      echo "note: not whole after round $round"
    fi
  done
}

run() {
  local out="$WORK/client.out" mark="$WORK/stop" history="$WORK/history.txt" seed=()
  [[ -n "${SEED:-}" ]] && seed=(--seed "$SEED")
  prepare
  step "$SCENARIO ($KIND_A a, $KIND_B b): the proxy, a and b"
  "$CHAOS" --control "127.0.0.1:$CONTROL" --random "$FAULTS" --every 2s..6s --hold 1s..4s \
    "${seed[@]}" "${ROUTES[@]}" > "$WORK/proxy.log" 2>&1 &
  PROXY_PID=$!
  wait_up "$CONTROL" || fail "the proxy is not listening"
  chaos random off
  grep "^seed " "$WORK/proxy.log"
  launch "$A"
  wait_state "$A" active && echo "a: active"
  launch "$B"
  wait_state "$B" backup && echo "b: its backup"

  step "History.java run, $PRODUCERS producers and $CONSUMERS consumers"
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/History.java")" run \
    "tcp://127.0.0.1:$PROXYA?ha=true&reconnectAttempts=-1&retryInterval=500" q \
    "$(native "$mark")" "$(native "$history")" "$PRODUCERS" "$CONSUMERS" 2>&1 \
    | grep --line-buffered -E "^(enqueued|done|corrupt|failed)" > "$out" &
  CLIENT_PID=$!
  for _ in $(seq 1 600); do grep -q "^enqueued 100$" "$out" && break; sleep 0.2; done
  grep -q "^enqueued 100$" "$out" || fail "the client sent nothing"
  chaos random on
  nemesis
  sleep "$PAUSE"
  chaos random off
  echo "faults: $(grep -c -E ' (cut|delay|blackhole|refuse|drop|dropped)' "$WORK/proxy.log")" \
    "($(grep -o -E '^ *[0-9.]+ [a-z]+: (cut|delay|blackhole|refuse|drop|dropped)' "$WORK/proxy.log" \
      | awk '{print $3}' | sort | uniq -c | awk '{printf "%s%s %s", sep, $1, $2; sep=", "}'))"
  echo "became active: a $(grep -c -E 'AMQ2210(07|10)' "$A/log/artemis.log") times," \
    "b $(grep -c -E 'AMQ2210(07|10)' "$B/log/artemis.log") times, for $(((ROUNDS + 1) / 2)) kills of a"

  step "healed; the pair whole, the producers stopped, the queue drained"
  whole || fail "the pair did not become whole once healed"
  touch "$mark"
  wait "$CLIENT_PID"
  CLIENT_PID=
  grep -E "^(done|corrupt|failed):" "$out"
  grep -q "^corrupt:" "$out" && fail "a message arrived corrupt"
  grep -q "^failed:" "$out" && fail "a client process failed"

  step "saranga-chaos check"
  "$CHAOS" check "$(native "$history")" || fail "the history is not valid"

  step "artemis stop (b, a)"
  stop_pair || fail "the pair did not stop"
  STOPPED=1
}

run
if [[ "$FAILED" -eq 0 ]]; then
  echo "PASSED ($SCENARIO, $KIND_A a, $KIND_B b)"
else
  KEEP=1
  echo "FAILED ($SCENARIO, $KIND_A a, $KIND_B b; kept $WORK)"
  exit 1
fi
