#!/usr/bin/env bash
# The network chaos soak: two brokers, made by "artemis create" and run
# through their instances' own bin/artemis, whose links go through the chaos
# proxy (tools/saranga-chaos), which delays, black-holes, cuts, refuses and
# drops their connections at random while tests/interop/chaos/Chaos.java
# produces and consumes in transactions. Then the proxy heals, the producer
# stops and the consumer receives the rest: every message whose transaction
# committed must be received, and none twice.
#
#   scripts/chaos-soak.sh <scenario> [A] [B]
#   scripts/chaos-soak.sh all
#
# A and B are saranga or artemis (default saranga). "all" runs every
# scenario in the four pairings. Scenarios:
#
#   bridge       A's core bridge forwards queue q to B through the proxy;
#                the producer is on A, the consumer on B.
#   cluster      A and B in a static cluster, ON_DEMAND with redistribution,
#                each announcing its proxy route, so both cluster bridges go
#                through the proxy; the producer is on A, the consumer on B.
#   federation   B federates queue q from upstream A through the proxy; the
#                producer is on A, the consumer on B.
#   mirror       A mirrors to B over an AMQP broker connection through the
#                proxy; the producer is on A, the consumer on B.
#   shared-store A primary and B its backup on one set of data directories;
#                the clients connect through the proxy, with ha=true.
#   replication  A primary and B its replicating backup, each announcing its
#                proxy route, so replication and the clients' connections
#                go through the proxy, which only delays them unless FAULTS
#                says otherwise: a lone pair's quorum vote needs no votes,
#                so a broken replication connection leaves two active
#                brokers, in Artemis as in Saranga (scripts/linearizability.sh
#                partitions a pair with a lock manager instead).
#
# DURATION (default 120) is the seconds of chaos; FAULTS (default
# cut,delay,blackhole,refuse,drop, delay alone for replication) the faults;
# SEED the proxy's seed (logged when not given); LARGE_EVERY (default 25)
# makes every n-th message large.
# KEEP=1 keeps the work directory.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HERE="$ROOT/tests/interop/chaos"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
CHAOS="$ROOT/target/release/saranga-chaos"
DURATION="${DURATION:-120}"
LARGE_GIVEN="${LARGE_EVERY+set}"
LARGE_EVERY="${LARGE_EVERY:-25}"
SCENARIOS="bridge cluster federation mirror shared-store replication"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

SCENARIO="${1:?usage: chaos-soak.sh <scenario|all> [A] [B]}"
if [[ "$SCENARIO" == all ]]; then
  failed=0
  for scenario in $SCENARIOS; do
    for a in saranga artemis; do
      for b in saranga artemis; do
        "$0" "$scenario" "$a" "$b" || failed=$((failed + 1))
      done
    done
  done
  [[ "$failed" -eq 0 ]] && echo "ALL PASSED (every scenario, every pairing)" || echo "FAILED: $failed runs"
  exit "$((failed > 0))"
fi
[[ " $SCENARIOS " == *" $SCENARIO "* ]] || { echo "unknown scenario: $SCENARIO" >&2; exit 2; }
KIND_A="${2:-saranga}" KIND_B="${3:-saranga}"
[[ "$SCENARIO" == replication ]] && FAULTS="${FAULTS:-delay}" || FAULTS="${FAULTS:-cut,delay,blackhole,refuse,drop}"
# What a backup logs when it becomes active: a shared-store backup
# AMQ221010, a replicating one AMQ221007, as a primary does.
[[ "$SCENARIO" == shared-store ]] && ACTIVE=AMQ221010 || ACTIVE=AMQ221007
# Artemis's Core queue federation never returns the credit of the large
# message chunks it reads (LargeMessageControllerImpl.take), so an Artemis
# downstream stops for good once a consumer window of them has passed:
# docs/upstream/artemis-issues.md, issue 18.
if [[ "$SCENARIO" == federation && "$KIND_B" == artemis && -z "$LARGE_GIVEN" ]]; then
  LARGE_EVERY=0
  echo "no large messages: an Artemis downstream stalls on them (issue 18)"
fi

cargo build -q --release -p saranga-cli -p saranga-chaos --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/chaos"
WORK="$(mktemp -d -p "$ROOT/target/chaos")"
A="$WORK/a" B="$WORK/b"
# The brokers' ports, the proxy's routes to them and its control port.
PORTA=64416 PORTB=64516 PROXYA=64420 PROXYB=64520 CONTROL=64499
PROXY_PID= CLIENT_PID=

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

# Puts a fragment in <core>, after the acceptors.
add_core() {
  XML="$2" perl -0pi -e 's{(</acceptors>)}{$1\n$ENV{XML}}' "$1/etc/broker.xml"
}
# Puts a fragment in <addresses> and one in <address-settings>.
add_queue() {
  perl -0pi -e 's{<addresses>}{<addresses>\n         <address name="q"><anycast><queue name="q"/></anycast></address>}' "$1/etc/broker.xml"
  [[ -n "${2:-}" ]] && XML="$2" perl -0pi -e 's{<address-settings>}{<address-settings>\n$ENV{XML}}' "$1/etc/broker.xml"
}
# Makes the connector an instance announces its proxy route.
announce() {
  perl -0pi -e "s{<connector name=\"artemis\">tcp://localhost:\\d+</connector>}{<connector name=\"artemis\">tcp://127.0.0.1:$2</connector>}" "$1/etc/broker.xml"
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

# "artemis run --allow-kill" in the background; a line is added to the
# instance's exited file when it ends (see wait_exit in the failback suite).
launch() {
  local dir="$1"
  mkdir -p "$dir/log"
  if windows; then
    (cd "$dir/bin" && { cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run --allow-kill >> "$dir/run.out" 2>&1; echo >> "$dir/exited"; } &)
  else
    (cd "$dir/bin" && { ./artemis run --allow-kill >> "$dir/run.out" 2>&1; echo >> "$dir/exited"; } &)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_up() {
  for _ in $(seq 1 300); do up "$1" && return 0; sleep 0.2; done
  return 1
}
# Waits for the n-th line matching text (default the first).
wait_for() {
  local file="$1" text="$2" n="${3:-1}"
  for _ in $(seq 1 600); do
    [[ "$(grep -c -- "$text" "$file" 2>/dev/null)" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}
# Waits for an instance's n-th artemis run (default the first) to end.
wait_exit() {
  local dir="$1" n="${2:-1}"
  for _ in $(seq 1 600); do
    [[ "$(grep -c "" "$dir/exited" 2>/dev/null)" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}

chaos() { "$CHAOS" ctl "127.0.0.1:$CONTROL" "$@" > /dev/null; }
stop_proxy() {
  [[ -z "$PROXY_PID" ]] && return
  kill "$PROXY_PID" 2>/dev/null
  wait "$PROXY_PID" 2>/dev/null
  PROXY_PID=
}
cleanup() {
  [[ -n "$CLIENT_PID" ]] && kill "$CLIENT_PID" 2>/dev/null
  stop_proxy
  for i in "$A" "$B"; do
    [[ -d "$i/bin" ]] && instance "$i" stop > /dev/null 2>&1
  done
  for i in "$A" "$B"; do
    [[ -d "$i/log" ]] && wait_exit "$i"
  done
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

step() { echo "=== $*"; }
fail() { echo "FAILED: $*"; FAILED=1; }
FAILED=0

# The instances of a scenario, the proxy's routes, the clients' URLs and how
# to know the brokers are ready.
ROUTES=() PRODUCER= CONSUMER=
prepare() {
  local a="tcp://localhost:$PORTA" b="tcp://localhost:$PORTB"
  case "$SCENARIO" in
    bridge)
      make_instance "$KIND_A" "$A" 2800
      make_instance "$KIND_B" "$B" 2900
      add_queue "$A"
      add_queue "$B"
      add_core "$A" "      <connectors><connector name=\"target\">tcp://127.0.0.1:$PROXYB</connector></connectors>
      <bridges>
         <bridge name=\"br\">
            <queue-name>q</queue-name>
            <retry-interval>500</retry-interval>
            <reconnect-attempts>-1</reconnect-attempts>
            <user>admin</user>
            <password>admin</password>
            <static-connectors><connector-ref>target</connector-ref></static-connectors>
         </bridge>
      </bridges>"
      ROUTES=("b=127.0.0.1:$PROXYB,127.0.0.1:$PORTB")
      PRODUCER="$a" CONSUMER="$b"
      ;;
    cluster)
      make_instance "$KIND_A" "$A" 2800 --clustered --static-cluster "tcp://127.0.0.1:$PROXYB" --cluster-user cu --cluster-password cpw
      make_instance "$KIND_B" "$B" 2900 --clustered --static-cluster "tcp://127.0.0.1:$PROXYA" --cluster-user cu --cluster-password cpw
      local redistribute='         <address-setting match="q"><redistribution-delay>0</redistribution-delay></address-setting>'
      add_queue "$A" "$redistribute"
      add_queue "$B" "$redistribute"
      announce "$A" "$PROXYA"
      announce "$B" "$PROXYB"
      ROUTES=("a=127.0.0.1:$PROXYA,127.0.0.1:$PORTA" "b=127.0.0.1:$PROXYB,127.0.0.1:$PORTB")
      PRODUCER="$a" CONSUMER="$b"
      ;;
    federation)
      make_instance "$KIND_A" "$A" 2800
      make_instance "$KIND_B" "$B" 2900
      add_queue "$A"
      add_queue "$B"
      add_core "$B" "      <connectors><connector name=\"up\">tcp://127.0.0.1:$PROXYA</connector></connectors>
      <federations>
         <federation name=\"fed\">
            <upstream name=\"a\" user=\"admin\" password=\"admin\">
               <circuit-breaker-timeout>1000</circuit-breaker-timeout>
               <retry-interval>500</retry-interval>
               <reconnect-attempts>-1</reconnect-attempts>
               <static-connectors><connector-ref>up</connector-ref></static-connectors>
               <policy ref=\"qp\"/>
            </upstream>
            <queue-policy name=\"qp\"><include queue-match=\"q\" address-match=\"#\"/></queue-policy>
         </federation>
      </federations>"
      ROUTES=("a=127.0.0.1:$PROXYA,127.0.0.1:$PORTA")
      PRODUCER="$a" CONSUMER="$b"
      ;;
    mirror)
      make_instance "$KIND_A" "$A" 2800
      make_instance "$KIND_B" "$B" 2900
      add_queue "$A"
      add_queue "$B"
      add_core "$A" "      <broker-connections>
         <amqp-connection uri=\"tcp://127.0.0.1:$PROXYB\" name=\"m\" user=\"admin\" password=\"admin\" retry-interval=\"500\">
            <mirror/>
         </amqp-connection>
      </broker-connections>"
      ROUTES=("b=127.0.0.1:$PROXYB,127.0.0.1:$PORTB")
      PRODUCER="$a" CONSUMER="$b"
      ;;
    shared-store)
      make_instance "$KIND_A" "$A" 2800 --shared-store --static-cluster "tcp://localhost:$PORTB" --cluster-user cu --cluster-password cpw
      make_instance "$KIND_B" "$B" 2900 --shared-store --backup --static-cluster "tcp://localhost:$PORTA" --cluster-user cu --cluster-password cpw
      local shared
      shared="$(native "$WORK/shared")"
      sed -i -E -e "s#<(paging|bindings|journal|large-messages)-directory>[^<]*<#<\1-directory>$shared/\1<#" \
        "$A/etc/broker.xml" "$B/etc/broker.xml"
      add_queue "$A"
      add_queue "$B"
      announce "$A" "$PROXYA"
      announce "$B" "$PROXYB"
      ROUTES=("a=127.0.0.1:$PROXYA,127.0.0.1:$PORTA" "b=127.0.0.1:$PROXYB,127.0.0.1:$PORTB")
      PRODUCER="tcp://127.0.0.1:$PROXYA?ha=true&reconnectAttempts=-1&retryInterval=500" CONSUMER="$PRODUCER"
      ;;
    replication)
      make_instance "$KIND_A" "$A" 2800 --replicated --static-cluster "tcp://127.0.0.1:$PROXYB" --cluster-user cu --cluster-password cpw
      make_instance "$KIND_B" "$B" 2900 --replicated --backup --static-cluster "tcp://127.0.0.1:$PROXYA" --cluster-user cu --cluster-password cpw
      perl -0pi -e 's|(<vote-on-replication-failure>true</vote-on-replication-failure>)|$1<check-for-active-server>true</check-for-active-server>|' "$A/etc/broker.xml"
      perl -0pi -e 's|<backup/>|<backup><allow-failback>true</allow-failback></backup>|' "$B/etc/broker.xml"
      add_queue "$A"
      add_queue "$B"
      announce "$A" "$PROXYA"
      announce "$B" "$PROXYB"
      ROUTES=("a=127.0.0.1:$PROXYA,127.0.0.1:$PORTA" "b=127.0.0.1:$PROXYB,127.0.0.1:$PORTB")
      PRODUCER="tcp://127.0.0.1:$PROXYA?ha=true&reconnectAttempts=-1&retryInterval=500" CONSUMER="$PRODUCER"
      ;;
  esac
}

# Waits until the brokers are ready for the load.
ready() {
  case "$SCENARIO" in
    bridge)
      wait_for "$A/log/artemis.log" AMQ221027 && echo "a: bridged to b" ;;
    cluster)
      wait_for "$A/log/artemis.log" AMQ221027 && echo "a: bridged to b"
      wait_for "$B/log/artemis.log" AMQ221027 && echo "b: bridged to a" ;;
    federation)
      sleep 2 && echo "b: federating from a" ;;
    mirror)
      wait_for "$A/log/artemis.log" AMQ111003 && echo "a: connected to b" ;;
    shared-store)
      wait_for "$A/log/artemis.log" AMQ221007 && echo "a: active"
      wait_for "$B/log/artemis.log" AMQ221031 && echo "b: announced" ;;
    replication)
      wait_for "$A/log/artemis.log" AMQ221007 && echo "a: active"
      wait_for "$B/log/artemis.log" AMQ221024 && echo "b: synchronized"
      wait_for "$B/log/artemis.log" AMQ221031 && echo "b: announced" ;;
  esac
}

committed() {
  local n
  n="$(grep -o -E "^committed [0-9]+$" "$1" | tail -1 | grep -o -E "[0-9]+")"
  echo "${n:-0}"
}
wait_committed() {
  for _ in $(seq 1 900); do
    [[ "$(committed "$1")" -ge "$2" ]] && return 0
    sleep 0.2
  done
  return 1
}

soak() {
  local out="$WORK/client.out" mark="$WORK/stop" seed=()
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
  wait_up "$PORTA" || fail "a is not listening"
  launch "$B"
  if [[ "$SCENARIO" != shared-store && "$SCENARIO" != replication ]]; then
    wait_up "$PORTB" || fail "b is not listening"
  fi
  ready

  step "Chaos.java load, $DURATION s of chaos"
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Chaos.java")" \
    load "$PRODUCER" "$CONSUMER" q "$(native "$mark")" "$LARGE_EVERY" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)" > "$out" &
  CLIENT_PID=$!
  wait_committed "$out" 200 || fail "the client committed nothing"
  chaos random on
  sleep "$DURATION"
  chaos random off
  echo "faults: $(grep -c -E ' (cut|delay|blackhole|refuse|drop|dropped)' "$WORK/proxy.log")" \
    "($(grep -o -E '^ *[0-9.]+ [a-z]+: (cut|delay|blackhole|refuse|drop|dropped)' "$WORK/proxy.log" \
      | awk '{print $3}' | sort | uniq -c | awk '{printf "%s%s %s", sep, $1, $2; sep=", "}'))"

  if [[ "$SCENARIO" == shared-store || "$SCENARIO" == replication ]]; then
    echo "b: became active $(grep -c "$ACTIVE" "$B/log/artemis.log" 2>/dev/null) times"
  fi

  step "healed; the producer stopped, the rest received"
  touch "$mark"
  wait "$CLIENT_PID"
  CLIENT_PID=
  grep -E "^(lost|received twice|corrupt|done):|^committed [0-9]+, received" "$out"
  grep -q -E "^done: lost 0, received twice 0$" "$out" || fail "the client lost or repeated messages"
  grep -q "^corrupt:" "$out" && fail "a large message arrived corrupt"
  grep -q "^failed:" "$out" && { fail "the client failed"; cat "$out"; }

  step "artemis stop (b, a)"
  stop_proxy
  instance "$B" stop > /dev/null
  instance "$A" stop > /dev/null
  wait_exit "$B" || fail "b did not stop"
  wait_exit "$A" || fail "a did not stop"
}

soak
if [[ "$FAILED" -eq 0 ]]; then
  echo "PASSED ($SCENARIO, $KIND_A a, $KIND_B b)"
else
  KEEP=1
  echo "FAILED ($SCENARIO, $KIND_A a, $KIND_B b; kept $WORK)"
  exit 1
fi
