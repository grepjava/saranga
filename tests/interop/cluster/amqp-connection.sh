#!/usr/bin/env bash
# The AMQP broker connection suite: two instances made by "artemis create"
# (not clustered), each run through its own bin/artemis, and the Artemis
# JMS client. Node 1 has an <amqp-connection> to node 2 with a <sender>
# for "outbox" and a <receiver> for "inbox". The output, normalized, must
# match tests/interop/cluster/amqp-connection.expected.txt (recorded with
# Artemis for both nodes).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis tests/interop/cluster/amqp-connection.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. Node 1 connects to node 2 (AMQ111003), its receiver consuming
#      node 2's "inbox".
#   2. Messages sent to "outbox" on node 1 go to "outbox" on node 2, and
#      those sent to "inbox" on node 2 come to "inbox" on node 1.
#   3. Management lists the broker connection, stops it (node 2's "inbox"
#      loses its consumer) and starts it again.
#   4. Node 2 stops: node 1 retries (AMQ111002) and connects again when
#      node 2 is back.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/cluster"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
NODE1="${NODE1:-saranga}" NODE2="${NODE2:-saranga}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/cluster"
WORK="$(mktemp -d -p "$ROOT/target/cluster")"
N1="$WORK/node1" N2="$WORK/node2"
PORT1=65466 PORT2=65496
cleanup() {
  for i in "$N1" "$N2"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

make_instance() {
  local kind="$1" dir="$2" offset="$3"
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --host localhost)
  case "$kind" in
    saranga) "$SARANGA" create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
    artemis) java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$dir")" "${opts[@]}" > /dev/null 2>&1 ;;
  esac
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

launch() {
  local dir="$1"
  mkdir -p "$dir/log"
  if windows; then
    (cd "$dir/bin" && cmd /c "$(cygpath -w "$dir/bin/artemis.cmd")" run --allow-kill >> "$dir/run.out" 2>&1 &)
  else
    (cd "$dir/bin" && ./artemis run --allow-kill >> "$dir/run.out" 2>&1 &)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_up() {
  for _ in $(seq 1 300); do up "$1" && return 0; sleep 0.2; done
  return 1
}
wait_down() {
  for _ in $(seq 1 300); do up "$1" || return 0; sleep 0.2; done
  return 1
}
wait_for() {
  local file="$1" text="$2" n="${3:-1}"
  for _ in $(seq 1 600); do
    [[ "$(grep -c -- "$text" "$file" 2>/dev/null)" -ge "$n" ]] && return 0
    sleep 0.2
  done
  return 1
}

step() { echo "=== $*"; }
client() {
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/BrokerConnection.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

U1="tcp://localhost:$PORT1" U2="tcp://localhost:$PORT2"

# Puts a fragment in <core>, after the acceptors.
add_core() {
  local dir="$1" xml="$2"
  XML="$xml" perl -0pi -e 's{(</acceptors>)}{$1\n$ENV{XML}}' "$dir/etc/broker.xml"
}

# Adds anycast addresses and their queues.
add_queues() {
  local dir="$1"; shift
  local xml=""
  for q in "$@"; do
    xml="$xml         <address name=\"$q\"><anycast><queue name=\"$q\"/></anycast></address>
"
  done
  XML="$xml" perl -0pi -e 's{(<addresses>\r?\n)}{$1$ENV{XML}}' "$dir/etc/broker.xml"
}

configure() {
  add_core "$N1" "      <broker-connections>
         <amqp-connection uri=\"tcp://localhost:$PORT2\" name=\"to-node2\" user=\"admin\" password=\"admin\" retry-interval=\"500\">
            <sender address-match=\"outbox\"/>
            <receiver queue-name=\"inbox\"/>
         </amqp-connection>
      </broker-connections>"
  add_queues "$N1" outbox inbox
  add_queues "$N2" outbox inbox
}

exchange() {
  step "BrokerConnection.java send outbox (node 1), receive outbox (node 2)"
  client send "$U1" outbox 10
  client receive "$U2" outbox 10
  client receive "$U1" outbox 1
  step "BrokerConnection.java send inbox (node 2), receive inbox (node 1)"
  client send "$U2" inbox 10
  client receive "$U1" inbox 10
}

suite() {
  make_instance "$NODE1" "$N1" 3850
  make_instance "$NODE2" "$N2" 3880
  configure

  step "artemis run (node 2, node 1)"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  wait_for "$N1/log/artemis.log" AMQ111003 && echo "node 1: connected to node 2"
  client consumers "$U2" inbox 1

  exchange

  step "management (node 1)"
  client manage "$U1" listBrokerConnections
  client attribute "$U1" brokerconnection.to-node2 uri
  client attribute "$U1" brokerconnection.to-node2 protocol
  client attribute "$U1" brokerconnection.to-node2 retryInterval
  client attribute "$U1" brokerconnection.to-node2 reconnectAttempts
  client manage "$U1" stopBrokerConnection to-node2
  client consumers "$U2" inbox 0
  client attribute "$U1" brokerconnection.to-node2 started
  client manage "$U1" stopBrokerConnection nothing
  client manage "$U1" startBrokerConnection to-node2
  client consumers "$U2" inbox 1
  client attribute "$U1" brokerconnection.to-node2 connected

  # 4. Node 2 goes and comes back.
  step "artemis stop (node 2)"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
  wait_for "$N1/log/artemis.log" AMQ111002 && echo "node 1: retrying"
  step "artemis run (node 2)"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ111003 3 && echo "node 1: connected to node 2"
  client consumers "$U2" inbox 1

  exchange

  step "artemis stop (node 1, node 2)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/amqp-connection.expected.txt"
  echo "== updated $HERE/amqp-connection.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/amqp-connection.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2; output in $OUT)"
  exit 1
fi
