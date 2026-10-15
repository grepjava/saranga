#!/usr/bin/env bash
# The AMQP bridge suite: two instances made by "artemis create" (not
# clustered), each run through its own bin/artemis, and the Artemis JMS
# client. Node 1 has an <amqp-connection> to node 2 with a <bridge>: to
# policies (node 1 sends "news" to node 2's "news" and its queue "outbox"
# to node 2's "inbox") and from policies (node 1 takes node 2's "alerts",
# through a durable subscription there, and its queues "orders" and "late"
# while there are consumers of them on node 1). The output, normalized,
# must match tests/interop/cluster/amqp-bridge.expected.txt (recorded with
# Artemis for both nodes).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis tests/interop/cluster/amqp-bridge.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. Node 1 connects to node 2 (AMQ111003) and attaches its senders.
#   2. To policies: a durable subscriber on node 2 receives what is
#      published on node 1, and node 2's "inbox" what is sent to node 1's
#      "outbox".
#   3. From policies: a durable subscriber on node 1 receives what is
#      published on node 2, and a consumer of "orders" on node 1 takes node
#      2's messages, leaving none there.
#   4. A consumer on node 1 for "late", which node 2 does not have and may
#      not create: the receiver is refused, then attached again (link
#      recovery) after the queue is made on node 2, and takes its messages.
#   5. Node 2 stops: node 1 retries (AMQ111002), connects again when node 2
#      is back, and the bridge works again.
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
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/AmqpBridge.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

U1="tcp://localhost:$PORT1" U2="tcp://localhost:$PORT2"

# Puts a fragment in <core>, after the acceptors.
add_core() {
  local dir="$1" xml="$2"
  XML="$xml" perl -0pi -e 's{(</acceptors>)}{$1\n$ENV{XML}}' "$dir/etc/broker.xml"
}

# Puts a fragment at the start of <addresses> or <address-settings>.
add_in() {
  local dir="$1" element="$2" xml="$3"
  XML="$xml" EL="$element" perl -0pi -e 's{(<$ENV{EL}>)}{$1\n$ENV{XML}}' "$dir/etc/broker.xml"
}

ADDRESSES='         <address name="orders"><anycast><queue name="orders"/></anycast></address>
         <address name="news"><multicast/></address>
         <address name="alerts"><multicast/></address>'

configure() {
  add_in "$N1" addresses "$ADDRESSES
         <address name=\"outbox\"><anycast><queue name=\"outbox\"/></anycast></address>"
  add_in "$N2" addresses "$ADDRESSES
         <address name=\"inbox\"><anycast><queue name=\"inbox\"/></anycast></address>"
  add_in "$N2" address-settings '         <address-setting match="late">
            <auto-create-queues>false</auto-create-queues>
            <auto-create-addresses>false</auto-create-addresses>
         </address-setting>'
  add_core "$N1" "      <broker-connections>
         <amqp-connection uri=\"tcp://localhost:$PORT2\" name=\"to-node2\" user=\"admin\" password=\"admin\" retry-interval=\"500\">
            <bridge name=\"bridge\">
               <bridge-to-address name=\"news-policy\">
                  <include address-match=\"news\"/>
               </bridge-to-address>
               <bridge-to-queue name=\"outbox-policy\" remote-address=\"inbox\">
                  <include address-match=\"#\" queue-match=\"outbox\"/>
               </bridge-to-queue>
               <bridge-from-address name=\"alerts-policy\" use-durable-subscriptions=\"true\">
                  <include address-match=\"alerts\"/>
               </bridge-from-address>
               <bridge-from-queue name=\"orders-policy\">
                  <include address-match=\"#\" queue-match=\"orders\"/>
                  <include address-match=\"#\" queue-match=\"late\"/>
               </bridge-from-queue>
               <property key=\"linkRecoveryInitialDelay\" value=\"500\"/>
               <property key=\"linkRecoveryDelay\" value=\"1000\"/>
            </bridge>
         </amqp-connection>
      </broker-connections>"
}

exchange() {
  step "AmqpBridge.java topic news (subscriber on node 2, published on node 1)"
  client topic "$U2" "$U1" news 10 "news-$1"
  step "AmqpBridge.java queue outbox inbox (sent on node 1, consumed on node 2)"
  client queue "$U1" outbox "$U2" inbox 10
  step "AmqpBridge.java topic alerts (subscriber on node 1, published on node 2)"
  client topic "$U1" "$U2" alerts 10 "alerts-$1"
  step "AmqpBridge.java queue orders (sent on node 2, consumed on node 1)"
  client queue "$U2" orders "$U1" orders 10
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

  exchange 1

  step "AmqpBridge.java late late (consumed on node 1, made and sent on node 2)"
  client late "$U2" "$U1" late 10

  # 5. Node 2 goes and comes back.
  step "artemis stop (node 2)"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
  wait_for "$N1/log/artemis.log" AMQ111002 && echo "node 1: retrying"
  step "artemis run (node 2)"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ111003 2 && echo "node 1: connected to node 2"

  exchange 2

  step "artemis stop (node 1, node 2)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/amqp-bridge.expected.txt"
  echo "== updated $HERE/amqp-bridge.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/amqp-bridge.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2; output in $OUT)"
  exit 1
fi
