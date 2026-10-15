#!/usr/bin/env bash
# The AMQP mirror suite: two instances made by "artemis create" (not
# clustered), each run through its own bin/artemis, and the Artemis client
# (Core). Each has an <amqp-connection> to the other with a <mirror>: dual
# mirroring. The output, normalized, must match
# tests/interop/cluster/amqp-mirror.expected.txt (recorded with Artemis for
# both nodes).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis tests/interop/cluster/amqp-mirror.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. Each node connects to the other (AMQ111003).
#   2. A queue made on node 1 is made on node 2; messages sent to it on
#      either node are on both, once (no loop).
#   3. Messages consumed on node 1 leave node 2 too (acknowledgements).
#   4. A subscription made on node 2 is made on node 1; what is published
#      on node 1 is in both.
#   5. Node 2 stops: what node 1 is sent meanwhile waits in its
#      store-and-forward queue and reaches node 2 when it is back.
#   6. Node 2 consumes everything: node 1 is left with none; a queue
#      deleted on node 2 goes from node 1.
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
PORT1=65511 PORT2=65526
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
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/AmqpMirror.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

U1="tcp://localhost:$PORT1" U2="tcp://localhost:$PORT2"

# Puts a fragment in <core>, after the acceptors.
add_core() {
  local dir="$1" xml="$2"
  XML="$xml" perl -0pi -e 's{(</acceptors>)}{$1\n$ENV{XML}}' "$dir/etc/broker.xml"
}

mirror_to() {
  local port="$1" name="$2"
  echo "      <broker-connections>
         <amqp-connection uri=\"tcp://localhost:$port\" name=\"$name\" user=\"admin\" password=\"admin\" retry-interval=\"500\">
            <mirror/>
         </amqp-connection>
      </broker-connections>"
}

# Both nodes' counts of a queue, once they are what is expected.
counts() {
  local queue="$1" expected="$2"
  echo -n "node 1: "; client count "$U1" "$queue" "$expected"
  echo -n "node 2: "; client count "$U2" "$queue" "$expected"
}

suite() {
  make_instance "$NODE1" "$N1" 3895
  make_instance "$NODE2" "$N2" 3910
  add_core "$N1" "$(mirror_to "$PORT2" to-node2)"
  add_core "$N2" "$(mirror_to "$PORT1" to-node1)"

  step "artemis run (node 1, node 2)"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ111003 && echo "node 1: connected to node 2"
  wait_for "$N2/log/artemis.log" AMQ111003 && echo "node 2: connected to node 1"

  step "AmqpMirror.java create jobs (node 1)"
  client create "$U1" jobs jobs anycast
  counts jobs 0

  step "AmqpMirror.java send jobs (10 on node 1, 10 on node 2)"
  client send "$U1" jobs a 10
  client send "$U2" jobs b 10
  counts jobs 20
  sleep 2
  counts jobs 20

  step "AmqpMirror.java consume jobs (8 on node 1)"
  client consume "$U1" jobs 8
  counts jobs 12

  step "AmqpMirror.java create events::events-sub (node 2)"
  client create "$U2" events events-sub multicast
  counts events-sub 0
  step "AmqpMirror.java send topic:events (5 on node 1)"
  client send "$U1" topic:events e 5
  counts events-sub 5

  step "artemis stop (node 2)"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
  wait_for "$N1/log/artemis.log" AMQ111002 && echo "node 1: retrying"
  step "AmqpMirror.java send jobs (5 on node 1, node 2 down)"
  client send "$U1" jobs c 5
  echo -n "node 1: "; client count "$U1" jobs 17
  step "artemis run (node 2)"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ111003 2 && echo "node 1: connected to node 2"
  counts jobs 17

  step "AmqpMirror.java consume jobs, events::events-sub (node 2)"
  client consume "$U2" jobs 17
  client consume "$U2" events::events-sub 5
  counts jobs 0
  counts events-sub 0

  step "AmqpMirror.java delete jobs (node 2)"
  client delete "$U2" jobs
  counts jobs -1

  step "artemis stop (node 1, node 2)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/amqp-mirror.expected.txt"
  echo "== updated $HERE/amqp-mirror.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/amqp-mirror.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2; output in $OUT)"
  exit 1
fi
