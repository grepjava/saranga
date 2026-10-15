#!/usr/bin/env bash
# The federation suite: two instances made by "artemis create" (not
# clustered), each run through its own bin/artemis, and the Artemis JMS
# client. Node 2 federates from node 1 (an <upstream> with an address
# policy for "news" and a queue policy for "orders"); node 1 has a
# <downstream> asking node 2 to federate "alerts" from it. The output,
# normalized, must match tests/interop/cluster/federation.expected.txt
# (recorded with Artemis for both nodes).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis tests/interop/cluster/federation.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. Node 1's downstream makes node 2 deploy an upstream back to node 1.
#   2. Messages published on node 1 reach a durable subscriber on node 2,
#      with one hop: through the upstream, and through the downstream's.
#   3. Messages in a queue on node 1 go to a consumer of that queue on
#      node 2.
#   4. Node 1 stops and starts again: node 2 undeploys what the downstream
#      asked for, deploys it again when asked again, and its federated
#      consumers connect again.
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
PORT1=65216 PORT2=65416
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
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Federation.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

U1="tcp://localhost:$PORT1" U2="tcp://localhost:$PORT2"

# Puts a fragment in <core>, after the acceptors.
add_core() {
  local dir="$1" xml="$2"
  XML="$xml" perl -0pi -e 's{(</acceptors>)}{$1\n$ENV{XML}}' "$dir/etc/broker.xml"
}

configure() {
  add_core "$N1" "      <connectors>
         <connector name=\"self\">tcp://localhost:$PORT1</connector>
         <connector name=\"node2\">tcp://localhost:$PORT2</connector>
      </connectors>
      <federations>
         <federation name=\"back\" user=\"admin\" password=\"admin\">
            <downstream name=\"node2\" user=\"admin\" password=\"admin\">
               <retry-interval>500</retry-interval>
               <static-connectors><connector-ref>node2</connector-ref></static-connectors>
               <policy ref=\"alerts\"/>
               <upstream-connector-ref>self</upstream-connector-ref>
            </downstream>
            <address-policy name=\"alerts\"><include address-match=\"alerts\"/></address-policy>
         </federation>
      </federations>"
  add_core "$N2" "      <connectors>
         <connector name=\"node1\">tcp://localhost:$PORT1</connector>
      </connectors>
      <federations downstream-authorization=\"amq\">
         <federation name=\"fed\">
            <upstream name=\"node1\" user=\"admin\" password=\"admin\">
               <static-connectors><connector-ref>node1</connector-ref></static-connectors>
               <policy ref=\"news\"/>
               <policy ref=\"orders\"/>
            </upstream>
            <queue-policy name=\"orders\"><include queue-match=\"orders\" address-match=\"#\"/></queue-policy>
            <address-policy name=\"news\"><include address-match=\"news\"/></address-policy>
         </federation>
      </federations>"
}

suite() {
  make_instance "$NODE1" "$N1" 3600
  make_instance "$NODE2" "$N2" 3800
  configure

  step "artemis run (node 2, node 1)"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  wait_for "$N1/log/artemis.log" AMQ222283 && echo "node 1: downstream deployed"
  wait_for "$N2/log/artemis.log" AMQ224160 && echo "node 2: deployed the downstream's federation"

  # 2. Address federation, through the upstream and the downstream's.
  step "Federation.java topic news 10"
  client topic "$U1" "$U2" news 10 news-client
  step "Federation.java topic alerts 10"
  client topic "$U1" "$U2" alerts 10 alerts-client

  # 3. Queue federation.
  step "Federation.java queue orders 10"
  client queue "$U1" "$U2" orders 10

  # 4. Node 1 starts again.
  step "artemis stop (node 1)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  wait_for "$N2/log/artemis.log" AMQ224161 && echo "node 2: undeployed the downstream's federation"
  step "artemis run (node 1)"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  wait_for "$N2/log/artemis.log" AMQ224160 2 && echo "node 2: deployed the downstream's federation"
  step "Federation.java topic news 10"
  client topic "$U1" "$U2" news 10 news-client
  step "Federation.java topic alerts 10"
  client topic "$U1" "$U2" alerts 10 alerts-client
  step "Federation.java queue orders 10"
  client queue "$U1" "$U2" orders 10

  step "artemis stop (node 1, node 2)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  wait_for "$N2/log/artemis.log" AMQ224161 2 && echo "node 2: undeployed the downstream's federation"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/federation.expected.txt"
  echo "== updated $HERE/federation.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/federation.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2; output in $OUT)"
  exit 1
fi
