#!/usr/bin/env bash
# The mixed suite, Phase 10's exit check for clusters: three instances made
# by "artemis create --clustered", each with the other two as static
# cluster connectors, ON_DEMAND, redistribution on (redistribution-delay 0)
# and a grouping handler, LOCAL on node 1 and REMOTE on nodes 2 and 3, each
# run through its own bin/artemis, and the Artemis JMS client. The output,
# normalized, must match tests/interop/cluster/mixed.expected.txt
# (recorded with Artemis for every node).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis NODE3=saranga|artemis tests/interop/cluster/mixed.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. The cluster forms: each node bridges to the other two, and a client
#      is given all three in the topology.
#   2. Load balancing: messages sent to node 1 are shared out among a
#      consumer on each node, and a topic's subscribers on every node each
#      receive everything.
#   3. Redistribution: messages left on one node move to a consumer on
#      another, in order, each way round the cluster.
#   4. Grouping: each message group goes to one consumer, in order,
#      whichever node it is sent to.
#   5. Load: a producer and a transacted consumer on each node; every
#      message is received once.
#   6. Node 3 stops: nodes 1 and 2 go on; node 3 comes back, the nodes
#      bridge to it again, the load is received once more and the groups go
#      where they went before.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/cluster"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
NODE1="${NODE1:-saranga}" NODE2="${NODE2:-saranga}" NODE3="${NODE3:-saranga}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/cluster"
WORK="$(mktemp -d -p "$ROOT/target/cluster")"
N1="$WORK/node1" N2="$WORK/node2" N3="$WORK/node3"
PORT1=63016 PORT2=63116 PORT3=63216
cleanup() {
  for i in "$N1" "$N2" "$N3"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

make_instance() {
  local kind="$1" dir="$2" offset="$3" others="$4"
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --host localhost
    --clustered --static-cluster "$others"
    --cluster-user cluster --cluster-password secret)
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
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Mixed.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

U1="tcp://localhost:$PORT1" U2="tcp://localhost:$PORT2" U3="tcp://localhost:$PORT3"
ALL="$U1,$U2,$U3"

# Redistribution on, and the node's grouping handler.
configure() {
  local dir="$1" kind="$2"
  perl -0pi -e 's{(<address-setting match="#">)}{$1\n            <redistribution-delay>0</redistribution-delay>};
    s{(\n\s*<address-settings>)}{\n      <grouping-handler name="my-grouping-handler">\n         <type>'"$kind"'</type>\n         <address>orders</address>\n         <timeout>5000</timeout>\n      </grouping-handler>\n$1}' "$dir/etc/broker.xml"
}

# Waits until node <n>'s log says it bridged to another node <count> times.
bridged() {
  local n="$1" dir="$2" count="$3"
  wait_for "$dir/log/artemis.log" AMQ221027 "$count" && echo "node $n: bridged to the others"
}

MAP="$WORK/groups.txt"

suite() {
  make_instance "$NODE1" "$N1" 1400 "$U2,$U3"
  make_instance "$NODE2" "$N2" 1500 "$U1,$U3"
  make_instance "$NODE3" "$N3" 1600 "$U1,$U2"
  configure "$N1" LOCAL
  configure "$N2" REMOTE
  configure "$N3" REMOTE

  # 1. The cluster forms.
  step "artemis run (node 1, node 2, node 3)"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  launch "$N3"
  wait_up "$PORT3" && echo "node 3: listening"
  bridged 1 "$N1" 2
  bridged 2 "$N2" 2
  bridged 3 "$N3" 2
  step "Mixed.java topology"
  client topology "$ALL"

  step "Mixed.java network-topology (node 1, node 3)"
  client network-topology "$ALL" 1
  client network-topology "$ALL" 3

  # 2. Load balancing.
  step "Mixed.java balance jobs 30"
  client balance "$ALL" jobs 30
  step "Mixed.java topic news 20"
  client topic "$ALL" news 20

  # 3. Redistribution, each way round.
  step "Mixed.java redistribute jobs 10 (node 1 to node 2)"
  client redistribute "$ALL" jobs 10 1 2
  step "Mixed.java redistribute jobs 10 (node 2 to node 3)"
  client redistribute "$ALL" jobs 10 2 3
  step "Mixed.java redistribute jobs 10 (node 3 to node 1)"
  client redistribute "$ALL" jobs 10 3 1

  # 4. Grouping, sent to each node.
  for n in 1 2 3; do
    step "Mixed.java grouped orders 60 6 (to node $n)"
    client grouped "$ALL" orders 60 6 "$n" "$(native "$MAP")"
  done

  # 5. Load.
  step "Mixed.java load work 300"
  client load "$ALL" work 300
  step "Mixed.java mqtt-takeover device-1 (node 1 to node 2)"
  client mqtt-takeover "$ALL" device-1 1 2
  step "Mixed.java mqtt-takeover device-2 (node 3 to node 1)"
  client mqtt-takeover "$ALL" device-2 3 1

  # 6. A node goes and comes back.
  step "artemis stop (node 3)"
  instance "$N3" stop > /dev/null
  wait_down "$PORT3" && echo "node 3: down"
  sleep 2
  step "Mixed.java load work 200 (node 1, node 2)"
  client load "$U1,$U2" work 200
  step "artemis run (node 3)"
  launch "$N3"
  wait_up "$PORT3" && echo "node 3: listening"
  bridged 1 "$N1" 3
  bridged 2 "$N2" 3
  bridged 3 "$N3" 4
  step "Mixed.java topology"
  client topology "$ALL"
  step "Mixed.java load work 300"
  client load "$ALL" work 300
  step "Mixed.java grouped orders 60 6 (to node 3)"
  client grouped "$ALL" orders 60 6 3 "$(native "$MAP")" 3

  step "artemis stop (node 1, node 2, node 3)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
  instance "$N3" stop > /dev/null
  wait_down "$PORT3" && echo "node 3: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/mixed.expected.txt"
  echo "== updated $HERE/mixed.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/mixed.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2, $NODE3 node 3)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2, $NODE3 node 3; output in $OUT)"
  exit 1
fi
