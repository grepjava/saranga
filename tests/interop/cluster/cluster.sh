#!/usr/bin/env bash
# The cluster suite: two instances made by "artemis create --clustered"
# with each other as static cluster connectors, each run through its own
# bin/artemis, and the Artemis JMS client. The output, normalized, must
# match tests/interop/cluster/cluster.expected.txt (recorded with Artemis
# for both nodes).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis tests/interop/cluster/cluster.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. Each node bridges to the other; a client is given both in the
#      topology.
#   2. ON_DEMAND: messages sent to node 1 reach a consumer on node 2; with
#      a consumer on each node they are shared out; a topic's subscribers
#      on both nodes each receive everything.
#   3. Node 2 stops: node 1 keeps its messages; node 2 comes back and the
#      nodes bridge to each other again.
#   4. Each node's ClusterConnectionControl, and node 1's cluster
#      connection stopped and started through it.
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
PORT1=64816 PORT2=64916
cleanup() {
  for i in "$N1" "$N2"; do
    [[ -d "$i" ]] && instance "$i" stop > /dev/null 2>&1
  done
  sleep 3
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

make_instance() {
  local kind="$1" dir="$2" offset="$3" other="$4"
  local opts=(--silent --no-autotune --user admin --password admin --require-login
    --port-offset "$offset" --no-web --host localhost
    --clustered --static-cluster "tcp://localhost:$other"
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
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Cluster.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

U1="tcp://localhost:$PORT1" U2="tcp://localhost:$PORT2"

suite() {
  make_instance "$NODE1" "$N1" 3200 "$PORT2"
  make_instance "$NODE2" "$N2" 3300 "$PORT1"

  # 1. The cluster forms.
  step "artemis run (node 1, node 2)"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ221027 && echo "node 1: bridged to node 2"
  wait_for "$N2/log/artemis.log" AMQ221027 && echo "node 2: bridged to node 1"
  step "Cluster.java topology (node 1)"
  client topology "$U1"

  # 2. Load balancing.
  step "Cluster.java on-demand orders 10"
  client on-demand "$U1" "$U2" orders 10
  step "Cluster.java balance orders 10"
  client balance "$U1" "$U2" orders 10
  step "Cluster.java topic news 5"
  client topic "$U1" "$U2" news 5

  # 3. A node goes and comes back.
  step "artemis stop (node 2)"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
  sleep 2
  step "Cluster.java send orders 3 (node 1)"
  client send "$U1" orders 3
  step "Cluster.java receive orders 3 (node 1)"
  client receive "$U1" orders 3
  step "artemis run (node 2)"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ221027 2 && echo "node 1: bridged to node 2"
  wait_for "$N2/log/artemis.log" AMQ221027 2 && echo "node 2: bridged to node 1"
  step "Cluster.java on-demand orders 4"
  client on-demand "$U1" "$U2" orders 4
  step "Cluster.java topology (node 1)"
  client topology "$U1"

  # 4. Management: the cluster connection's control on each node, then a
  #    stop and start through it.
  step "Cluster.java control (node 1)"
  client control "$U1"
  step "Cluster.java control (node 2)"
  client control "$U2"
  step "Cluster.java control-restart (node 1)"
  client control-restart "$U1"
  step "Cluster.java on-demand orders 2"
  client on-demand "$U1" "$U2" orders 2

  step "artemis stop (node 1, node 2)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/cluster.expected.txt"
  echo "== updated $HERE/cluster.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/cluster.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2; output in $OUT)"
  exit 1
fi
