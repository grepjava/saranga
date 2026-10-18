#!/usr/bin/env bash
# The multiple cluster connections suite: two instances made by "artemis
# create --clustered", their one cluster connection replaced by two, each
# for its own addresses, and the Artemis JMS client. The output, normalized,
# must match tests/interop/cluster/multi.expected.txt (recorded with
# Artemis for both nodes).
#
#   NODE1=saranga|artemis NODE2=saranga|artemis tests/interop/cluster/multi.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   orders-cluster  a <cluster-connection> for the address orders,
#                   ON_DEMAND (the default one, the first deployed);
#   news-cluster    a <cluster-connection-uri> for the address news, STRICT.
#
#   1. Each node bridges to the other through both cluster connections;
#      management shows both controls.
#   2. orders: messages sent to node 1 reach a consumer on node 2.
#   3. news: with no consumer anywhere, STRICT shares the messages sent to
#      node 1 out between the two nodes.
#   4. local, which neither cluster connection covers: a consumer on node 2
#      gets nothing of what is sent to node 1.
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
PORT1=65016 PORT2=65116
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
  # The two cluster connections in place of my-cluster, and the queues
  # news and local on every node.
  OTHER="$other" perl -0pi -e '
    s{<cluster-connections>.*?</cluster-connections>}{<cluster-connections>
         <cluster-connection name="orders-cluster">
            <address>orders</address>
            <connector-ref>artemis</connector-ref>
            <message-load-balancing>ON_DEMAND</message-load-balancing>
            <max-hops>1</max-hops>
            <static-connectors>
               <connector-ref>node0</connector-ref>
            </static-connectors>
         </cluster-connection>
         <cluster-connection-uri name="news-cluster" address="static://(tcp://localhost:$ENV{OTHER})?connectorName=artemis&amp;address=news&amp;messageLoadBalancingType=STRICT&amp;maxHops=1"/>
      </cluster-connections>}s;
    s{</addresses>}{   <address name="news"><anycast><queue name="news"/></anycast></address>
         <address name="local"><anycast><queue name="local"/></anycast></address>
      </addresses>}s;
  ' "$dir/etc/broker.xml"
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
  make_instance "$NODE1" "$N1" 3400 "$PORT2"
  make_instance "$NODE2" "$N2" 3500 "$PORT1"

  # 1. Both cluster connections bridge.
  step "artemis run (node 1, node 2)"
  launch "$N1"
  wait_up "$PORT1" && echo "node 1: listening"
  launch "$N2"
  wait_up "$PORT2" && echo "node 2: listening"
  wait_for "$N1/log/artemis.log" AMQ221027 2 && echo "node 1: both bridged to node 2"
  wait_for "$N2/log/artemis.log" AMQ221027 2 && echo "node 2: both bridged to node 1"
  step "Cluster.java controls (node 1)"
  client controls "$U1"
  step "Cluster.java controls (node 2)"
  client controls "$U2"

  # 2. orders, ON_DEMAND.
  step "Cluster.java on-demand orders 10"
  client on-demand "$U1" "$U2" orders 10

  # 3. news, STRICT: shared out without consumers.
  step "Cluster.java send news 10 (node 1)"
  client send "$U1" news 10
  step "Cluster.java drain news (node 1)"
  client drain "$U1" news
  step "Cluster.java drain news (node 2)"
  client drain "$U2" news

  # 4. local, in neither cluster connection.
  step "Cluster.java on-demand local 3"
  client on-demand "$U1" "$U2" local 3
  step "Cluster.java drain local (node 1)"
  client drain "$U1" local

  step "artemis stop (node 1, node 2)"
  instance "$N1" stop > /dev/null
  wait_down "$PORT1" && echo "node 1: down"
  instance "$N2" stop > /dev/null
  wait_down "$PORT2" && echo "node 2: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/multi.expected.txt"
  echo "== updated $HERE/multi.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/multi.expected.txt" "$OUT"; then
  echo "ALL PASSED ($NODE1 node 1, $NODE2 node 2)"
  rm -f "$OUT"
else
  echo "FAILED ($NODE1 node 1, $NODE2 node 2; output in $OUT)"
  exit 1
fi
