#!/usr/bin/env bash
# The bridge suite: a source broker whose core bridge forwards its queue
# src to the address dst of a target broker, each made by "artemis create"
# and run through its instance's own bin/artemis, with the Artemis JMS
# client, Qpid JMS and management messages. The output, normalized, must
# match tests/interop/cluster/bridge.expected.txt (recorded with Artemis on
# both sides).
#
#   SOURCE=saranga|artemis TARGET=saranga|artemis tests/interop/cluster/bridge.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. The bridge connects; Core messages sent to src reach dst with the
#      bridge's duplicate ID, an AMQP message stays one, and a large
#      message arrives whole.
#   2. The target is killed: messages wait on src; it is started again and
#      the bridge reconnects and forwards them.
#   3. BridgeControl's attributes; a bridge created through management is
#      journaled, deployed again after a restart, and destroyed for good.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/cluster"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
SOURCE="${SOURCE:-saranga}" TARGET="${TARGET:-saranga}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
mkdir -p "$ROOT/target/cluster"
WORK="$(mktemp -d -p "$ROOT/target/cluster")"
S="$WORK/source" T="$WORK/target"
PORTS=64616 PORTT=64716
cleanup() {
  for i in "$S" "$T"; do
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

# Inserts text after <addresses> and before </core> of an instance's
# broker.xml.
configure() {
  local dir="$1" addresses="$2" core="$3"
  ADDRESSES="$addresses" CORE="$core" perl -0pi -e \
    's#<addresses>#<addresses>$ENV{ADDRESSES}#; s#</core>#$ENV{CORE}</core>#' "$dir/etc/broker.xml"
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
# The bridge's lines in a broker's log since the last call: connected,
# connection failed, stopped. Sorted: bridges start and stop on executors of
# their own, in no set order.
declare -A SEEN
said() {
  local who="$1" dir="$2" from="${SEEN[$1]:-0}"
  local lines
  lines="$(grep -o -E 'AMQ22(1027|1029|2095|2122|2123|2124|2125|2126|2175)[^]]*' "$dir/log/artemis.log")"
  SEEN[$who]="$(printf '%s\n' "$lines" | grep -c .)"
  printf '%s\n' "$lines" | tail -n +"$((from + 1))" | grep . \
    | sed -E -e 's/(AMQ221027): Bridge BridgeImpl@[0-9a-f]+ \[name=([^,]*),.*/\1: Bridge \2 is connected/' \
    | LC_ALL=C sort | sed "s/^/$who: /"
}

step() { echo "=== $*"; }
client() {
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Bridge.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

BRIDGE='<bridges>
  <bridge name="br">
    <queue-name>src</queue-name>
    <forwarding-address>dst</forwarding-address>
    <retry-interval>500</retry-interval>
    <user>admin</user>
    <password>admin</password>
    <static-connectors><connector-ref>target</connector-ref></static-connectors>
  </bridge>
</bridges>'

suite() {
  make_instance "$SOURCE" "$S" 3000
  make_instance "$TARGET" "$T" 3100
  configure "$S" '<address name="src"><anycast><queue name="src"/></anycast></address>
    <address name="src2"><anycast><queue name="src2"/></anycast></address>' \
    "<connectors><connector name=\"target\">tcp://localhost:$PORTT</connector></connectors>$BRIDGE"
  configure "$T" '<address name="dst"><anycast><queue name="dst"/></anycast></address>' ''

  # 1. Forwarding.
  step "artemis run (target, source)"
  launch "$T"
  wait_up "$PORTT" && echo "target: listening"
  launch "$S"
  wait_up "$PORTS" && wait_for "$S/log/artemis.log" AMQ221027 && echo "source: listening"
  said source "$S"
  step "Bridge.java send src 0..10 (source)"
  client send "tcp://localhost:$PORTS" src 0 10
  step "Bridge.java receive dst 10 (target)"
  client receive "tcp://localhost:$PORTT" dst 10
  step "Bridge.java send-amqp src 2 (source)"
  client send-amqp "tcp://localhost:$PORTS" src 2
  step "Bridge.java receive-amqp dst 2 (target)"
  client receive-amqp "tcp://localhost:$PORTT" dst 2
  step "Bridge.java send-large src 300000 (source)"
  client send-large "tcp://localhost:$PORTS" src 300000
  step "Bridge.java receive dst 1 (target)"
  client receive "tcp://localhost:$PORTT" dst 1

  # 2. The target goes and comes back.
  step "artemis kill (target)"
  instance "$T" kill > /dev/null
  wait_down "$PORTT" && echo "target: down"
  wait_for "$S/log/artemis.log" AMQ222095 && said source "$S"
  step "Bridge.java send src 10..15 (source)"
  client send "tcp://localhost:$PORTS" src 10 15
  step "artemis run (target)"
  launch "$T"
  wait_up "$PORTT" && echo "target: listening"
  wait_for "$S/log/artemis.log" AMQ221027 2 && said source "$S"
  step "Bridge.java receive dst 5 (target)"
  client receive "tcp://localhost:$PORTT" dst 5

  # 3. Management.
  step "Bridge.java attributes br (source)"
  client attributes "tcp://localhost:$PORTS" br
  step "Bridge.java create mb (source)"
  client create "tcp://localhost:$PORTS" '{"name":"mb","queue-name":"src2","forwarding-address":"dst","static-connectors":["target"],"user":"admin","password":"admin","retry-interval":500}'
  wait_for "$S/log/artemis.log" AMQ221027 3 && said source "$S"
  step "Bridge.java names (source)"
  client names "tcp://localhost:$PORTS"
  step "Bridge.java send src2 20..22 (source)"
  client send "tcp://localhost:$PORTS" src2 20 22
  step "Bridge.java receive dst 2 (target)"
  client receive "tcp://localhost:$PORTT" dst 2
  step "artemis stop (source)"
  instance "$S" stop > /dev/null
  wait_down "$PORTS" && wait_for "$S/log/artemis.log" AMQ221029 2 && echo "source: down"
  said source "$S"
  step "artemis run (source)"
  launch "$S"
  wait_up "$PORTS" && wait_for "$S/log/artemis.log" AMQ221027 5 && echo "source: listening"
  said source "$S"
  step "Bridge.java names (source)"
  client names "tcp://localhost:$PORTS"
  step "Bridge.java destroy mb (source)"
  client destroy "tcp://localhost:$PORTS" mb
  wait_for "$S/log/artemis.log" AMQ221029 3 && said source "$S"
  step "Bridge.java names (source)"
  client names "tcp://localhost:$PORTS"
  step "artemis stop (source)"
  instance "$S" stop > /dev/null
  wait_down "$PORTS" && wait_for "$S/log/artemis.log" AMQ221029 4 && echo "source: down"
  said source "$S"
  step "artemis run (source)"
  launch "$S"
  wait_up "$PORTS" && wait_for "$S/log/artemis.log" AMQ221027 6 && echo "source: listening"
  sleep 1
  said source "$S"
  step "Bridge.java names (source)"
  client names "tcp://localhost:$PORTS"

  step "artemis stop (source, target)"
  instance "$S" stop > /dev/null
  wait_down "$PORTS" && echo "source: down"
  instance "$T" stop > /dev/null
  wait_down "$PORTT" && echo "target: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/bridge.expected.txt"
  echo "== updated $HERE/bridge.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/bridge.expected.txt" "$OUT"; then
  echo "ALL PASSED ($SOURCE source, $TARGET target)"
  rm -f "$OUT"
else
  echo "FAILED ($SOURCE source, $TARGET target; output in $OUT)"
  exit 1
fi
