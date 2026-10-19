#!/usr/bin/env bash
# The outbound TLS suite: a source broker connecting over TLS to a target
# broker whose TLS acceptor wants a client certificate, each made by
# "artemis create" and run through its instance's own bin/artemis, with the
# Artemis JMS client and the source instance's CLI. The output, normalized,
# must match tests/interop/cluster/tls.expected.txt (recorded with Artemis
# on both sides).
#
#   SOURCE=saranga|artemis TARGET=saranga|artemis tests/interop/cluster/tls.sh
#
# Each defaults to saranga. UPDATE=1 rewrites the expected output instead of
# comparing.
#
#   1. A core bridge whose connector has sslEnabled, a truststore with a
#      masked password and a keystore, forwards src to the target's dst.
#   2. An AMQP broker connection with the same TLS sends outbox to the
#      target's outbox.
#   3. The source's CLI produces and consumes over TLS.
#   4. A bridge whose truststore does not trust the target's certificate
#      fails (AMQ214016) and forwards nothing, rather than connecting
#      without TLS.
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
PORTS=64816 PORTT=64916 PORTTLS=64917
TLS="$(native "$ROOT/tests/tls")"
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

# Inserts text after <addresses>, after </acceptors> and in <acceptors> of
# an instance's broker.xml.
configure() {
  local dir="$1" addresses="$2" core="$3" acceptor="${4:-}"
  ADDRESSES="$addresses" CORE="$core" ACCEPTOR="$acceptor" perl -0pi -e \
    's#<addresses>#<addresses>$ENV{ADDRESSES}#; s#</acceptors>#$ENV{ACCEPTOR}</acceptors>\n$ENV{CORE}#' "$dir/etc/broker.xml"
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
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/Bridge.java")" "$@" 2>&1 \
    | grep --line-buffered -v -E "^WARNING|^SLF4J|^\s*at |^[0-9-]+ [0-9:,]+ (INFO|WARN)"
}

# "changeit", masked (artemis mask changeit).
MASKED='ENC(14dd946cd17fd15122fa49a8d8fa115d)'
CLIENT_TLS="sslEnabled=true;trustStorePath=$TLS/truststore.jks;trustStorePassword=$MASKED;keyStorePath=$TLS/client.p12;keyStorePassword=changeit;keyStoreType=PKCS12"
UNTRUSTED_TLS="sslEnabled=true;trustStorePath=$TLS/untrusted.p12;trustStorePassword=changeit;trustStoreType=PKCS12;keyStorePath=$TLS/client.p12;keyStorePassword=changeit;keyStoreType=PKCS12"
TARGET_TLS="sslEnabled=true;keyStorePath=$TLS/server.p12;keyStorePassword=changeit;keyStoreType=PKCS12;needClientAuth=true;trustStorePath=$TLS/truststore.jks;trustStorePassword=changeit"

bridge() {
  echo "<bridge name=\"$1\">
    <queue-name>$2</queue-name>
    <forwarding-address>dst</forwarding-address>
    <retry-interval>500</retry-interval>
    <user>admin</user>
    <password>admin</password>
    <static-connectors><connector-ref>$3</connector-ref></static-connectors>
  </bridge>"
}

suite() {
  make_instance "$SOURCE" "$S" 3200
  make_instance "$TARGET" "$T" 3300
  configure "$S" '<address name="src"><anycast><queue name="src"/></anycast></address>
    <address name="bad"><anycast><queue name="bad"/></anycast></address>
    <address name="outbox"><anycast><queue name="outbox"/></anycast></address>' \
    "<connectors>
       <connector name=\"target\">tcp://localhost:$PORTTLS?$CLIENT_TLS</connector>
       <connector name=\"untrusted\">tcp://localhost:$PORTTLS?$UNTRUSTED_TLS</connector>
     </connectors>
     <bridges>$(bridge br src target)$(bridge bad bad untrusted)</bridges>
     <broker-connections>
       <amqp-connection uri=\"tcp://localhost:$PORTTLS?$CLIENT_TLS\" name=\"tls-out\" user=\"admin\" password=\"admin\" retry-interval=\"500\">
         <sender address-match=\"outbox\"/>
       </amqp-connection>
     </broker-connections>"
  configure "$T" '<address name="dst"><anycast><queue name="dst"/></anycast></address>
    <address name="outbox"><anycast><queue name="outbox"/></anycast></address>
    <address name="cli"><anycast><queue name="cli"/></anycast></address>' '' \
    "<acceptor name=\"tls\">tcp://localhost:$PORTTLS?$TARGET_TLS;protocols=CORE,AMQP</acceptor>"

  step "artemis run (target, source)"
  launch "$T"
  wait_up "$PORTTLS" && echo "target: listening"
  launch "$S"
  wait_up "$PORTS" && echo "source: listening"
  wait_for "$S/log/artemis.log" "AMQ221027.*name=br," && echo "source: bridge br is connected"
  wait_for "$S/log/artemis.log" AMQ111003 && echo "source: tls-out is connected"
  wait_for "$S/log/artemis.log" AMQ214016 && echo "source: AMQ214016 (the untrusted connector)"

  # 1. The bridge.
  step "Bridge.java send src 0..5 (source)"
  client send "tcp://localhost:$PORTS" src 0 5
  step "Bridge.java receive dst 5 (target)"
  client receive "tcp://localhost:$PORTT" dst 5

  # 2. The AMQP broker connection.
  step "Bridge.java send outbox 0..3 (source)"
  client send "tcp://localhost:$PORTS" outbox 0 3
  step "Bridge.java receive outbox 3 (target)"
  client receive "tcp://localhost:$PORTT" outbox 3

  # 3. The CLI.
  step "artemis producer --url tcp://localhost:$PORTTLS?<tls> (source's CLI)"
  instance "$S" producer --url "tcp://localhost:$PORTTLS?$CLIENT_TLS" --user admin --password admin \
    --destination queue://cli --message-count 3 --protocol core | grep -o -E "Produced: [0-9]+ messages"
  step "artemis consumer --url tcp://localhost:$PORTTLS?<tls> (source's CLI)"
  instance "$S" consumer --url "tcp://localhost:$PORTTLS?$CLIENT_TLS" --user admin --password admin \
    --destination queue://cli --message-count 3 --protocol core | grep -o -E "Consumed: [0-9]+ messages"

  # 4. The untrusted bridge forwarded nothing.
  step "Bridge.java send bad 10..12 (source)"
  client send "tcp://localhost:$PORTS" bad 10 12
  sleep 2
  step "Bridge.java receive dst 0 (target)"
  client receive "tcp://localhost:$PORTT" dst 0
  step "Bridge.java receive bad 2 (source)"
  client receive "tcp://localhost:$PORTS" bad 2

  step "artemis stop (source, target)"
  instance "$S" stop > /dev/null
  wait_down "$PORTS" && echo "source: down"
  instance "$T" stop > /dev/null
  wait_down "$PORTTLS" && echo "target: down"
}

OUT="$(mktemp)"
suite > "$OUT" 2>&1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/tls.expected.txt"
  echo "== updated $HERE/tls.expected.txt"
  rm -f "$OUT"
  exit 0
fi
if diff -u "$HERE/tls.expected.txt" "$OUT"; then
  echo "ALL PASSED ($SOURCE source, $TARGET target)"
  rm -f "$OUT"
else
  echo "FAILED ($SOURCE source, $TARGET target; output in $OUT)"
  exit 1
fi
