#!/usr/bin/env bash
# The SASL suite: an instance made by "artemis create --require-login" with
# an AMQP acceptor offering SCRAM-SHA-256, SCRAM-SHA-512 and PLAIN through
# the login.config realm amqp-sasl-scram (SCRAMPropertiesLoginModule), and
# SCRAMLoginModule in the broker's realm before PropertiesLoginModule.
# A second AMQP acceptor offers XOAUTH2, OAUTHBEARER and PLAIN, whose bearer
# tokens PropertiesLoginModule checks as passwords. Suite.java logs in with
# Qpid JMS (and OAUTHBEARER over a raw socket); two broker connections to
# the SCRAM acceptor log in with SCRAM, one with a wrong password.
#
#   tests/interop/sasl/sasl.sh [saranga|artemis] [test-name...]
#
# The users file has alice's SCRAM-SHA-256 line made by "saranga mask
# --scram", bob's lines made by SCRAMPropertiesLoginModule's main, and carol
# with a password in clear, for both brokers.
#
# OFFSET: the port offset, 3000 unless given (Windows may reserve its ports).
# MAX_DISK_USAGE: for a machine whose disk is past create's 90%, where the
# broker would block every producer.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/sasl"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
KIND="${1:-saranga}"; shift || true
OFFSET="${OFFSET:-3000}"
PORT=$((61616 + OFFSET))
SASL_PORT=$((PORT + 1))
TOKEN_PORT=$((PORT + 2))
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
windows() { [[ "$(uname -s)" == MINGW* || "$(uname -s)" == MSYS* || "$(uname -s)" == CYGWIN* ]]; }
# The Qpid JMS client: the dist's lib, or, without a dist, the jars in the
# local Maven repository.
client_cp() {
  if [[ -d "$DIST/lib" ]]; then echo "$(native "$DIST/lib")/*"; return; fi
  local m=~/.m2/repository n=4.1.137.Final cp="" j
  for j in org/apache/qpid/qpid-jms-client/1.17.0/qpid-jms-client-1.17.0.jar \
    org/apache/qpid/proton-j/0.35.0/proton-j-0.35.0.jar \
    jakarta/jms/jakarta.jms-api/2.0.3/jakarta.jms-api-2.0.3.jar \
    org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar \
    $(for x in buffer common handler codec codec-http resolver transport transport-native-unix-common; do
        echo io/netty/netty-$x/$n/netty-$x-$n.jar; done); do
    cp+="$(native "$m/$j");"
  done
  echo "$cp"
}

mkdir -p "$ROOT/target/sasl"
WORK="$(mktemp -d -p "$ROOT/target/sasl")"
I="$WORK/i"
instance() {
  if windows; then
    (cd "$I/bin" && timeout 300 cmd /c call "$(cygpath -w "$I/bin/artemis.cmd")" "$@" < /dev/null 2>&1)
  else
    (cd "$I/bin" && timeout 300 ./artemis "$@" < /dev/null 2>&1)
  fi
}
up() { (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; }
cleanup() {
  [[ -d "$I" ]] && instance stop > /dev/null 2>&1
  for _ in $(seq 1 100); do up || break; sleep 0.1; done
  [[ -n "${KEEP:-}" ]] && echo "kept $WORK" >&2 || rm -rf "$WORK"
}
trap cleanup EXIT

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
SARANGA="$ROOT/target/release/saranga"
opts=(--silent --no-autotune --user artemis --password artemis --require-login
  --port-offset "$OFFSET" --no-web --host localhost)
case "$KIND" in
  saranga)
    "$SARANGA" create "$(native "$I")" "${opts[@]}" > /dev/null 2>&1 || { echo "create failed"; exit 1; }
    ;;
  artemis)
    java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
      org.apache.activemq.artemis.boot.Artemis create "$(native "$I")" "${opts[@]}" > /dev/null 2>&1 \
      || { echo "create failed"; exit 1; }
    ;;
  *) echo "usage: sasl.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

ETC="$I/etc"
{
  "$SARANGA" mask --scram alice alicepw | grep '^alice|SHA256='
  if [[ -d "$DIST/lib" ]]; then
    java -cp "$(native "$DIST/lib")/*" \
      org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule bob bobpw | grep -v '^#'
  else
    "$SARANGA" mask --scram bob bobpw | grep -v '^#'
  fi
  echo "carol = carolpw"
  "$SARANGA" mask --scram dave davepw | grep -v '^#'
} > "$ETC/scram-users.properties"
printf 'amq = alice,bob,carol\n' > "$ETC/scram-roles.properties"
# tok logs in with a bearer token, which PropertiesLoginModule checks as
# its password. create's users file ends without a line end.
printf '\ntok = secret-token\n' >> "$ETC/artemis-users.properties"
sed -i -E 's/^amq ?= ?(.*)$/amq = \1,tok/' "$ETC/artemis-roles.properties"
cat > "$ETC/login.config" <<CONF
activemq {
   org.apache.activemq.artemis.spi.core.security.jaas.SCRAMLoginModule sufficient;
   org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule sufficient
       reload=true
       org.apache.activemq.jaas.properties.user="artemis-users.properties"
       org.apache.activemq.jaas.properties.role="artemis-roles.properties";
};

amqp-sasl-scram {
   org.apache.activemq.artemis.spi.core.security.jaas.SCRAMPropertiesLoginModule required
       org.apache.activemq.jaas.properties.user="scram-users.properties"
       org.apache.activemq.jaas.properties.role="scram-roles.properties";
};
CONF
ACCEPTOR="<acceptor name=\"scram\">tcp://localhost:$SASL_PORT?protocols=AMQP;saslMechanisms=SCRAM-SHA-512,SCRAM-SHA-256,PLAIN;saslLoginConfigScope=amqp-sasl-scram</acceptor>"
ACCEPTOR+="<acceptor name=\"token\">tcp://localhost:$TOKEN_PORT?protocols=AMQP;saslMechanisms=XOAUTH2,OAUTHBEARER,PLAIN</acceptor>"
CONNECT="<broker-connections>"
CONNECT+="<amqp-connection uri=\"tcp://localhost:$SASL_PORT\" name=\"scram-good\" user=\"bob\" password=\"bobpw\" retry-interval=\"500\" reconnect-attempts=\"2\"><sender address-match=\"bc.unused.#\"/></amqp-connection>"
CONNECT+="<amqp-connection uri=\"tcp://localhost:$SASL_PORT\" name=\"scram-bad\" user=\"bob\" password=\"wrong\" retry-interval=\"500\" reconnect-attempts=\"2\"><sender address-match=\"bc.unused.#\"/></amqp-connection>"
CONNECT+="</broker-connections>"
sed -i "s|</acceptors>|$ACCEPTOR</acceptors>$CONNECT|" "$ETC/broker.xml"
if [[ -n "${MAX_DISK_USAGE:-}" ]]; then
  sed -i -E "s#<max-disk-usage>[0-9]+</max-disk-usage>#<max-disk-usage>$MAX_DISK_USAGE</max-disk-usage>#" "$ETC/broker.xml"
fi

mkdir -p "$I/log"
if windows; then
  (cd "$I/bin" && cmd /c "$(cygpath -w "$I/bin/artemis.cmd")" run > "$WORK/run.out" 2>&1 &)
else
  (cd "$I/bin" && ./artemis run > "$WORK/run.out" 2>&1 &)
fi
for _ in $(seq 1 600); do up && break; sleep 0.2; done
up || { echo "the broker did not start"; cat "$WORK/run.out"; exit 1; }

java -cp "$(client_cp)" "$(native "$HERE/Suite.java")" "127.0.0.1:$SASL_PORT" "127.0.0.1:$TOKEN_PORT" "$@"
status=$?

# The broker connections: bob's SCRAM login, and a wrong password. Both
# brokers log AMQ111003 once the socket connects, before SASL, so the one
# refused is told by its retries, which go on, each connect resetting its
# count.
if [[ $# -eq 0 || " $* " == *" brokerConnection "* ]]; then
  for _ in $(seq 1 100); do
    [[ $(grep -c "Retrying Server AMQP Connection scram-bad" "$WORK/run.out") -ge 3 ]] && break
    sleep 0.1
  done
  # Artemis starts its broker connections before its acceptors, so each
  # retries once at startup there: what counts is what scram-good did last.
  last_good=$(grep -oE "(Connected on|Retrying) Server AMQP Connection scram-good" "$WORK/run.out" | tail -1)
  if [[ "$last_good" == "Connected on Server AMQP Connection scram-good" ]] \
    && [[ $(grep -c "Retrying Server AMQP Connection scram-bad" "$WORK/run.out") -ge 3 ]]; then
    echo "PASS brokerConnection"
  else
    echo "FAIL brokerConnection"
    grep -E -A1 "AMQ11100[123]|SCRAM" "$WORK/run.out" | head -30
    status=1
  fi
fi
exit $status
