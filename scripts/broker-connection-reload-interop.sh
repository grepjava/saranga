#!/usr/bin/env bash
# Runs the AMQP broker connection reload suite
# (tests/interop/jms/BrokerConnectionReload.java) against Saranga or the
# reference Artemis broker, both run natively on instances made by
# `artemis create`: a peer, and a broker whose <broker-connections> connect
# to it. The broker's broker.xml is rewritten step by step: a connection
# kept, one changed, one added without auto-start, connections started and
# stopped through management and then changed, connections removed, a
# mirror added, changed and removed (which a reload leaves alone), and
# finally a name given twice, which fails the reload as Artemis fails it.
#
#   scripts/broker-connection-reload-interop.sh [saranga|artemis]
set -euo pipefail

TARGET="${1:-saranga}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
PORT=61700
PEER=61701
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
PIDS=()
INSTANCES=()
cleanup() {
  for i in "${INSTANCES[@]}"; do touch "$i/etc/STOP_ME" 2>/dev/null || true; done
  for p in "${PIDS[@]}"; do
    for _ in $(seq 1 50); do kill -0 "$p" 2>/dev/null || break; sleep 0.2; done
    kill "$p" 2>/dev/null || true
  done
  rm -rf "$WORK"
}
trap cleanup EXIT

CP="$(native "$DIST/lib")/*"
artemis() {   # $1: instance, then the command
  local inst="$1"; shift
  java -cp "$CP" -Dartemis.home="$(native "$DIST")" -Dartemis.instance="$(native "$inst")" \
    -Dartemis.instance.etc="$(native "$inst/etc")" \
    -Djava.security.auth.login.config="$(native "$inst/etc/login.config")" \
    -Dlog4j2.configurationFile="file:///$(native "$inst/etc/log4j2.properties")" \
    org.apache.activemq.artemis.boot.Artemis "$@"
}

# broker.xml with an acceptor on $2 and $3 inside <core>.
config() {   # $1: file, $2: port, $3: more of <core>
  cat > "$1" <<EOF
<?xml version="1.0"?>
<configuration xmlns="urn:activemq" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="urn:activemq /schema/artemis-configuration.xsd">
   <core xmlns="urn:activemq:core">
      <persistence-enabled>true</persistence-enabled>
      <security-enabled>false</security-enabled>
      <configuration-file-refresh-period>500</configuration-file-refresh-period>
      <acceptors>
         <acceptor name="all">tcp://127.0.0.1:$2</acceptor>
      </acceptors>
      <addresses>
         <address name="in"><anycast><queue name="in"/></anycast></address>
         <address name="out"><anycast><queue name="out"/></anycast></address>
         <address name="a"><anycast><queue name="a"/></anycast></address>
      </addresses>
$3
   </core>
</configuration>
EOF
}
# An <amqp-connection> to the peer receiving the peer's queue $2.
connection() {   # $1: name, $2: queue, $3: more attributes
  echo "<amqp-connection uri=\"tcp://127.0.0.1:$PEER\" name=\"$1\" $3><receiver queue-name=\"$2\"/></amqp-connection>"
}
connections() { echo "<broker-connections>$*</broker-connections>"; }
MIRROR="<amqp-connection uri=\"tcp://127.0.0.1:$PEER\" name=\"c4\" retry-interval=\"1000\"><mirror/></amqp-connection>"
MIRROR_CHANGED="<amqp-connection uri=\"tcp://127.0.0.1:$PEER\" name=\"c4\" retry-interval=\"1500\"><mirror/></amqp-connection>"

start() {   # $1: instance, $2: log
  case "$TARGET" in
    saranga)
      ARTEMIS_INSTANCE="$(native "$1")" ARTEMIS_INSTANCE_ETC="$(native "$1/etc")" \
        "$ROOT/target/release/saranga" run > "$2" 2>&1 & ;;
    artemis) artemis "$1" run > "$2" 2>&1 & ;;
  esac
  PIDS+=($!)
  INSTANCES+=("$1")
  for _ in $(seq 1 150); do grep -aqs AMQ221007 "$2" && return; sleep 0.2; done
  echo "broker did not start"; tail -30 "$2"; exit 1
}

case "$TARGET" in
  saranga) cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" ;;
  artemis) ;;
  *) echo "usage: broker-connection-reload-interop.sh [saranga|artemis]"; exit 2 ;;
esac
for i in peer broker; do
  java -cp "$CP" -Dartemis.home="$(native "$DIST")" org.apache.activemq.artemis.boot.Artemis \
    create --silent --force --no-web --user a --password a --role amq --require-login \
    "$(native "$WORK/$i")" > "$WORK/create-$i.log" 2>&1 || { cat "$WORK/create-$i.log"; exit 1; }
done
config "$WORK/peer/etc/broker.xml" "$PEER" ""
config "$WORK/broker/etc/broker.xml" "$PORT" \
  "$(connections "$(connection c1 in 'retry-interval="1000"')" "$(connection c2 out 'retry-interval="1000"')")"
start "$WORK/peer" "$WORK/peer.log"
start "$WORK/broker" "$WORK/broker.log"

LOG="$WORK/broker.log"
reloads() { grep -ac AMQ221087 "$LOG" || true; }
reload() {   # $1: more of <core>
  local before; before=$(reloads)
  config "$WORK/broker/etc/broker.xml" "$PORT" "$1"
  for _ in $(seq 1 60); do (( $(reloads) > before )) && break; sleep 0.5; done
  (( $(reloads) > before )) || { echo "no reload logged"; tail -30 "$LOG"; exit 1; }
  sleep 2
}
SUITE="$(native "$ROOT/tests/interop/jms/BrokerConnectionReload.java")"
FAILED=0
suite() {
  echo "--- $1"
  java -cp "$CP" "$SUITE" "$1" "$PORT" "$PEER" "$(native "$WORK/labels")" 2>&1 | grep -v '^WARNING' \
    || FAILED=1
}

suite before
reload "$(connections "$(connection c1 in 'retry-interval="1000"')" \
  "$(connection c2 out 'retry-interval="2000"')" "$(connection c3 a 'auto-start="false"')")"
suite changed
suite managed
# Both changed and without auto-start: the stopped c2 stays stopped, the
# started c3 starts again.
reload "$(connections "$(connection c1 in 'retry-interval="1000"')" \
  "$(connection c2 out 'retry-interval="3000" auto-start="false"')" \
  "$(connection c3 a 'retry-interval="3000" auto-start="false"')")"
suite restarted
reload "$(connections "$(connection c3 a 'retry-interval="3000" auto-start="false"')" "$MIRROR")"
suite mirrored
reload "$(connections "$(connection c3 a 'retry-interval="3000" auto-start="false"')" "$MIRROR_CHANGED")"
suite mirror-changed
reload ""
suite none

# A name given twice: Collectors.toMap's IllegalStateException fails the
# reload, AMQ224069 before AMQ221087.
before=$(reloads)
config "$WORK/broker/etc/broker.xml" "$PORT" \
  "$(connections "$(connection d1 in '')" "$(connection d1 out '')")"
for _ in $(seq 1 60); do grep -aq AMQ224069 "$LOG" && break; sleep 0.5; done
sleep 1
if grep -aq "Duplicate key d1 (attempted merging values org.apache.activemq.artemis.core.config.amqpBrokerConnectivity.AMQPBrokerConnectConfiguration@" "$LOG" \
   && (( $(reloads) == before )); then
  echo "PASS duplicateNameFailsReload"
else
  echo "FAIL duplicateNameFailsReload"; FAILED=1
fi

echo "--- the broker's reload log"
grep -a "AMQ221056\|AMQ221087\|AMQ224069\|Skipping\|Duplicate key" "$LOG" \
  | sed -E 's/^[0-9-]+ [0-9:,]+ +//; s/(AMQPBrokerConnectConfiguration)@[0-9a-f]+/\1@.../g'
[[ $FAILED == 0 ]] && echo "ALL PASSED" || { echo "SOME FAILED"; exit 1; }
