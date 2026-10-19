#!/usr/bin/env bash
# Runs the acceptor and jaasConfigs reload suite
# (tests/interop/jms/AcceptorReload.java) against Saranga or the reference
# Artemis broker, both run natively on an instance made by `artemis create`:
# the broker starts with tests/interop/reload/acceptors-before.xml, which is
# replaced by acceptors-after.xml with jaas.properties as its broker
# properties; then the properties are emptied; then acceptors-clash.xml adds
# an acceptor on a port in use, which fails the reload as Artemis fails it.
#
#   scripts/acceptor-reload-interop.sh [saranga|artemis]
set -euo pipefail

TARGET="${1:-saranga}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
BASE=61690
CONF="$ROOT/tests/interop/reload"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
INSTANCE="$WORK/instance"
BROKER_PID=""
HOLD_PID=""
cleanup() {
  [[ -n "$HOLD_PID" ]] && kill "$HOLD_PID" 2>/dev/null || true
  if [[ -n "$BROKER_PID" ]]; then
    touch "$INSTANCE/etc/STOP_ME" 2>/dev/null || true
    for _ in $(seq 1 50); do kill -0 "$BROKER_PID" 2>/dev/null || break; sleep 0.2; done
    kill "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

CP="$(native "$DIST/lib")/*"
artemis() {
  java -cp "$CP" -Dartemis.home="$(native "$DIST")" -Dartemis.instance="$(native "$INSTANCE")" \
    -Dartemis.instance.etc="$(native "$INSTANCE/etc")" \
    -Djava.security.auth.login.config="$(native "$INSTANCE/etc/login.config")" \
    -Dlog4j2.configurationFile="file:///$(native "$INSTANCE/etc/log4j2.properties")" \
    org.apache.activemq.artemis.boot.Artemis "$@"
}

java -cp "$CP" -Dartemis.home="$(native "$DIST")" org.apache.activemq.artemis.boot.Artemis \
  create --silent --force --no-web --user a --password a --role amq --require-login \
  "$(native "$INSTANCE")" > "$WORK/create.log" 2>&1 || { cat "$WORK/create.log"; exit 1; }
cp "$CONF/acceptors-before.xml" "$INSTANCE/etc/broker.xml"
# There from the start, so that it is watched (Run.execute).
echo "# no jaasConfigs yet" > "$INSTANCE/etc/broker.properties"

case "$TARGET" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    ARTEMIS_INSTANCE="$(native "$INSTANCE")" ARTEMIS_INSTANCE_ETC="$(native "$INSTANCE/etc")" \
      "$ROOT/target/release/saranga" run > "$WORK/broker.log" 2>&1 &
    BROKER_PID=$! ;;
  artemis)
    artemis run > "$WORK/broker.log" 2>&1 &
    BROKER_PID=$! ;;
  *) echo "usage: acceptor-reload-interop.sh [saranga|artemis]"; exit 2 ;;
esac
logs() { cat "$WORK/broker.log"; }
reloads() { logs | grep -ac AMQ221087 || true; }
for _ in $(seq 1 150); do logs | grep -aq AMQ221007 && break; sleep 0.2; done
logs | grep -aq AMQ221007 || { echo "broker did not start"; logs | tail -30; exit 1; }

SUITE="$(native "$ROOT/tests/interop/jms/AcceptorReload.java")"
suite() { java -cp "$CP" "$SUITE" "$@" 2>&1 | grep -v '^WARNING'; }
# Waits for the next completed reload, then for any other the change
# started (the properties and broker.xml are watched apart).
await_reload() {   # $1: reloads completed before
  for _ in $(seq 1 60); do (( $(reloads) > $1 )) && break; sleep 0.5; done
  (( $(reloads) > $1 )) || { echo "no reload logged"; logs | tail -30; exit 1; }
  sleep 3
}

FAILED=0
suite before "$BASE" || FAILED=1

java -cp "$CP" "$SUITE" hold "$BASE" "$(native "$WORK/reloaded")" > "$WORK/hold.out" 2>&1 &
HOLD_PID=$!
for _ in $(seq 1 100); do grep -q HELD "$WORK/hold.out" && break; sleep 0.2; done
grep -q HELD "$WORK/hold.out" || { cat "$WORK/hold.out"; exit 1; }

before=$(reloads)
cp "$CONF/jaas.properties" "$INSTANCE/etc/broker.properties"
cp "$CONF/acceptors-after.xml" "$INSTANCE/etc/broker.xml"
await_reload "$before"
touch "$WORK/reloaded"
wait "$HOLD_PID" || FAILED=1
HOLD_PID=""
grep -v '^WARNING\|^HELD' "$WORK/hold.out"
suite after "$BASE" || FAILED=1

before=$(reloads)
echo "# no jaasConfigs" > "$INSTANCE/etc/broker.properties"
await_reload "$before"
suite dropped "$BASE" || FAILED=1

# An acceptor on the kept acceptor's port: AMQ224104, then the reload
# fails with AMQ224069 before AMQ221087.
before=$(reloads)
cp "$CONF/acceptors-clash.xml" "$INSTANCE/etc/broker.xml"
for _ in $(seq 1 60); do logs | grep -aq AMQ224069 && break; sleep 0.5; done
sleep 1
if logs | grep -aq "AMQ224104: Error starting the Acceptor clash" && logs | grep -aq AMQ224069 \
   && (( $(reloads) == before )); then
  echo "PASS clashingAcceptorFailsReload"
else
  echo "FAIL clashingAcceptorFailsReload"; FAILED=1
fi
echo "--- the broker's reload log"
logs | grep -a "AMQ221056\|AMQ221087\|AMQ221020\|AMQ224104\|AMQ224069\|AMQ229230\|AMQ222080\|^Caused by" \
  | sed -E 's/^[0-9-]+ [0-9:,]+ +//'
[[ $FAILED == 0 ]] && echo "ALL PASSED" || { echo "SOME FAILED"; exit 1; }
