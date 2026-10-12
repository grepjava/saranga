#!/usr/bin/env bash
# The plugin suite: runs Artemis CLI commands against a broker with
# LoggingActiveMQServerPlugin (LOG_ALL_EVENTS) and compares the plugin's
# AMQ841 lines, normalized by normalize.py, with expected.txt (made
# against Artemis 2.57.0).
#
#   tests/interop/plugin/plugin.sh [saranga|artemis]
#
# UPDATE=1 rewrites expected.txt from the run; KEEP_LOG=<file> keeps the
# broker's log.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/plugin"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
OFFSET=730
PORT=$((61616 + OFFSET))
NAME=saranga-plugin-ref
BROKER="${1:-saranga}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
PID=""
cleanup() {
  [[ -n "$PID" ]] && kill "$PID" 2>/dev/null
  [[ -d "$WORK/i" ]] && ARTEMIS_INSTANCE="$(native "$WORK/i")" "$SARANGA" stop 2>/dev/null
  docker rm -f "$NAME" >/dev/null 2>&1
  sleep 1
  rm -rf "$WORK"
}
trap cleanup EXIT

PLUGIN='<broker-plugins><broker-plugin class-name="org.apache.activemq.artemis.core.server.plugin.impl.LoggingActiveMQServerPlugin"><property key="LOG_ALL_EVENTS" value="true"/></broker-plugin></broker-plugins>'
# The plugin goes last in <core>.
plugin_on() { sed -i "s|</core>|$PLUGIN</core>|" "$1"; }

case "$BROKER" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
    I="$WORK/i"
    "$SARANGA" create "$(native "$I")" --silent --no-autotune --user artemis --password artemis \
      --require-login --port-offset $OFFSET > /dev/null 2>&1 || { echo "create failed"; exit 1; }
    plugin_on "$I/etc/broker.xml"
    ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" run > "$WORK/broker.log" 2>&1 &
    PID=$!
    for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    LOG="$I/log/artemis.log"
    ;;
  artemis)
    mkdir -p "$WORK/etc-override"
    docker rm -f "$NAME" > /dev/null 2>&1
    docker create --name "$NAME" -e ANONYMOUS_LOGIN=false \
      -p "127.0.0.1:$PORT:61616" "saranga/artemis-ref:$VERSION" > /dev/null
    # The image makes its instance when it starts: start it once for its
    # broker.xml, then again with the plugin.
    docker start "$NAME" > /dev/null
    for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    docker cp "$NAME":/var/lib/artemis-instance/etc/broker.xml "$(native "$WORK/etc-override/broker.xml")" > /dev/null
    plugin_on "$WORK/etc-override/broker.xml"
    docker stop "$NAME" > /dev/null
    docker cp "$(native "$WORK/etc-override")" "$NAME":/var/lib/artemis-instance/etc-override > /dev/null
    docker cp "$(native "$WORK/etc-override/broker.xml")" "$NAME":/var/lib/artemis-instance/etc/broker.xml > /dev/null
    docker start "$NAME" > /dev/null
    for _ in $(seq 1 90); do [[ $(docker logs "$NAME" 2>&1 | grep -c AMQ221007) -ge 2 ]] && break; sleep 1; done
    LOG="$WORK/artemis.log"
    ;;
  *) echo "usage: plugin.sh [saranga|artemis]"; exit 2 ;;
esac

cli() {
  timeout 60 java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis "$@" --url "tcp://127.0.0.1:$PORT" --silent \
    > /dev/null 2>&1
}
cli queue create --name pq --address pq --anycast --durable --auto-create-address \
  --user artemis --password artemis
cli producer --destination queue://pq --message-count 1 --user artemis --password artemis
cli consumer --destination queue://pq --message-count 1 --receive-timeout 5000 \
  --user artemis --password artemis
cli queue delete --name pq --user artemis --password artemis
sleep 2
[[ "$BROKER" == artemis ]] && docker exec "$NAME" cat /var/lib/artemis-instance/log/artemis.log > "$LOG"

[[ -n "${KEEP_LOG:-}" ]] && cp "$LOG" "$KEEP_LOG"
python -I "$(native "$HERE/normalize.py")" "$(native "$LOG")" > "$WORK/actual.txt" || exit 1
if [[ -n "${UPDATE:-}" ]]; then
  cp "$WORK/actual.txt" "$HERE/expected.txt"
  echo "expected.txt updated ($(wc -l < "$HERE/expected.txt") lines)"
  exit 0
fi
if diff -u "$HERE/expected.txt" "$WORK/actual.txt"; then
  echo "ALL PASSED"
else
  echo "FAILED: the plugin's log differs from Artemis's"
  exit 1
fi
