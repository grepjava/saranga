#!/usr/bin/env bash
# Runs the configuration reload suite (tests/interop/jms/Reload.java)
# against Saranga or the reference Artemis broker: the broker starts with
# tests/interop/reload/broker-before.xml, which is then replaced by
# broker-after.xml while it runs.
#
#   scripts/reload-interop.sh [saranga|artemis]
set -euo pipefail

TARGET="${1:-saranga}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
PORT="${PORT:-61680}"
NAME="saranga-reload"
CONF="$ROOT/tests/interop/reload"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

case "$TARGET" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    mkdir -p "$WORK/etc"
    cp "$CONF/broker-before.xml" "$WORK/etc/broker.xml"
    "$ROOT/target/release/saranga" run --config "$(native "$WORK/etc/broker.xml")" \
      --bind "127.0.0.1:$PORT" > "$WORK/broker.log" 2>&1 &
    BROKER_PID=$!
    for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    logs() { cat "$WORK/broker.log"; }
    swap() { cp "$CONF/broker-after.xml" "$WORK/etc/broker.xml"; touch "$WORK/etc/broker.xml"; }
    ;;
  artemis)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker create --name "$NAME" -e ANONYMOUS_LOGIN=true \
      -p "127.0.0.1:$PORT:61616" "saranga/artemis-ref:$VERSION" >/dev/null
    mkdir -p "$WORK/etc-override"
    cp "$CONF/broker-before.xml" "$WORK/etc-override/broker.xml"
    docker cp "$(native "$WORK/etc-override")" "$NAME":/var/lib/artemis-instance/etc-override >/dev/null
    docker start "$NAME" >/dev/null
    for _ in $(seq 1 60); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    logs() { docker logs "$NAME" 2>&1; }
    swap() {
      cp "$CONF/broker-after.xml" "$WORK/broker.xml"; touch "$WORK/broker.xml"
      docker cp "$(native "$WORK/broker.xml")" "$NAME":/var/lib/artemis-instance/etc/broker.xml >/dev/null
    }
    ;;
  *) echo "usage: reload-interop.sh [saranga|artemis]"; exit 2 ;;
esac

CP="$(native "$DIST/lib")/*"
SUITE="$(native "$ROOT/tests/interop/jms/Reload.java")"
java -cp "$CP" "$SUITE" "tcp://127.0.0.1:$PORT" before
swap
for _ in $(seq 1 30); do logs | grep -aq AMQ221087 && break; sleep 1; done
logs | grep -aq AMQ221087 || { echo "no reload logged"; logs | tail -20; exit 1; }
java -cp "$CP" "$SUITE" "tcp://127.0.0.1:$PORT" after
