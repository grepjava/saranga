#!/usr/bin/env bash
# Run the Java interop suites against Saranga, or against the reference
# Artemis broker to check the suites themselves.
#
#   scripts/interop.sh [saranga|artemis] [test-name...]
#
# SUITE=<name> picks tests/interop/jms/<name>.java (default Phase2), and
# CONFIG=<broker.xml> the configuration both brokers run with (default
# tests/golden/etc/broker.xml, the image's own). ETC=<dir> adds the files of
# a directory (login.config, users and roles) to both brokers' etc/; without
# it both use the login files the image makes for anonymous login
# (tests/interop/etc).
# PORT and NAME (the Artemis container) let two runs share a machine.
#
# Runs tests/interop/jms/Phase2.java and the golden scenarios that the current
# phase supports, with the Artemis client jars from the reference build.
set -euo pipefail

TARGET="${1:-saranga}"; shift || true
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
PORT="${PORT:-61621}"
NAME="${NAME:-saranga-interop}"
CONFIG="${CONFIG:-$ROOT/tests/golden/etc/broker.xml}"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  [[ -n "${INSTANCE:-}" ]] && rm -rf "$INSTANCE"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

case "$TARGET" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    # A throwaway broker instance: the journal goes to its data/.
    INSTANCE="$(mktemp -d)"
    mkdir -p "$INSTANCE/etc" && cp "$ROOT"/tests/interop/etc/* "$INSTANCE/etc/"
    cp "$CONFIG" "$INSTANCE/etc/broker.xml"
    [[ -n "${ETC:-}" ]] && cp "$ETC"/* "$INSTANCE/etc/"
    "$ROOT/target/release/saranga" run --config "$(native "$INSTANCE/etc/broker.xml")" --bind "127.0.0.1:$PORT" &
    BROKER_PID=$!
    for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    ;;
  artemis)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker create --name "$NAME" -e ANONYMOUS_LOGIN=true \
      -p "127.0.0.1:$PORT:61616" "saranga/artemis-ref:$VERSION" >/dev/null
    # The image creates its instance with etc-override/broker.xml.
    INSTANCE="$(mktemp -d)"
    mkdir -p "$INSTANCE/etc-override" && cp "$CONFIG" "$INSTANCE/etc-override/broker.xml"
    [[ -n "${ETC:-}" ]] && cp "$ETC"/* "$INSTANCE/etc-override/"
    docker cp "$(native "$INSTANCE/etc-override")" "$NAME":/var/lib/artemis-instance/etc-override >/dev/null
    docker start "$NAME" >/dev/null
    for _ in $(seq 1 60); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    ;;
  *) echo "usage: interop.sh [saranga|artemis] [test-name...]"; exit 2 ;;
esac

CP="$(native "$DIST/lib")/*"
URL="tcp://127.0.0.1:$PORT"
SUITE="${SUITE:-Phase2}"
java -cp "$CP" "$(native "$ROOT/tests/interop/jms/$SUITE.java")" "$URL" "$@"
# The golden scenarios log in as the anonymous guest.
if [[ $# -eq 0 && -z "${ETC:-}" ]]; then
  for scenario in CoreBasic CoreXa; do
    java -cp "$CP" "$(native "$ROOT/tests/golden-gen/scenarios/$scenario.java")" "$URL"
  done
fi
