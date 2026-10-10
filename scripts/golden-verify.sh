#!/usr/bin/env bash
# Start a broker on a fresh copy of a golden data dir and print what a client sees.
#
#   scripts/golden-verify.sh <scenario> [artemis|saranga]     (default: artemis)
#
# Extracts tests/golden/<scenario>/data.tar.gz, starts the broker on it, then runs
# tests/golden-gen/tools/DumpQueues.java with the queues listed in
# tests/golden/<scenario>/QUEUES. The dump goes to stdout. The golden data is never
# modified: the broker works on a copy.
#
#   scripts/golden-verify.sh persist-durable > tests/golden/persist-durable/recovered.txt
#
# A scenario with tests/golden/etc/<scenario>.broker.xml runs with that config, as in golden.sh.
set -euo pipefail

SCENARIO="${1:?usage: golden-verify.sh <scenario> [artemis|saranga]}"
TARGET="${2:-artemis}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="saranga/artemis-ref:$VERSION"
NAME="saranga-verify-$SCENARIO"
PORT="${VERIFY_PORT:-61619}"
DIR="$ROOT/tests/golden/$SCENARIO"
DIST="$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION"
CONFIG="$ROOT/tests/golden/etc/broker.xml"
[[ -f "$ROOT/tests/golden/etc/$SCENARIO.broker.xml" ]] && CONFIG="$ROOT/tests/golden/etc/$SCENARIO.broker.xml"
export MSYS_NO_PATHCONV=1

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

[[ -f "$DIR/data.tar.gz" ]] || { echo "no $DIR/data.tar.gz" >&2; exit 1; }
[[ -f "$DIR/QUEUES" ]] || { echo "no $DIR/QUEUES" >&2; exit 1; }
mapfile -t QUEUES < <(tr -d '\r' < "$DIR/QUEUES" | grep -v '^\s*$')

WORK="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT
tar -C "$WORK" -xzf "$DIR/data.tar.gz"

case "$TARGET" in
  artemis)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    # Root, so the copied-in files (owned by root) are writable by the broker.
    docker create --name "$NAME" --user root -e ANONYMOUS_LOGIN=true \
      -p "127.0.0.1:$PORT:61616" "$IMAGE" >/dev/null
    mkdir -p "$WORK/etc-override" && cp "$CONFIG" "$WORK/etc-override/broker.xml"
    docker cp "$(native "$WORK/etc-override")" "$NAME:/var/lib/artemis-instance/etc-override" >/dev/null
    docker cp "$(native "$WORK/data")" "$NAME:/var/lib/artemis-instance/data" >/dev/null
    docker start "$NAME" >/dev/null
    for _ in $(seq 1 90); do
      docker logs "$NAME" 2>&1 | grep -q "AMQ221007" && break   # "Server is now active"
      sleep 1
    done
    docker logs "$NAME" 2>&1 | grep -q "AMQ221007" || { docker logs "$NAME" >&2; exit 1; }
    ;;
  saranga)
    # A broker instance around the extracted data: etc/broker.xml and data/.
    mkdir -p "$WORK/etc" && cp "$CONFIG" "$WORK/etc/broker.xml"
    cargo build -q -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
    "$ROOT/target/debug/saranga" run --config "$(native "$WORK/etc/broker.xml")" --bind "127.0.0.1:$PORT" >&2 &
    BROKER_PID=$!
    for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    ;;
  *) echo "usage: golden-verify.sh <scenario> [artemis|saranga]" >&2; exit 2 ;;
esac

java -cp "$(native "$DIST/lib")/*" \
  "$(native "$ROOT/tests/golden-gen/tools/DumpQueues.java")" "tcp://127.0.0.1:$PORT" "${QUEUES[@]}"
