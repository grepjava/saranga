#!/usr/bin/env bash
# Persistence and cross-store checks: run the "before" half of
# tests/interop/jms/Persist.java on one broker, stop it (cleanly or with a
# kill), start a broker on the same data directory, and run the "after" half.
#
#   scripts/persistence.sh <before> <after> [stop|kill] [test-name...]
#
# <before> and <after> are artemis or saranga. With different brokers the
# data directory moves from one to the other: artemis -> saranga checks that
# Saranga recovers what Artemis wrote, saranga -> artemis the reverse.
# kill is SIGKILL for Artemis and KILL_ME (an immediate halt) for Saranga.
set -euo pipefail

BEFORE="${1:?usage: persistence.sh <before> <after> [stop|kill] [test...]}"
AFTER="${2:?usage: persistence.sh <before> <after> [stop|kill] [test...]}"
MODE="${3:-stop}"
shift 3 || shift $#
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="saranga/artemis-ref:$VERSION"
NAME="saranga-persistence"
PORT="${PERSIST_PORT:-61623}"
DIST="$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION"
# The golden broker.xml with golden.paging.# paging above 100 KiB, for the
# paging tests.
CONFIG="$ROOT/tests/golden/etc/persist-paging.broker.xml"
RUN_ID="$(date +%s)"
export MSYS_NO_PATHCONV=1

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
INSTANCE="$WORK/instance"          # Saranga's instance: etc/ and data/
mkdir -p "$INSTANCE/etc" && cp "$CONFIG" "$INSTANCE/etc/broker.xml"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0; sleep 0.2; done
  echo "broker did not open port $PORT" >&2; return 1
}

start_artemis() {   # $1: copy Saranga's data in first
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker create --name "$NAME" --user root -e ANONYMOUS_LOGIN=true \
    -p "127.0.0.1:$PORT:61616" "$IMAGE" >/dev/null
  # The image creates its instance with etc-override/broker.xml.
  docker cp "$(native "$INSTANCE/etc")" "$NAME:/var/lib/artemis-instance/etc-override" >/dev/null
  if [[ "${1:-}" == "with-data" ]]; then
    docker cp "$(native "$INSTANCE/data")" "$NAME:/var/lib/artemis-instance/data" >/dev/null
  fi
  docker start "$NAME" >/dev/null
  for _ in $(seq 1 90); do
    docker logs "$NAME" 2>&1 | grep -q AMQ221007 && return 0   # "Server is now active"
    sleep 1
  done
  docker logs "$NAME" >&2; return 1
}

stop_artemis() {
  if [[ "$MODE" == kill ]]; then docker kill "$NAME" >/dev/null; else docker stop -t 60 "$NAME" >/dev/null; fi
}

restart_artemis() {
  docker start "$NAME" >/dev/null
  until docker logs "$NAME" 2>&1 | grep -c AMQ221007 | grep -q '^[2-9]'; do sleep 1; done
}

start_saranga() {
  cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")"
  "$ROOT/target/release/saranga" run --config "$(native "$INSTANCE/etc/broker.xml")" \
    --bind "127.0.0.1:$PORT" > "$WORK/saranga-$1.log" 2>&1 &
  BROKER_PID=$!
  wait_port
}

stop_saranga() {
  if [[ "$MODE" == kill ]]; then touch "$INSTANCE/etc/KILL_ME"; else touch "$INSTANCE/etc/STOP_ME"; fi
  wait "$BROKER_PID" || true
  BROKER_PID=""
}

client() {
  local phase="$1"; shift
  java -cp "$(native "$DIST/lib")/*" "$(native "$ROOT/tests/interop/jms/Persist.java")" \
    "tcp://127.0.0.1:$PORT" "$phase" "$RUN_ID" "$@"
}

echo "== before on $BEFORE, $MODE, after on $AFTER"
case "$BEFORE" in
  artemis) start_artemis ;;
  saranga) start_saranga before ;;
  *) echo "unknown broker $BEFORE" >&2; exit 2 ;;
esac
client before "$@"
case "$BEFORE" in
  artemis) stop_artemis ;;
  saranga) stop_saranga ;;
esac

case "$BEFORE->$AFTER" in
  artemis-\>artemis) restart_artemis ;;
  saranga-\>saranga) start_saranga after ;;
  artemis-\>saranga)
    docker cp "$NAME:/var/lib/artemis-instance/data" "$(native "$INSTANCE/data")" >/dev/null
    docker rm -f "$NAME" >/dev/null
    start_saranga after ;;
  saranga-\>artemis) start_artemis with-data ;;
  *) echo "unknown broker $AFTER" >&2; exit 2 ;;
esac
client after "$@"
