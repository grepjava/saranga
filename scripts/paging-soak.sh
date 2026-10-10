#!/usr/bin/env bash
# The multi-GB paging soak: fill a paging queue with a backlog of several GB
# on one broker, stop it, start a broker on the same data directory and drain
# the queue, checking every message arrives once, in order and intact.
#
#   scripts/paging-soak.sh <sender> <receiver> [MiB] [message-bytes]
#
# <sender> and <receiver> are artemis or saranga; with different brokers the
# paging directory moves from one to the other. The queue, soak.queue, pages
# above 50 MiB in 10 MiB pages (tests/interop/soak.broker.xml), so nearly the
# whole backlog is in page files. SOAK_QUEUE=golden.paging.soak puts it on
# 20 KiB pages instead: one message per page file. For Saranga the script reports
# the peak resident memory of each run, which stays bounded by the paging
# settings rather than growing with the backlog. Defaults: 3 GiB (3072 MiB)
# of 10 KiB messages.
set -euo pipefail

SENDER="${1:?usage: paging-soak.sh <sender> <receiver> [MiB] [message-bytes]}"
RECEIVER="${2:?usage: paging-soak.sh <sender> <receiver> [MiB] [message-bytes]}"
MIB="${3:-3072}"
SIZE="${4:-10240}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="saranga/artemis-ref:$VERSION"
NAME="saranga-soak"
PORT="${SOAK_PORT:-61627}"
CONFIG="$ROOT/tests/interop/soak.broker.xml"
QUEUE="${SOAK_QUEUE:-soak.queue}"
COUNT=$(( MIB * 1024 * 1024 / SIZE ))
export MSYS_NO_PATHCONV=1

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
INSTANCE="$WORK/instance"
mkdir -p "$INSTANCE/etc" && cp "$CONFIG" "$INSTANCE/etc/broker.xml"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

wait_port() {
  for _ in $(seq 1 300); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0; sleep 0.2; done
  echo "broker did not open port $PORT" >&2; return 1
}

start_artemis() {   # $1: copy Saranga's data in first
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker create --name "$NAME" --user root -e ANONYMOUS_LOGIN=true \
    -p "127.0.0.1:$PORT:61616" "$IMAGE" >/dev/null
  docker cp "$(native "$INSTANCE/etc")" "$NAME:/var/lib/artemis-instance/etc-override" >/dev/null
  if [[ "${1:-}" == "with-data" ]]; then
    docker cp "$(native "$INSTANCE/data")" "$NAME:/var/lib/artemis-instance/data" >/dev/null
  fi
  docker start "$NAME" >/dev/null
  for _ in $(seq 1 600); do
    docker logs "$NAME" 2>&1 | grep -q AMQ221007 && return 0   # "Server is now active"
    sleep 1
  done
  docker logs "$NAME" >&2; return 1
}

start_saranga() {
  "$ROOT/target/release/saranga" run --config "$(native "$INSTANCE/etc/broker.xml")" \
    --bind "127.0.0.1:$PORT" > "$WORK/saranga-$1.log" 2>&1 &
  BROKER_PID=$!
  wait_port
}

# Saranga's peak resident memory so far, in MiB.
saranga_peak() {
  if [[ -r "/proc/$BROKER_PID/winpid" ]]; then   # MSYS/Cygwin on Windows
    powershell -NoProfile -Command       "[int]((Get-Process -Id $(cat "/proc/$BROKER_PID/winpid")).PeakWorkingSet64 / 1MB)"
  else
    awk '/VmHWM/ { printf "%d", $2 / 1024 }' "/proc/$BROKER_PID/status"
  fi
}

stop_saranga() {
  echo "saranga peak resident memory: $(saranga_peak) MiB"
  touch "$INSTANCE/etc/STOP_ME"
  wait "$BROKER_PID" || true
  BROKER_PID=""
}

soak() {
  "$ROOT/target/release/examples/page_soak" "127.0.0.1:$PORT" "$QUEUE" "$1" "$COUNT" "$SIZE"
}

cargo build -q --release -p saranga-cli -p saranga-client --example page_soak \
  --manifest-path "$(native "$ROOT/Cargo.toml")"

echo "== $COUNT messages of $SIZE bytes ($MIB MiB): send on $SENDER, receive on $RECEIVER"
case "$SENDER" in
  artemis) start_artemis ;;
  saranga) start_saranga send ;;
  *) echo "unknown broker $SENDER" >&2; exit 2 ;;
esac
soak send
case "$SENDER" in
  artemis)
    docker exec "$NAME" du -sh /var/lib/artemis-instance/data/paging
    docker stop -t 120 "$NAME" >/dev/null ;;
  saranga)
    du -sh "$INSTANCE/data/paging"
    stop_saranga ;;
esac

case "$SENDER->$RECEIVER" in
  artemis-\>artemis)
    docker start "$NAME" >/dev/null
    until docker logs "$NAME" 2>&1 | grep -c AMQ221007 | grep -q '^[2-9]'; do sleep 1; done ;;
  saranga-\>saranga) start_saranga receive ;;
  artemis-\>saranga)
    docker cp "$NAME:/var/lib/artemis-instance/data" "$(native "$INSTANCE/data")" >/dev/null
    docker rm -f "$NAME" >/dev/null
    start_saranga receive ;;
  saranga-\>artemis) start_artemis with-data ;;
  *) echo "unknown broker $RECEIVER" >&2; exit 2 ;;
esac
soak receive
case "$RECEIVER" in
  artemis)
    sleep 5
    docker exec "$NAME" du -sh /var/lib/artemis-instance/data/paging ;;
  saranga)
    sleep 5
    du -sh "$INSTANCE/data/paging"
    stop_saranga ;;
esac
echo "== soak passed"
