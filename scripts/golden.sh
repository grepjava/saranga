#!/usr/bin/env bash
# Generate one golden-corpus scenario against the real Artemis broker.
#
#   scripts/golden.sh <scenario>        e.g. scripts/golden.sh core-basic
#   GOLDEN_KILL=1 scripts/golden.sh persist-crash
#
# GOLDEN_KILL=1: the scenario prints a line "READY" and then blocks; the broker
# is then killed with `docker kill` (SIGKILL) instead of stopped cleanly, while
# the client is still connected. Without it the scenario must exit by itself.
# GOLDEN_WIRE=0: no capture proxy and no wire/ (for bulk scenarios whose traffic
# would be tens of MB).
# If tests/golden/etc/<scenario>.broker.xml exists it replaces the image's
# broker.xml (copied into etc-override/ before the instance is created).
#
# Flow: start saranga/artemis-ref in Docker -> start the capture proxy in front
# of it -> run tests/golden-gen/scenarios/<Scenario>.java on the host JDK
# through the proxy -> stop the broker cleanly -> copy its data dir out.
# Output: tests/golden/<scenario>/{wire/conn-N/{c2s,s2c}.bin, data.tar.gz, MANIFEST,
#         data-print.txt, recovered.txt (only if QUEUES exists)}
set -euo pipefail

SCENARIO="${1:?usage: golden.sh <scenario>}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="saranga/artemis-ref:$VERSION"
NAME="saranga-golden-$SCENARIO"
BROKER_PORT=61617
PROXY_PORT=61616
OUT="$ROOT/tests/golden/$SCENARIO"
CLASS="$(echo "$SCENARIO" | sed -E 's/(^|-)([a-z])/\U\2/g')"   # core-basic -> CoreBasic
DIST="$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION"
export MSYS_NO_PATHCONV=1

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

[[ -f "$ROOT/tests/golden-gen/scenarios/$CLASS.java" ]] || { echo "no scenario $CLASS.java"; exit 1; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "missing $IMAGE: run scripts/build-artemis.sh"; exit 1; }

PROXY_PID=""
cleanup() {
  [[ -n "$PROXY_PID" ]] && kill "$PROXY_PID" 2>/dev/null || true
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# QUEUES is hand-written input (the queues to dump); everything else is regenerated.
mkdir -p "$OUT"
rm -rf "$OUT/wire" "$OUT/data" "$OUT/data.tar.gz" "$OUT/MANIFEST" "$OUT/data-print.txt" "$OUT/recovered.txt"
mkdir -p "$OUT/wire"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker create --name "$NAME" -e ANONYMOUS_LOGIN=true \
  -p "127.0.0.1:$BROKER_PORT:61616" "$IMAGE" >/dev/null
CONFIG="$ROOT/tests/golden/etc/$SCENARIO.broker.xml"
if [[ -f "$CONFIG" ]]; then
  echo "using $CONFIG"
  OVERRIDE="$(mktemp -d)"
  mkdir -p "$OVERRIDE/etc-override" && cp "$CONFIG" "$OVERRIDE/etc-override/broker.xml"
  docker cp "$(native "$OVERRIDE/etc-override")" "$NAME:/var/lib/artemis-instance/etc-override" >/dev/null
  rm -rf "$OVERRIDE"
fi
docker start "$NAME" >/dev/null

echo "waiting for broker..."
for _ in $(seq 1 60); do
  docker logs "$NAME" 2>&1 | grep -q "AMQ221007" && break   # "Server is now active"
  sleep 1
done
docker logs "$NAME" 2>&1 | grep -q "AMQ221007" || { docker logs "$NAME"; exit 1; }

if [[ "${GOLDEN_WIRE:-1}" == 1 ]]; then
  cargo build -q -p saranga-capture --manifest-path "$(native "$ROOT/Cargo.toml")"
  "$ROOT/target/debug/saranga-capture" "127.0.0.1:$PROXY_PORT" "127.0.0.1:$BROKER_PORT" "$(native "$OUT/wire")" &
  PROXY_PID=$!
  sleep 1
else
  rmdir "$OUT/wire"
  PROXY_PORT=$BROKER_PORT                      # no capture: talk to the broker directly
fi

JAVA=(java -cp "$(native "$DIST/lib")/*"
  "$(native "$ROOT/tests/golden-gen/scenarios/$CLASS.java")" "tcp://127.0.0.1:$PROXY_PORT")

if [[ "${GOLDEN_KILL:-0}" == 1 ]]; then
  LOG="$(mktemp)"
  "${JAVA[@]}" > "$LOG" &
  JAVA_PID=$!
  for _ in $(seq 1 120); do
    grep -q '^READY' "$LOG" && break
    kill -0 "$JAVA_PID" 2>/dev/null || { cat "$LOG"; echo "scenario exited before READY"; exit 1; }
    sleep 1
  done
  grep -q '^READY' "$LOG" || { cat "$LOG"; echo "scenario never printed READY"; exit 1; }
  sleep 2                                       # let the journal writer flush
  docker kill "$NAME" >/dev/null                # SIGKILL: no clean shutdown
  kill "$JAVA_PID" 2>/dev/null || true
  cat "$LOG"; rm -f "$LOG"
  [[ -n "$PROXY_PID" ]] && kill "$PROXY_PID"; PROXY_PID=""
else
  "${JAVA[@]}"
  [[ -n "$PROXY_PID" ]] && kill "$PROXY_PID"; PROXY_PID=""
  docker stop -t 60 "$NAME" >/dev/null         # clean shutdown, journal synced
fi
docker cp "$NAME:/var/lib/artemis-instance/data" "$(native "$OUT/data")"
# Journal files are pre-allocated (mostly zeros): archive them byte-exact, compressed.
tar -C "$OUT" -czf "$OUT/data.tar.gz" data && rm -rf "$OUT/data"

cat > "$OUT/MANIFEST" <<MANIFEST
scenario: $SCENARIO
wire: $([[ "${GOLDEN_WIRE:-1}" == 1 ]] && echo "captured" || echo "not captured (GOLDEN_WIRE=0)")
shutdown: $([[ "${GOLDEN_KILL:-0}" == 1 ]] && echo "docker kill (SIGKILL)" || echo "docker stop (clean)")
config: $([[ -f "$CONFIG" ]] && echo "tests/golden/etc/$SCENARIO.broker.xml" || echo "image default (tests/golden/etc/broker.xml)")
artemis-version: $VERSION
artemis-commit: $(git -C "$(native "$ROOT/reference/activemq-artemis")" rev-parse HEAD)
generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)
MANIFEST
echo "golden $SCENARIO -> $OUT"

# Reference outputs for the data dir: Java's `artemis data print`, and what a
# client sees after the real broker recovers it.
"$ROOT/scripts/golden-print.sh" "$SCENARIO"
if [[ -f "$OUT/QUEUES" ]]; then
  "$ROOT/scripts/golden-verify.sh" "$SCENARIO" artemis > "$OUT/recovered.txt"
  echo "recovered $SCENARIO -> $OUT/recovered.txt"
fi
