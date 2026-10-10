#!/usr/bin/env bash
# Generate one golden-corpus scenario against the real Artemis broker.
#
#   scripts/golden.sh <scenario>        e.g. scripts/golden.sh core-basic
#
# Flow: start saranga/artemis-ref in Docker -> start the capture proxy in front
# of it -> run tests/golden-gen/scenarios/<Scenario>.java on the host JDK
# through the proxy -> stop the broker cleanly -> copy its data dir out.
# Output: tests/golden/<scenario>/{wire/conn-N/{c2s,s2c}.bin, data.tar.gz, MANIFEST}
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

rm -rf "$OUT" && mkdir -p "$OUT/wire"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -e ANONYMOUS_LOGIN=true \
  -p "127.0.0.1:$BROKER_PORT:61616" "$IMAGE" >/dev/null

echo "waiting for broker..."
for _ in $(seq 1 60); do
  docker logs "$NAME" 2>&1 | grep -q "AMQ221007" && break   # "Server is now active"
  sleep 1
done
docker logs "$NAME" 2>&1 | grep -q "AMQ221007" || { docker logs "$NAME"; exit 1; }

cargo build -q -p saranga-capture --manifest-path "$(native "$ROOT/Cargo.toml")"
"$ROOT/target/debug/saranga-capture" "127.0.0.1:$PROXY_PORT" "127.0.0.1:$BROKER_PORT" "$(native "$OUT/wire")" &
PROXY_PID=$!
sleep 1

java -cp "$(native "$DIST/lib")/*" \
  "$(native "$ROOT/tests/golden-gen/scenarios/$CLASS.java")" "tcp://127.0.0.1:$PROXY_PORT"

kill "$PROXY_PID"; PROXY_PID=""
docker stop -t 60 "$NAME" >/dev/null           # clean shutdown, journal synced
docker cp "$NAME:/var/lib/artemis-instance/data" "$(native "$OUT/data")"
# Journal files are pre-allocated (mostly zeros): archive them byte-exact, compressed.
tar -C "$OUT" -czf "$OUT/data.tar.gz" data && rm -rf "$OUT/data"

cat > "$OUT/MANIFEST" <<MANIFEST
scenario: $SCENARIO
artemis-version: $VERSION
artemis-commit: $(git -C "$(native "$ROOT/reference/activemq-artemis")" rev-parse HEAD)
generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)
MANIFEST
echo "golden $SCENARIO -> $OUT"
