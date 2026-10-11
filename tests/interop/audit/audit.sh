#!/usr/bin/env bash
# The audit suite: runs Artemis CLI commands against a broker whose audit
# loggers are on, and compares its audit log, normalized by normalize.py,
# with expected.txt (made against Artemis 2.57.0).
#
#   tests/interop/audit/audit.sh [saranga|artemis]
#
# saranga (the default) runs a `saranga create` instance; artemis runs the
# reference image. UPDATE=1 rewrites expected.txt from the run; KEEP_LOG=<file>
# keeps the broker's audit.log.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/audit"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
OFFSET=720
PORT=$((61616 + OFFSET))
NAME=saranga-audit-ref
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

# The instance's log4j2.properties with the three audit loggers on.
audit_on() { sed -i -E 's/^(logger\.audit_[a-z]+\.level) = OFF/\1 = INFO/' "$1"; }

case "$BROKER" in
  saranga)
    cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
    I="$WORK/i"
    "$SARANGA" create "$(native "$I")" --silent --no-autotune --user artemis --password artemis \
      --require-login --port-offset $OFFSET > /dev/null 2>&1 || { echo "create failed"; exit 1; }
    audit_on "$I/etc/log4j2.properties"
    ARTEMIS_INSTANCE="$(native "$I")" "$SARANGA" run > "$WORK/broker.log" 2>&1 &
    PID=$!
    for _ in $(seq 1 100); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.1; done
    LOG="$I/log/audit.log"
    ;;
  artemis)
    mkdir -p "$WORK/etc-override"
    cp "$ROOT/crates/saranga-cli/templates/etc/log4j2.properties" "$WORK/etc-override/"
    audit_on "$WORK/etc-override/log4j2.properties"
    docker rm -f "$NAME" > /dev/null 2>&1
    docker create --name "$NAME" -e ANONYMOUS_LOGIN=false \
      -p "127.0.0.1:$PORT:61616" "saranga/artemis-ref:$VERSION" > /dev/null
    docker cp "$(native "$WORK/etc-override")" "$NAME":/var/lib/artemis-instance/etc-override > /dev/null
    docker start "$NAME" > /dev/null
    for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
    LOG="$WORK/audit.log"
    ;;
  *) echo "usage: audit.sh [saranga|artemis]"; exit 2 ;;
esac

cli() {
  timeout 60 java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis "$@" --url "tcp://127.0.0.1:$PORT" --silent \
    > /dev/null 2>&1
}
# Each step is marked in the log by a connection of its own
# (normalize.py splits the log there), so the order within a step may
# differ but the steps may not.
cli queue create --name aq --address aq --anycast --durable --auto-create-address \
  --user artemis --password artemis
cli producer --destination queue://aq --message-count 1 --user artemis --password artemis
cli consumer --destination queue://aq --message-count 1 --receive-timeout 5000 \
  --user artemis --password artemis
cli queue stat --queueName aq --user artemis --password artemis
cli queue stat --user bad --password bad
cli queue delete --name aq --user artemis --password artemis
sleep 2
[[ "$BROKER" == artemis ]] && docker exec "$NAME" cat /var/lib/artemis-instance/log/audit.log > "$LOG"

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
  echo "FAILED: the audit log differs from Artemis's"
  exit 1
fi
