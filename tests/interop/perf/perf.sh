#!/usr/bin/env bash
# The perf suite: `perf producer`, `consumer` and `client` against a broker,
# and `perf-journal` in an instance, their output normalized (rates and
# latencies as N, each command's lines sorted and unique, as the number of
# one-second reports varies) and compared with
# tests/interop/perf/expected.txt (made with the Artemis CLI against Artemis
# 2.57.0). The message totals are kept: they are exact. The --json report is
# reduced to its structure, and the --hdr log is decoded with HdrHistogram's
# own reader.
#
#   SUITE_CMD=tests/interop/perf/perf.sh scripts/interop.sh saranga|artemis
#
# CLI=artemis (the default) runs the Artemis CLI of the reference
# distribution; CLI=saranga runs target/release/saranga. UPDATE=1 rewrites
# expected.txt instead of comparing.
set -uo pipefail

HOSTPORT="${1:?host:port}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/perf"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }
export MSYS_NO_PATHCONV=1

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
# The client commands run in an instance with nothing in it; perf-journal in
# one `artemis create` makes.
mkdir -p "$WORK/empty/etc"
INSTANCE="$WORK/empty"

cli() {
  case "${CLI:-artemis}" in
    artemis)
      timeout 120 java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
        -Dartemis.instance="$(native "$INSTANCE")" \
        org.apache.activemq.artemis.boot.Artemis "$@" ;;
    saranga) ARTEMIS_INSTANCE="$(native "$INSTANCE")" timeout 120 "$ROOT/target/release/saranga" "$@" ;;
  esac
}

URL="tcp://$HOSTPORT"
# Without --silent, which would stop the reports.
run() {
  echo "=== $*"
  cli "$@" --url "$URL" </dev/null 2>&1
  echo "--- exit $?"
}
run_bare() {
  echo "=== $*"
  cli "$@" </dev/null 2>&1
  echo "--- exit $?"
}

# --max-pending 0: Artemis 2.57's perf producer can stall over Core with the
# default of 1 (a send completion is lost while it waits for one).
suite() {
  run perf producer --max-pending 0 --message-count 100 --persistent queue://perfq
  run perf consumer --message-count 100 queue://perfq
  run perf client --max-pending 0 --message-count 100 --show-latency queue://perfc
  # 10 messages over 4 producers: 2 each, 8 in all.
  run perf producer --max-pending 0 --message-count 10 --producers 2 --num-destinations 2 queue://pn
  run perf consumer --message-count 8 queue://pn0 queue://pn1
  run perf client --max-pending 0 --message-count 105 --commit-interval 10 --tx queue://ptx
  # Two subscribers each get every message.
  run perf client --max-pending 0 --message-count 50 --consumers 2 topic://pt
  run perf client --max-pending 0 --durable --clientID pc --message-count 50 topic://pd
  run perf client --max-pending 0 --shared 2 --consumers 2 --message-count 40 topic://ps
  run perf client --max-pending 0 --shared 1 --durable --clientID sd --message-count 20 topic://psd
  run perf producer --max-pending 0 --duration 2 --warmup 1 --show-latency queue://pdur
  run perf consumer --duration 2 --show-latency queue://pdur
  run perf client --max-pending 0 --message-count 300 --rate 200 --show-latency --json "$(native "$WORK/r.json")" --hdr "$(native "$WORK/h.hdr")" topic://pr
  echo "=== json"
  python "$(native "$HERE/normalize.py")" json "$(native "$WORK/r.json")"
  echo "=== hdr"
  java -cp "$(native "$DIST/lib")/*" "$(native "$HERE/HdrCheck.java")" "$(native "$WORK/h.hdr")" 2>&1
  run perf client --max-pending 0 --message-count 0 --duration 2 --rate 50 --clientID x queue://pid
  run perf client --max-pending 0 --durable queue://nope

  # perf-journal, in an instance.
  java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis create "$(native "$WORK/inst")" --silent --no-autotune \
    --user u --password p --require-login >/dev/null 2>&1
  INSTANCE="$WORK/inst"
  run_bare perf-journal --writes 20 --tries 2
  run_bare perf-journal --writes 20 --tries 2 --verbose --no-sync --sync-writes --journal-type ASYNCIO
  run_bare perf-journal --journal-type nope
  INSTANCE="$WORK/empty"
}

OUT="$WORK/out.txt"
suite | sed "s#$(native "$WORK")#<work>#g; s#$WORK#<work>#g" \
  | python "$(native "$HERE/normalize.py")" output > "$OUT"
[[ -n "${SAVE:-}" ]] && cp "$OUT" "$SAVE"
if [[ -n "${UPDATE:-}" ]]; then
  cp "$OUT" "$HERE/expected.txt"
  echo "== updated $HERE/expected.txt"
  exit 0
fi
if diff -u "$HERE/expected.txt" "$OUT"; then
  echo "ALL PASSED"
else
  echo "FAILED"
  exit 1
fi
