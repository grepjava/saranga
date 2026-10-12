#!/usr/bin/env bash
# `data imp` round trips: a golden-corpus data directory exported with
# `artemis data exp`, imported into a fresh broker, which is stopped and
# exported again; content.py compares the two exports (without IDs and new
# user IDs).
#
#   tests/interop/data/imp.sh [scenario...]
#
# Each scenario is imported three ways: with `saranga data imp` and with
# `artemis data imp` into Saranga, and with `saranga data imp` into Artemis
# (the reference image; NO_ARTEMIS=1 skips it).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
HERE="$ROOT/tests/interop/data"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
OFFSET=740
PORT=$((61616 + OFFSET))
NAME=saranga-imp-ref
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
WORK="$(mktemp -d)"
PID=""
cleanup() {
  [[ -n "$PID" ]] && kill "$PID" 2>/dev/null
  docker rm -f "$NAME" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

SCENARIOS=("$@")
if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
  for d in "$ROOT"/tests/golden/*/data.tar.gz; do SCENARIOS+=("$(basename "$(dirname "$d")")"); done
fi

artemis() {
  java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis "$@"
}
# `artemis data exp` of a data directory.
export_dir() {
  (cd "$WORK" && artemis data exp --bindings "$(native "$1/bindings")" --journal "$(native "$1/journal")" \
    --paging "$(native "$1/paging")" --large-messages "$(native "$1/large-messages")" 2>/dev/null)
}
# The names of the source's addresses and queues.
names() {
  python -I -c 'import sys, xml.etree.ElementTree as ET
for b in ET.parse(sys.argv[1]).getroot().find("bindings"): print(b.attrib["name"])' "$(native "$1")"
}
content() { python -I "$(native "$HERE/content.py")" "$(native "$1")" "$(native "$WORK/names.txt")"; }
wait_port() { for _ in $(seq 1 150); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && return 0; sleep 0.2; done; return 1; }

# Imports $1 with importer $2 (saranga|artemis) into a fresh Saranga
# instance; the export of its data afterwards is $3.
into_saranga() {
  local i="$WORK/i"
  rm -rf "$i"
  "$SARANGA" create "$(native "$i")" --silent --no-autotune --user a --password a --require-login \
    --port-offset $OFFSET > /dev/null 2>&1 || { echo "create failed"; return 1; }
  ARTEMIS_INSTANCE="$(native "$i")" "$SARANGA" run > "$WORK/broker.log" 2>&1 &
  PID=$!
  wait_port || { echo "the broker did not start"; tail "$WORK/broker.log"; return 1; }
  import "$1" "$2"
  ARTEMIS_INSTANCE="$(native "$i")" "$SARANGA" stop
  wait "$PID"; PID=""
  export_dir "$i/data" > "$3"
}

# Imports $1 with saranga into the Artemis image; its export is $2.
into_artemis() {
  docker rm -f "$NAME" > /dev/null 2>&1
  docker run -d --name "$NAME" -e ANONYMOUS_LOGIN=false -e ARTEMIS_USER=a -e ARTEMIS_PASSWORD=a \
    -p "127.0.0.1:$PORT:61616" "saranga/artemis-ref:$VERSION" > /dev/null
  for _ in $(seq 1 90); do docker logs "$NAME" 2>&1 | grep -q AMQ221007 && break; sleep 1; done
  import "$1" saranga
  docker stop "$NAME" > /dev/null
  rm -rf "$WORK/adata" && mkdir -p "$WORK/adata"
  docker cp "$NAME":/var/lib/artemis-instance/data "$(native "$WORK/adata")" > /dev/null
  export_dir "$WORK/adata/data" > "$2"
}

import() {
  local args=(data imp --input "$(native "$1")" --url "tcp://127.0.0.1:$PORT" --user a --password a)
  if [[ "$2" == saranga ]]; then
    "$SARANGA" "${args[@]}" > "$WORK/imp.log" 2>&1
  else
    artemis "${args[@]}" > "$WORK/imp.log" 2>&1
  fi || { echo "import failed:"; tail -5 "$WORK/imp.log"; }
}

failed=0
check() {
  if diff <(content "$WORK/src.xml") <(content "$1") > "$WORK/diff.txt"; then
    echo "PASS $2"
  else
    echo "FAIL $2"
    head -${DIFF_LINES:-10} "$WORK/diff.txt"
    failed=1
  fi
}

for s in "${SCENARIOS[@]}"; do
  rm -rf "$WORK/src" && mkdir -p "$WORK/src"
  tar -xzf "$ROOT/tests/golden/$s/data.tar.gz" -C "$WORK/src"
  export_dir "$WORK/src/data" > "$WORK/src.xml"
  names "$WORK/src.xml" > "$WORK/names.txt"
  into_saranga "$WORK/src.xml" saranga "$WORK/out.xml" && check "$WORK/out.xml" "$s: saranga imp -> saranga"
  into_saranga "$WORK/src.xml" artemis "$WORK/out.xml" && check "$WORK/out.xml" "$s: artemis imp -> saranga"
  if [[ -z "${NO_ARTEMIS:-}" ]]; then
    into_artemis "$WORK/src.xml" "$WORK/out.xml" && check "$WORK/out.xml" "$s: saranga imp -> artemis"
  fi
done
[[ $failed == 0 ]] && echo "ALL PASSED"
exit $failed
