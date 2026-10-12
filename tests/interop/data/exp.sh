#!/usr/bin/env bash
# `saranga data exp` against `artemis data exp` (Artemis 2.57.0's CLI) on
# each golden-corpus data directory: the XML must be the same.
#
#   tests/interop/data/exp.sh [scenario...]
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SARANGA="$ROOT/target/release/saranga"
export MSYS_NO_PATHCONV=1
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

cargo build -q --release -p saranga-cli --manifest-path "$(native "$ROOT/Cargo.toml")" || exit 1
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

SCENARIOS=("$@")
if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
  for d in "$ROOT"/tests/golden/*/data.tar.gz; do SCENARIOS+=("$(basename "$(dirname "$d")")"); done
fi

# A fresh copy of a scenario's data directory (the tools may add files).
fresh() {
  rm -rf "$WORK/$1" && mkdir -p "$WORK/$1"
  tar -xzf "$ROOT/tests/golden/$2/data.tar.gz" -C "$WORK/$1"
  local data; data="$(find "$WORK/$1" -type d -name bindings -print -quit)"
  dirname "$data"
}
args() {
  echo --bindings "$(native "$1/bindings")" --journal "$(native "$1/journal")" \
    --paging "$(native "$1/paging")" --large-messages "$(native "$1/large-messages")"
}

failed=0
for s in "${SCENARIOS[@]}"; do
  a="$(fresh java "$s")"
  # shellcheck disable=SC2046
  (cd "$WORK" && java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis data exp $(args "$a") \
    > "$WORK/java.xml" 2> "$WORK/java.err")
  b="$(fresh rust "$s")"
  # shellcheck disable=SC2046
  (cd "$WORK" && "$SARANGA" data exp $(args "$b") > "$WORK/rust.xml" 2> "$WORK/rust.err")
  # A message converted from AMQP gets a new user ID at each export.
  if [[ "$s" == amqp* ]]; then
    sed -i -E 's/ user-id="[^"]*"/ user-id="#"/' "$WORK/java.xml" "$WORK/rust.xml"
  fi
  if cmp -s "$WORK/java.xml" "$WORK/rust.xml"; then
    echo "PASS $s ($(grep -c '<message ' "$WORK/java.xml") messages)"
  else
    echo "FAIL $s"
    diff "$WORK/java.xml" "$WORK/rust.xml" | head -${DIFF_LINES:-20}
    tail -3 "$WORK/rust.err"
    failed=1
  fi
done
[[ $failed == 0 ]] && echo "ALL PASSED"
exit $failed
