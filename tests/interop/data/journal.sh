#!/usr/bin/env bash
# The journal tools against Artemis 2.57.0's on each golden-corpus data
# directory:
#  - `data encode`: the same text;
#  - `data decode` of that text: a journal whose `artemis data exp` (with
#    the original bindings) is the same as the original's;
#  - `data recover`: a journal whose export is the same as that of the
#    journal `artemis data recover` makes.
#
#   tests/interop/data/journal.sh [scenario...]
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

artemis() {
  (cd "$WORK" && java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis "$@" 2>/dev/null)
}
saranga() { (cd "$WORK" && "$SARANGA" "$@" 2>/dev/null); }
# A fresh copy of the scenario's data directory.
fresh() {
  rm -rf "$WORK/$1" && mkdir -p "$WORK/$1"
  tar -xzf "$ROOT/tests/golden/$2/data.tar.gz" -C "$WORK/$1"
  echo "$WORK/$1/data"
}
# `artemis data exp` of journal $2 with the bindings, paging and large
# messages of data directory $1.
export_with() {
  artemis data exp --bindings "$(native "$1/bindings")" --journal "$(native "$2")" \
    --paging "$(native "$1/paging")" --large-messages "$(native "$1/large-messages")" \
    | sed -E 's/ user-id="[^"]*"//'
}

failed=0
result() {
  if cmp -s "$2" "$3"; then
    echo "PASS $1"
  else
    echo "FAIL $1"
    diff "$2" "$3" | head -${DIFF_LINES:-10}
    failed=1
  fi
}

for s in "${SCENARIOS[@]}"; do
  d="$(fresh a "$s")"
  artemis data encode --directory "$(native "$d/journal")" > "$WORK/java.txt"
  d2="$(fresh b "$s")"
  saranga data encode --directory "$(native "$d2/journal")" > "$WORK/rust.txt"
  # Java's println ends lines with the platform's separator.
  sed -i 's/\r$//' "$WORK/java.txt"
  result "$s: encode" "$WORK/java.txt" "$WORK/rust.txt"

  rm -rf "$WORK/decoded"
  saranga data decode --directory "$(native "$WORK/decoded")" --input "$(native "$WORK/java.txt")"
  export_with "$d" "$d/journal" > "$WORK/original.xml"
  export_with "$d" "$WORK/decoded" > "$WORK/decoded.xml"
  result "$s: decode" "$WORK/original.xml" "$WORK/decoded.xml"

  d="$(fresh a "$s")"
  artemis data recover --bindings "$(native "$d/bindings")" --journal "$(native "$d/journal")" \
    --paging "$(native "$d/paging")" --large-messages "$(native "$d/large-messages")" \
    --target "$(native "$WORK/java-recovered")" > /dev/null
  export_with "$d" "$WORK/java-recovered" > "$WORK/java-recovered.xml"
  d2="$(fresh b "$s")"
  saranga data recover --bindings "$(native "$d2/bindings")" --journal "$(native "$d2/journal")" \
    --paging "$(native "$d2/paging")" --large-messages "$(native "$d2/large-messages")" \
    --target "$(native "$WORK/rust-recovered")" > /dev/null
  export_with "$d2" "$WORK/rust-recovered" > "$WORK/rust-recovered.xml"
  result "$s: recover" "$WORK/java-recovered.xml" "$WORK/rust-recovered.xml"
  rm -rf "$WORK/java-recovered" "$WORK/rust-recovered"
done
[[ $failed == 0 ]] && echo "ALL PASSED"
exit $failed
