#!/usr/bin/env bash
# The data tools failing, against Artemis 2.57.0's CLI: what each prints on
# stdout and stderr, and its exit status, must be the same. Artemis reports
# what fails in the tool on stderr as "Error:<message>" between blank lines
# (`Configurable.treatError`) and exits 0; what fails before (a missing
# directory of print or recover) ends it with the usage hint. Java's stack
# traces are left out of the comparison.
#
#   tests/interop/data/errors.sh
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

artemis() {
  java -cp "$(native "$DIST/lib")/*" -Dartemis.home="$(native "$DIST")" \
    org.apache.activemq.artemis.boot.Artemis "$@"
}
saranga() { "$SARANGA" "$@"; }

# Stdout, stderr and the exit status, without stack traces and colours.
normalize() {
  tr -d '\r' | sed -e 's/\x1b\[[0-9;]*m//g' -e 's/Usage: artemis /Usage: saranga /' |
    grep -Ev '^[[:space:]]+at |^[[:space:]]*\.\.\. [0-9]+ more|^Caused by: |^[a-z][a-zA-Z0-9_]*(\.[a-zA-Z0-9_$]+)+(: .*)?$'
}

failed=0
# Each case runs in the same fresh directory, with bindings, journal and
# paging folders b, j and p.
check() {
  for tool in artemis saranga; do
    rm -rf "$WORK/c" && mkdir -p "$WORK/c/b" "$WORK/c/j" "$WORK/c/p"
    (cd "$WORK/c" && "$tool" "$@" < /dev/null > "$WORK/out" 2> "$WORK/err"; echo "exit $?" > "$WORK/code")
    { echo "-- stdout"; normalize < "$WORK/out"
      echo "-- stderr"; normalize < "$WORK/err"
      cat "$WORK/code"; } > "$WORK/$tool.txt"
  done
  if diff -u "$WORK/artemis.txt" "$WORK/saranga.txt" > "$WORK/diff"; then
    echo "ok    $*"
  else
    echo "FAIL  $*"
    cat "$WORK/diff"
    failed=1
  fi
}

check data print --bindings b --journal j --paging p
check data print --bindings nb --journal j --paging p --large-messages b
check data recover --bindings b --journal j --paging p --target t
check data exp --bindings nb --journal j --paging p --large-messages b
check data decode --directory d --input nosuch
check data encode --directory nosuch
check data compact

[[ $failed -eq 0 ]] && echo "ALL PASSED"
exit $failed
