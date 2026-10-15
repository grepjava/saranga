#!/usr/bin/env bash
# Phase 10's exit check: every suite of tests/interop/cluster with Java and
# Saranga brokers in each role. The two-node suites run in all four
# Artemis/Saranga pairings, so that federation, bridges, broker connections
# and mirroring each go from Java to Saranga and from Saranga to Java; the
# three-node mixed cluster runs all Saranga and in four mixes, with each
# kind as the LOCAL grouping node.
#
#   scripts/cluster-interop.sh [suite...]
#
# Suites: discovery bridge cluster grouping federation amqp-connection
# amqp-federation amqp-bridge amqp-mirror mixed (default: all of them).
# Needs the Artemis distribution (scripts/build-artemis.sh) and java.
# Prints a line per run and a summary; exits with the number of failures.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HERE="$ROOT/tests/interop/cluster"
SUITES=("$@")
[[ ${#SUITES[@]} -eq 0 ]] && SUITES=(discovery bridge cluster grouping federation
  amqp-connection amqp-federation amqp-bridge amqp-mirror mixed)
PAIRS=("artemis saranga" "saranga artemis" "saranga saranga" "artemis artemis")
TRIPLES=("saranga saranga saranga" "artemis saranga saranga" "saranga artemis artemis"
  "artemis saranga artemis" "saranga artemis saranga")

failed=0 runs=0
summary=()
run() {
  local label="$1"; shift
  local log
  log="$(mktemp)"
  runs=$((runs + 1))
  if env "$@" > "$log" 2>&1; then
    summary+=("ok      $label")
    rm -f "$log"
  else
    failed=$((failed + 1))
    summary+=("FAILED  $label (log in $log)")
  fi
  echo "${summary[-1]}"
}

for suite in "${SUITES[@]}"; do
  script="$HERE/$suite.sh"
  [[ -f "$script" ]] || { echo "unknown suite $suite"; exit 2; }
  case "$suite" in
    mixed)
      for t in "${TRIPLES[@]}"; do
        set -- $t
        run "$suite ($1, $2, $3)" NODE1="$1" NODE2="$2" NODE3="$3" bash "$script"
      done
      ;;
    discovery)
      for p in "${PAIRS[@]}"; do
        set -- $p
        run "$suite (primary $1, backup $2)" PRIMARY="$1" BACKUP="$2" bash "$script"
      done
      ;;
    bridge)
      for p in "${PAIRS[@]}"; do
        set -- $p
        run "$suite (source $1, target $2)" SOURCE="$1" TARGET="$2" bash "$script"
      done
      ;;
    *)
      for p in "${PAIRS[@]}"; do
        set -- $p
        run "$suite ($1 node 1, $2 node 2)" NODE1="$1" NODE2="$2" bash "$script"
      done
      ;;
  esac
done

echo
echo "== $((runs - failed)) of $runs runs passed"
printf '%s\n' "${summary[@]}"
exit $failed
