#!/usr/bin/env bash
# The Phase 3 exit checks, in one run:
#  1. Saranga recovers every golden data directory Artemis wrote and shows a
#     client what Artemis showed (tests/golden/<scenario>/recovered.txt).
#  2. scripts/persistence.sh in every direction: Saranga to Saranga,
#     Artemis to Saranga and Saranga to Artemis, after a clean stop and
#     after a kill.
#
#   scripts/cross-store.sh
#
# Requires Docker, a JDK 17+ and the Artemis distribution (scripts/build-artemis.sh).
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
failed=0

# Scenarios whose recovery differs from Artemis only through features of
# later phases (none at present).
declare -A LATER=()

# Scenarios whose messages get a new random user ID each time they are
# read (AMQP message IDs that are not UUIDs, converted to Core).
declare -A RANDOM_USER_IDS=([amqp-durable]=1)
mask() { if [[ -n "${RANDOM_USER_IDS[$1]:-}" ]]; then sed -E 's/userID=[0-9a-f-]+/userID=*/'; else cat; fi; }

echo "== Saranga recovers the golden data directories"
for dir in "$ROOT"/tests/golden/*/; do
  s="$(basename "$dir")"
  [[ -f "$dir/data.tar.gz" && -f "$dir/recovered.txt" ]] || continue
  got="$(bash "$ROOT/scripts/golden-verify.sh" "$s" saranga 2>/dev/null | tr -d '\r' | mask "$s")"
  if diff -q <(tr -d '\r' < "$dir/recovered.txt" | mask "$s") <(echo "$got") >/dev/null; then
    echo "ok       $s"
  elif [[ -n "${LATER[$s]:-}" ]]; then
    echo "expected $s (differs: ${LATER[$s]})"
  else
    echo "FAIL     $s"
    diff <(tr -d '\r' < "$dir/recovered.txt" | mask "$s") <(echo "$got") | head -20
    failed=1
  fi
done

for combo in "saranga saranga" "artemis saranga" "saranga artemis"; do
  for mode in stop kill; do
    out="$(bash "$ROOT/scripts/persistence.sh" $combo $mode 2>&1)"
    if [[ "$(grep -c '^ALL PASSED' <<<"$out")" == 2 ]]; then
      echo "ok       persistence $combo $mode"
    else
      echo "FAIL     persistence $combo $mode"
      grep -E '^(FAIL|==)' <<<"$out"
      failed=1
    fi
  done
done
exit $failed
