#!/usr/bin/env bash
# Run Java's `artemis data print` over a golden scenario's data dir.
#
#   scripts/golden-print.sh <scenario>     e.g. scripts/golden-print.sh core-basic
#
# Extracts tests/golden/<scenario>/data.tar.gz into a temp dir, mounts it at
# /golden/data in the reference image and runs the tool with explicit
# --journal/--bindings/--paging/--large-messages, so no broker instance is needed.
# Output: tests/golden/<scenario>/data-print.txt (stdout of the tool, verbatim).
set -euo pipefail

SCENARIO="${1:?usage: golden-print.sh <scenario>}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="saranga/artemis-ref:$VERSION"
OUT="$ROOT/tests/golden/$SCENARIO"
export MSYS_NO_PATHCONV=1

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

[[ -f "$OUT/data.tar.gz" ]] || { echo "no $OUT/data.tar.gz"; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
tar -C "$WORK" -xzf "$OUT/data.tar.gz"
mkdir -p "$WORK/data/paging" "$WORK/data/large-messages"

# Fixed container paths keep the output free of host paths.
docker run --rm --user root -v "$(native "$WORK/data"):/golden/data" \
  --entrypoint /opt/artemis/bin/artemis "$IMAGE" \
  data print \
  --journal /golden/data/journal --bindings /golden/data/bindings \
  --paging /golden/data/paging --large-messages /golden/data/large-messages \
  > "$OUT/data-print.txt"
echo "data print $SCENARIO -> $OUT/data-print.txt"
