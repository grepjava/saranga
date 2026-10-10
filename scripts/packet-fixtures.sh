#!/usr/bin/env bash
# Regenerate packet fixtures with the real Artemis classes.
#
#   scripts/packet-fixtures.sh <Group>    e.g. scripts/packet-fixtures.sh Connection
#
# Runs tests/golden-gen/packets/<Group>Fixtures.java (plus the shared
# Fixtures.java) on the host JDK (22+) against the reference distribution, and
# writes tests/golden/packets/<group>/*.bin. Run scripts/build-artemis.sh first.
set -euo pipefail

GROUP="${1:?usage: packet-fixtures.sh <Group>}"
VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
SRC="$ROOT/tests/golden-gen/packets/${GROUP}Fixtures.java"
OUT="$ROOT/tests/golden/packets/$(echo "$GROUP" | tr '[:upper:]' '[:lower:]')"

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

[[ -f "$SRC" ]] || { echo "no $SRC"; exit 1; }
[[ -d "$DIST" ]] || { echo "missing $DIST: run scripts/build-artemis.sh"; exit 1; }
rm -rf "$OUT"
# Multi-file source launch (JEP 458) finds Fixtures.java next to the generator.
java -cp "$(native "$DIST/lib")/*" "$(native "$SRC")" "$(native "$OUT")" 2>&1 | grep -v '^WARNING'
