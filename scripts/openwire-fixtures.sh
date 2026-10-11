#!/usr/bin/env bash
# Regenerate the OpenWire codec fixtures with the ActiveMQ 5 marshallers that
# Artemis ships (tests/golden-gen/openwire/CodecFixtures.java), into
# tests/golden/openwire/codec/v<N>.txt.gz. Run scripts/build-artemis.sh first.
#
# tests/golden-gen/openwire/fields.txt and the generated
# crates/saranga-protocol-openwire/src/commands.rs come from schema.json:
#   python tests/golden-gen/openwire/generate.py schema.json \
#       crates/saranga-protocol-openwire/src/commands.rs fields.txt
#   cargo fmt -p saranga-protocol-openwire
set -euo pipefail

VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="${ARTEMIS_DIST:-$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION}"
GEN="$ROOT/tests/golden-gen/openwire"
OUT="$ROOT/tests/golden/openwire/codec"

native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

[[ -d "$DIST" ]] || { echo "missing $DIST: run scripts/build-artemis.sh"; exit 1; }
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
java -cp "$(native "$DIST/lib")/*" "$(native "$GEN/CodecFixtures.java")" "$(native "$GEN/fields.txt")" "$(native "$TMP")" 2>&1 | grep -v '^WARNING' || true
mkdir -p "$OUT"
for f in "$TMP"/v*.txt; do
  gzip -9 -n -c "$f" > "$OUT/$(basename "$f").gz"
done
ls -l "$OUT"
