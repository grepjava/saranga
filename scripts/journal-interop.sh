#!/usr/bin/env bash
# Checks that Artemis's JournalImpl and Saranga's journal recover the same
# records from a journal Saranga wrote, and that Saranga reads the journal
# back after Artemis has loaded (and appended to) it.
#
#   scripts/journal-interop.sh
#
# Requires a JDK 17+ and the Artemis distribution built by
# scripts/build-artemis.sh.
set -euo pipefail

VERSION="${ARTEMIS_VERSION:-2.57.0}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIST="$ROOT/reference/activemq-artemis/artemis-distribution/target/apache-artemis-$VERSION-bin/apache-artemis-$VERSION"
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cargo build -q -p saranga-journal --example sample --manifest-path "$(native "$ROOT/Cargo.toml")"
SAMPLE="$ROOT/target/debug/examples/sample"
"$SAMPLE" write "$(native "$WORK/journal")"
# Loading appends (rollbacks of incomplete transactions), so each side
# loads its own copy.
cp -r "$WORK/journal" "$WORK/java"
"$SAMPLE" dump "$(native "$WORK/journal")" > "$WORK/saranga.txt"
java -cp "$(native "$DIST/lib")/*" "$(native "$ROOT/tests/interop/journal/JournalDump.java")" \
  "$(native "$WORK/java")" activemq-data amq 65536 2>/dev/null > "$WORK/artemis.txt"
diff -u "$WORK/artemis.txt" "$WORK/saranga.txt"
echo "Artemis and Saranga recover the same $(wc -l < "$WORK/saranga.txt") lines"

# Artemis's load rolled back the incomplete transaction; Saranga must agree.
"$SAMPLE" dump "$(native "$WORK/java")" > "$WORK/saranga-after-java.txt"
grep -v '^F ' "$WORK/saranga.txt" > "$WORK/expected.txt"
diff -u "$WORK/expected.txt" "$WORK/saranga-after-java.txt"
echo "Saranga reads the journal back after Artemis loaded it"
