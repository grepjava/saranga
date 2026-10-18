#!/usr/bin/env bash
# Packages a release build of saranga as a home, laid out as an Artemis
# home is (bin/saranga, web/ for Artemis's console.war), with the licence,
# the changelog, COMPAT.md and the guide:
#
#   scripts/package.sh <target> [binary]
#
# The binary defaults to target/<target>/release/saranga(.exe). The archive,
# dist/saranga-<version>-<target>.tar.gz (.zip for Windows), is printed.
set -euo pipefail

TARGET="${1:?usage: package.sh <target> [binary]}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)"
EXE=""
[[ "$TARGET" == *windows* ]] && EXE=".exe"
BINARY="${2:-target/$TARGET/release/saranga$EXE}"
[[ -f "$BINARY" ]] || { echo "no binary at $BINARY" >&2; exit 1; }

NAME="saranga-$VERSION-$TARGET"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
HOME_DIR="$STAGE/$NAME"
mkdir -p "$HOME_DIR/bin" "$HOME_DIR/web" "$HOME_DIR/docs"
cp "$BINARY" "$HOME_DIR/bin/saranga$EXE"
chmod 755 "$HOME_DIR/bin/saranga$EXE"
cp LICENSE NOTICE README.md CHANGELOG.md COMPAT.md "$HOME_DIR/"
cp -r docs/guide "$HOME_DIR/docs/"
cp docs/benchmarks.md "$HOME_DIR/docs/"
cat > "$HOME_DIR/web/README.txt" <<'EOF'
Saranga ships no web console. Put Artemis's console.war (a directory, from
an Artemis 2.57.0 distribution's web/) here to have it served, as an
instance's bootstrap.xml declares it.
EOF

mkdir -p dist
if [[ -n "$EXE" ]]; then
  ARCHIVE="dist/$NAME.zip"
  rm -f "$ARCHIVE"
  native() { command -v cygpath >/dev/null && cygpath -w "$1" || echo "$1"; }
  pwsh -NoProfile -Command "Compress-Archive -Path '$(native "$HOME_DIR")' -DestinationPath '$(native "$ROOT/$ARCHIVE")'"
else
  # The modes are set here, not taken from a file system that may not
  # keep them (NTFS): directories and the binary 755, the rest 644.
  ARCHIVE="dist/$NAME.tar.gz"
  TAR="$STAGE/$NAME.tar"
  if tar --version 2>/dev/null | grep -q GNU; then
    OWNER=(--owner=0 --group=0 --numeric-owner)
    tar -C "$STAGE" -cf "$TAR" "${OWNER[@]}" --mode=u=rwX,go=rX --exclude="$NAME/bin/saranga" "$NAME"
    tar -C "$STAGE" -rf "$TAR" "${OWNER[@]}" --mode=755 "$NAME/bin/saranga"
  else
    # BSD tar (macOS) has no --mode; its file systems keep chmod's.
    find "$HOME_DIR" -type d -exec chmod 755 {} +
    find "$HOME_DIR" -type f -exec chmod 644 {} +
    chmod 755 "$HOME_DIR/bin/saranga"
    tar -C "$STAGE" -cf "$TAR" --uid 0 --gid 0 --uname root --gname root "$NAME"
  fi
  gzip -9 -n -c "$TAR" > "$ARCHIVE"
fi
echo "$ARCHIVE"
