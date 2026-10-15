#!/usr/bin/env bash
# The full-disk soak: an in-process broker on a small tmpfs, with producers
# of small, paged and large messages filling it, cycle after cycle. Odd
# cycles are held back by max-disk-usage; even ones run the disk out of
# space, so a store write fails with ENOSPC, the broker stops on the
# critical I/O error, and a broker restarted on the same data must have
# every message whose send was answered, once and in order
# (crates/saranga-client/tests/disk_soak.rs).
#
#   scripts/disk-soak.sh [seconds] [size]
#
# Linux only: the tmpfs is mounted with sudo, unless SOAK_DIR names a small
# file system already mounted (from Windows, mount one in WSL as root and
# run the script there with SOAK_DIR). Defaults: 120 seconds on 64 MiB.
set -euo pipefail

SECONDS_="${1:-120}"
SIZE="${2:-64m}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="${SOAK_DIR:-}"
MOUNTED=""

cleanup() {
    if [ -n "$MOUNTED" ]; then
        sudo umount "$MOUNTED" || true
        rmdir "$MOUNTED" || true
    fi
}
trap cleanup EXIT

if [ -z "$DIR" ]; then
    DIR="$(mktemp -d)"
    sudo mount -t tmpfs -o "size=$SIZE" tmpfs "$DIR"
    sudo chown "$(id -u):$(id -g)" "$DIR"
    MOUNTED="$DIR"
fi
echo "== soaking for ${SECONDS_}s on $DIR ($(df -h "$DIR" | awk 'NR==2 {print $2}'))"

cd "$ROOT"
SARANGA_SOAK_DIR="$DIR" SARANGA_SOAK_SECONDS="$SECONDS_" \
    cargo test --release -p saranga-client --test disk_soak -- --ignored --nocapture
