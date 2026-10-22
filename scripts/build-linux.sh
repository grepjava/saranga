#!/usr/bin/env bash
# Builds the Linux release binaries, x86_64 and aarch64, against glibc 2.28
# (RHEL 8, Debian 10, Ubuntu 18.04 and later), with cargo-zigbuild in a
# Rust container: Zig links against that glibc and cross-compiles aarch64,
# so one x86_64 machine with Docker builds both.
#
#   scripts/build-linux.sh
#
# The binaries land in target/<target>/release/saranga, for package.sh.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TOOLCHAIN="$(sed -n 's/^channel = "\(.*\)"$/\1/p' "$ROOT/rust-toolchain.toml")"
GLIBC=2.28
TARGETS=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)
export MSYS_NO_PATHCONV=1   # stop Git Bash rewriting container paths
HOST_ROOT="$ROOT"
command -v cygpath >/dev/null && HOST_ROOT="$(cygpath -m "$ROOT")"

# The registry and the build live in volumes, not the bind mount.
docker run --rm \
  -v "$HOST_ROOT:/src" -w /src \
  -v saranga-cargo-registry:/usr/local/cargo/registry \
  -v saranga-linux-target:/target \
  -e CARGO_TARGET_DIR=/target \
  -e SARANGA_CONSOLE_REQUIRED \
  "rust:$TOOLCHAIN-bookworm" bash -euc "
    apt-get -qq update && apt-get -qq install -y python3-pip > /dev/null
    pip3 install -q --break-system-packages ziglang==0.15.2 cargo-zigbuild==0.23.4
    rustup target add ${TARGETS[*]}
    cargo zigbuild --release --locked -p saranga-cli $(printf -- '--target %s.'$GLIBC' ' "${TARGETS[@]}")
    for t in ${TARGETS[*]}; do
      mkdir -p /src/target/\$t/release
      cp /target/\$t/release/saranga /src/target/\$t/release/saranga
    done
  "
for t in "${TARGETS[@]}"; do
  echo "target/$t/release/saranga"
done
