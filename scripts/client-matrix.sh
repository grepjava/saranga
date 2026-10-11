#!/usr/bin/env bash
# The non-Java AMQP clients of the Phase 7 client matrix against Saranga, or
# against the reference Artemis broker to check the programs themselves:
# Qpid Proton's Python binding, AMQPNetLite (.NET) and fe2o3-amqp (Rust),
# each running tests/interop/clients/<language>.
#
#   scripts/client-matrix.sh [saranga|artemis] [python|dotnet|rust...]
#
# Needs python (3.9+), the .NET SDK (10) and cargo. The clients are fetched
# (pip, NuGet, crates.io) and built in CLIENTS_DIR (default: a directory
# under the system temp directory), never in the repository.
set -euo pipefail

TARGET="${1:-saranga}"; shift || true
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CLIENTS_DIR="${CLIENTS_DIR:-${TMPDIR:-/tmp}/saranga-clients}"
LANGS=("$@")
[[ ${#LANGS[@]} -eq 0 ]] && LANGS=(python dotnet rust)
export CONFIG="$ROOT/tests/interop/conversion/broker.xml"
mkdir -p "$CLIENTS_DIR"
native() { command -v cygpath >/dev/null && cygpath -m "$1" || echo "$1"; }

failed=0
for lang in "${LANGS[@]}"; do
  echo "== $lang"
  case "$lang" in
    python)
      dir="$CLIENTS_DIR/python"
      if [[ ! -d "$dir/venv" ]]; then
        python -m venv "$dir/venv"
        "$dir/venv/Scripts/python" -m pip install -q python-qpid-proton==0.40.0 2>/dev/null \
          || "$dir/venv/bin/python" -m pip install -q python-qpid-proton==0.40.0
      fi
      py="$dir/venv/Scripts/python"; [[ -x "$py" || -x "$py.exe" ]] || py="$dir/venv/bin/python"
      cmd="$py -I $(native "$ROOT/tests/interop/clients/python/amqp_check.py")"
      ;;
    dotnet)
      dir="$CLIENTS_DIR/dotnet"
      mkdir -p "$dir"
      cp "$ROOT"/tests/interop/clients/dotnet/{Program.cs,AmqpCheck.csproj} "$dir/"
      dotnet build -nologo -v q -c Release -o "$dir/out" "$dir/AmqpCheck.csproj" >/dev/null
      cmd="$dir/out/AmqpCheck"
      ;;
    rust)
      dir="$CLIENTS_DIR/rust"
      mkdir -p "$dir/src"
      cp "$ROOT/tests/interop/clients/rust/Cargo.toml" "$dir/"
      cp "$ROOT/tests/interop/clients/rust/src/main.rs" "$dir/src/"
      # tokio-util 0.7.17 and later break type inference in fe2o3-amqp 0.18.
      (cd "$dir" && cargo generate-lockfile -q && cargo update -q -p tokio-util --precise 0.7.16 \
        && cargo build -q --release)
      cmd="$dir/target/release/amqp_check"
      ;;
    *) echo "unknown client $lang"; exit 2 ;;
  esac
  SUITE_CMD="$cmd" "$ROOT/scripts/interop.sh" "$TARGET" || failed=$((failed + 1))
done
exit $failed
