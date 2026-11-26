#!/usr/bin/env bash
# Real loadable mismatch control, isolated from v1 Go package initialization.
# The fixture is Linux amd64, where LD_BIND_NOW also tests eager symbol binding.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
[[ $(go env GOOS)-$(go env GOARCH) == linux-amd64 ]]
MATCHING_DIR=${1:?directory containing the verified DuckDB 2.0.1 shared library}
FIXTURE="$REPO_ROOT/dynamic-dir/v2.0.0"
mkdir -p "$FIXTURE"
SHA256=993ad89e7dfca66cd6da994c2cad090c2fc160ddf9f550498a098ab4bb13cfb1
ARCHIVE="$FIXTURE/duckdb-2.0.0.tar.gz"
verify() { printf '%s  %s\n' "$SHA256" "$1" | sha256sum -c --strict -; }
if ! verify "$ARCHIVE" >/dev/null 2>&1; then
  curl --retry 3 -fsSL \
    https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb-shared-libs-linux-amd64.tar.gz \
    -o "$ARCHIVE"
  verify "$ARCHIVE"
fi
tar -xzf "$ARCHIVE" -C "$FIXTURE"

DEPS=$(go list -tags=duckdb_use_lib -deps -test ./internal/capi2)
if grep -Fxq github.com/duckdb/duckdb-go-bindings <<< "$DEPS"; then
  echo 'capi2: mismatch binary must not initialize v1 bindings' >&2
  exit 1
fi
go test -c -tags=duckdb_use_lib -o "$FIXTURE/probe" \
  -ldflags="-extldflags '-L$MATCHING_DIR -Wl,-rpath,$MATCHING_DIR'" ./internal/capi2

run_test() {
  local name=$1 output
  output=$("$FIXTURE/probe" -test.v -test.run="^$name$" 2>&1) || {
    printf '%s\n' "$output" >&2
    return 1
  }
  printf '%s\n' "$output"
  if ! grep -q "^--- PASS: $name " <<< "$output"; then
    echo "capi2: expected $name to pass" >&2
    return 1
  fi
}

# Start the exact same executable against the matching library first, then
# substitute a checksummed engine whose parse_sql ABI is incompatible.
LD_LIBRARY_PATH="$MATCHING_DIR" LD_BIND_NOW=1 \
  run_test TestPersistenceAndConnectionRelease
LD_LIBRARY_PATH="$FIXTURE" LD_BIND_NOW=1 CAPI2_MISMATCH_TEST=1 \
  run_test TestRejectMismatchedLibrary
