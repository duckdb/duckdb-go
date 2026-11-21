#!/usr/bin/env bash
# Test against the pinned static archives and extension loader.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
export CGO_ENABLED=1

PLATFORM="$(go env GOOS)-$(go env GOARCH)"
# Windows ARM64 has no pre-built Go platform module: its MSVC archives require
# CRT settings for the entire cgo build, including runtime/cgo and Arrow.
# The bindings helper stages those archives and exports the build-wide settings.
if [[ $PLATFORM == windows-arm64 ]]; then
  BINDINGS_DIR="$(go mod download -json github.com/duckdb/duckdb-go-bindings | jq -r .Dir)"
  static_env=$(cd "$BINDINGS_DIR" && bash ./scripts/static-env.sh)
  eval "$static_env"
  go test -tags="${TAGS:+$TAGS,}duckdb_use_static_lib" "$@"
  exit
fi

NATIVE="go.duckdb.org/libduckdb/$PLATFORM"
NATIVE_DIR="$(go list -f '{{.Dir}}' "$NATIVE")"
LINK_FLAGS="$(go list -f '{{join .CgoLDFLAGS " "}}' "$NATIVE")"
LIB_DIR="$REPO_ROOT/static-dir"
rm -rf "$LIB_DIR"
mkdir -p "$LIB_DIR"

# Split the compiler command and flag lists into separate arguments.
read -r -a COMPILER <<<"$(go env CC)"
read -r -a COMPILER_FLAGS <<<"$(go env GOGCCFLAGS) $(go env CGO_CPPFLAGS) $(go env CGO_CFLAGS)"
"${COMPILER[@]}" "${COMPILER_FLAGS[@]}" \
  -c "$NATIVE_DIR/static_extension_loader.c" -o "$LIB_DIR/static_extension_loader.o"

go test -tags="${TAGS:+$TAGS,}duckdb_use_static_lib" \
  -ldflags="-extldflags '$LIB_DIR/static_extension_loader.o $LINK_FLAGS'" "$@"
