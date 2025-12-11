#!/usr/bin/env bash

# Helper script to build a Go program against a locally built DuckDB (debug),
# with symbols and proper rpath so you can debug into DuckDB C/C++ sources.
#
# Disclaimer:
# - This script is intended for development and debugging purposes only.
# - We don't guarantee on maintaining this script in future releases.
#
# Why this script?
# - CGO relies on environment variables (CGO_CFLAGS/CGO_LDFLAGS). If they are not
#   exported to the environment, "go build" will not see them, leading to
#   linker errors such as: "ld: library 'duckdb' not found".
# - This script exports the correct flags and runs go build with debug options.
#
# Requirements:
# 1) Build DuckDB in Debug configuration from DuckDB source code:
#      ```bash
#      make debug
#      ```
#    After this, libduckdb (or libduckdb.dylib on macOS) should be in build/debug/src
# 2) Have a Go application that uses github.com/duckdb/duckdb-go with build tag duckdb_use_lib.
#
# Usage:
#   scripts/go-build-duckdb-debug.sh /path/to/go/app [package_or_dir] [output_name]
#
# Examples:
#   scripts/go-build-duckdb-debug.sh ~/Development/example-duckdb-go-project ./examples/simple simple_debug
#   scripts/go-build-duckdb-debug.sh ~/Development/example-duckdb-go-project ./ cmd_debug
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 /absolute/path/to/go/app [package_or_dir] [output_name]" >&2
  exit 2
fi

GO_APP_DIR="$1"               # absolute path to the Go module root or the app dir
GO_PKG_OR_DIR="${2:-.}"       # package path or relative directory to build from inside GO_APP_DIR
OUTPUT_NAME="${3:-duckdb_go_debug}"

# DuckDB source dir (this repo). Allow override via DUCKDB_SRC.
DUCKDB_SRC="${DUCKDB_SRC:-$REPO_ROOT}"
# If DUCKDB_SRC is not a valid directory (e.g., user passed a malformed value with spaces/globs),
# fall back to the repository root to avoid surprising failures.
if [[ ! -d "$DUCKDB_SRC" ]]; then
  echo "Warning: DUCKDB_SRC ('$DUCKDB_SRC') is not a directory. Falling back to $REPO_ROOT" >&2
  DUCKDB_SRC="$REPO_ROOT"
fi
INCLUDE_DIR="$DUCKDB_SRC/src/include"
LIB_DIR="$DUCKDB_SRC/build/debug/src"

if [[ ! -d "$INCLUDE_DIR" ]]; then
  echo "ERROR: DuckDB include dir not found: $INCLUDE_DIR" >&2
  echo "Make sure DUCKDB_SRC points to this DuckDB repo root." >&2
  exit 1
fi

if [[ ! -d "$LIB_DIR" ]]; then
  echo "ERROR: DuckDB debug build lib dir not found: $LIB_DIR" >&2
  echo "You likely need to build DuckDB in Debug mode first, e.g.:" >&2
  echo "  mkdir -p $DUCKDB_SRC/build/debug && cd $DUCKDB_SRC/build/debug" >&2
  echo "  cmake -DCMAKE_BUILD_TYPE=Debug ../.. && make -j" >&2
  exit 1
fi

# Provide a friendly hint if the library file is missing from the expected dir
if [[ ! -e "$LIB_DIR/libduckdb.dylib" && ! -e "$LIB_DIR/libduckdb.so" && ! -e "$LIB_DIR/libduckdb.a" ]]; then
  echo "Warning: Could not find libduckdb.{dylib,so,a} in $LIB_DIR" >&2
  echo "         Ensure the debug build succeeded and produced the DuckDB library." >&2
fi

if [[ ! -d "$GO_APP_DIR" ]]; then
  echo "ERROR: Go app directory does not exist: $GO_APP_DIR" >&2
  exit 1
fi

# Compose CGO flags and export them so Go toolchain sees them.
export CGO_ENABLED=1
export CGO_CFLAGS="-I${INCLUDE_DIR}"

export CGO_LDFLAGS="-L${LIB_DIR} -lduckdb -Wl,-rpath,${LIB_DIR}"

echo "Using settings:"
echo "  DUCKDB_SRC  = $DUCKDB_SRC"
echo "  INCLUDE_DIR = $INCLUDE_DIR"
echo "  LIB_DIR     = $LIB_DIR"
echo "  GO_APP_DIR  = $GO_APP_DIR"
echo "  GO_PKG/DIR  = $GO_PKG_OR_DIR"
echo "  OUTPUT_NAME = $OUTPUT_NAME"
echo "  CGO_CFLAGS  = $CGO_CFLAGS"
echo "  CGO_LDFLAGS = $CGO_LDFLAGS"

pushd "$GO_APP_DIR" >/dev/null

# Ensure Go toolchain is available
if ! command -v go >/dev/null 2>&1; then
  echo "ERROR: 'go' binary not found in PATH." >&2
  exit 1
fi

# Validate the package/dir argument. It should point inside the Go app dir.
PKG_DIR_CANDIDATE="$GO_PKG_OR_DIR"

# Reject clearly unsafe upward references which will likely fail
if [[ "$PKG_DIR_CANDIDATE" == ..* || "$PKG_DIR_CANDIDATE" == */..* ]]; then
  echo "Warning: The provided package/dir ('$PKG_DIR_CANDIDATE') points outside the Go app directory; adjusting..." >&2
  PKG_DIR_CANDIDATE="."
fi

# If the candidate does not exist locally, try common defaults
if [[ ! -d "$PKG_DIR_CANDIDATE" && ! -f "$PKG_DIR_CANDIDATE" ]]; then
  if [[ -d "./examples/simple" ]]; then
    echo "Info: '$GO_PKG_OR_DIR' not found. Falling back to ./examples/simple" >&2
    PKG_DIR_CANDIDATE="./examples/simple"
  else
    echo "Info: '$GO_PKG_OR_DIR' not found. Falling back to current directory (.)" >&2
    PKG_DIR_CANDIDATE="."
  fi
fi

echo "Building package/dir: $PKG_DIR_CANDIDATE"

# Build with the static tag duckdb_use_lib to link against external libduckdb
set -x
go build -tags "duckdb_use_lib" -gcflags "all=-N -l" -o "$OUTPUT_NAME" "$PKG_DIR_CANDIDATE"
set +x

echo
echo "Build completed: $(pwd)/$OUTPUT_NAME"
echo "You can run it directly. The rpath should allow it to find libduckdb at runtime."
echo "If it fails to locate libduckdb at runtime, you can export:"
if [[ "$(uname)" == "Darwin" ]]; then
  echo "  export DYLD_LIBRARY_PATH=$LIB_DIR:\${DYLD_LIBRARY_PATH:-}"
else
  echo "  export LD_LIBRARY_PATH=$LIB_DIR:\${LD_LIBRARY_PATH:-}"
fi

popd >/dev/null
