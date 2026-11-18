#!/usr/bin/env bash
# Test against the pinned shared library, verifying its checksum before use.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
export CGO_ENABLED=1

PLATFORM="$(go env GOOS)-$(go env GOARCH)"
BINDINGS_DIR="$(go mod download -json github.com/duckdb/duckdb-go-bindings | jq -r .Dir)"
MANIFEST="$BINDINGS_DIR/duckdb-release.json"
BASE_URL="$(jq -r .base_url "$MANIFEST")"
ARTIFACT="$(jq -er --arg platform "$PLATFORM" '.platforms[$platform].shared' "$MANIFEST")"
SHA256="$(jq -er --arg platform "$PLATFORM" '.platforms[$platform].shared_sha256' "$MANIFEST")"

if command -v sha256sum >/dev/null; then
  CHECKSUM=(sha256sum)
else
  CHECKSUM=(shasum -a 256)
fi

verify() {
  printf '%s  %s\n' "$SHA256" "$1" | "${CHECKSUM[@]}" -c --strict -
}

LIB_DIR="$REPO_ROOT/dynamic-dir"
ARCHIVE="$LIB_DIR/libs.tar.gz"
mkdir -p "$LIB_DIR"
if ! verify "$ARCHIVE" >/dev/null 2>&1; then
  DOWNLOAD="$(mktemp "$LIB_DIR/libs.tar.gz.XXXXXX")"
  trap 'rm -f "$DOWNLOAD"' EXIT
  curl --retry 3 -fsSL -o "$DOWNLOAD" "$BASE_URL/duckdb-shared-libs-$ARTIFACT.tar.gz"
  verify "$DOWNLOAD"
  mv "$DOWNLOAD" "$ARCHIVE"
fi
tar -xzf "$ARCHIVE" -C "$LIB_DIR"

# Prefer the verified library over any inherited Linux library search path.
LD_LIBRARY_PATH="$LIB_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  go test -tags=duckdb_use_lib \
    -ldflags="-extldflags '-L$LIB_DIR -Wl,-rpath,$LIB_DIR'" "$@"
