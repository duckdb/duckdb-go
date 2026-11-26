#!/usr/bin/env bash
# Verify the selected artifact, not merely the version requested in go.mod.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
MODE=${1:?prebuilt, static, or dynamic}
PIN=internal/capi2/manifest.json
PLATFORM="$(go env GOOS)-$(go env GOARCH)"

if command -v sha256sum >/dev/null; then
  CHECKSUM=(sha256sum)
else
  CHECKSUM=(shasum -a 256)
fi

check_headers() {
  local dir=$1 header expected actual
  for header in duckdb_v2.h duckdb_extension_v2.h; do
    expected=$(jq -er --arg header "$header" '.headers[$header]' "$PIN")
    # Git checkouts on Windows can use CRLF. Compare normalized header bytes.
    actual=$(sed 's/\r$//' "$dir/$header" | "${CHECKSUM[@]}" | awk '{print $1}')
    if [[ $actual != "$expected" ]]; then
      echo "capi2: header mismatch: $dir/$header" >&2
      exit 1
    fi
  done
}

check_headers internal/capi2/include
case "$MODE" in
  prebuilt|static)
    NATIVE_DIR=${2:-$(go list -f '{{.Dir}}' "go.duckdb.org/libduckdb/$PLATFORM")}
    jq -e --slurpfile pin "$PIN" --arg platform "$PLATFORM" '
      .source == $pin[0].source and .duckdb_version == $pin[0].duckdb_version
      and .platform == $platform and (.sha256 | test("^[0-9a-f]{64}$"))
    ' "$NATIVE_DIR/provenance.json" >/dev/null
    echo "capi2: selected $MODE provenance matches $(jq -r .source "$PIN")"
    ;;
  dynamic)
    LIB_DIR=${2:?shared archive directory}
    SELECTED=${3:?selected release manifest}
    jq -e --slurpfile pin "$PIN" '
      .source == $pin[0].source and .duckdb_version == $pin[0].duckdb_version
    ' "$SELECTED" >/dev/null
    check_headers "$LIB_DIR"
    echo "capi2: checksummed shared archive headers match the pinned source"
    ;;
  *) echo "Unknown capi2 link mode: $MODE" >&2; exit 1 ;;
esac

# Compile every shim with the upstream function-pointer macros active. This
# checks extension-compatible dispatch, not a working loadable Go extension.
read -r -a COMPILER <<<"$(go env CC)"
"${COMPILER[@]}" -x c -fsyntax-only -Werror -DDUCKDB_V2_API_ALLOW_UNSTABLE=1 \
  -Iinternal/capi2 - <<'EOF'
#include "include/duckdb_extension_v2.h"
DUCKDB_EXTENSION_EXTERN
#include "shims.h"
EOF
echo "capi2: shims compile through the host API table"
