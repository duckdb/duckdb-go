#!/usr/bin/env bash
# Vendor an external consumer of this checkout, then query offline with empty
# caches to catch missing headers and native libraries.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONSUMER="$(mktemp -d)"
trap 'rm -rf "$CONSUMER"' EXIT

cd "$CONSUMER"
export GOWORK=off
export CGO_ENABLED=1
go mod init vendor-test
go mod edit "-replace=github.com/duckdb/duckdb-go/v2=$REPO_ROOT"

cat > vendor_test.go <<'EOF'
package vendor_test

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestVendoredQuery(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var result int
	if err := db.QueryRow("SELECT 6 * 7").Scan(&result); err != nil {
		t.Fatal(err)
	}
	if result != 42 {
		t.Fatalf("got %d, want 42", result)
	}
}
EOF

go mod tidy
go mod vendor

# Do not let cached packages or downloads hide missing vendored files.
GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
  GOMODCACHE="$CONSUMER/mod-cache" GOCACHE="$CONSUMER/build-cache" \
  go test -mod=vendor -count=1 -v .
