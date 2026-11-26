//go:build !duckdb_use_lib && !duckdb_use_static_lib

package capi2

import _ "go.duckdb.org/libduckdb/darwin-amd64"
