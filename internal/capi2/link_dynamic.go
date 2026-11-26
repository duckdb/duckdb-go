//go:build duckdb_use_lib

package capi2

/*
#cgo LDFLAGS: -lduckdb
*/
import "C"

// Shared DuckDB libraries register their own extensions and do not export the
// plural static loader symbol.
func registerStaticExtensions() error { return nil }
