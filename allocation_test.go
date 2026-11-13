//go:build debug_bindings

package duckdb

import (
	"testing"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func TestMain(m *testing.M) {
	m.Run()

	// The singleton cache normally lives until process exit. Release it after all
	// tests and their cleanups have finished so every allocation counter must balance.
	cache := GetInstanceCache()
	mapping.DestroyInstanceCache(&cache)
	mapping.VerifyAllocationCounters()
}
