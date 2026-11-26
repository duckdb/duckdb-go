package capi2

/*
#include "include/duckdb_v2.h"
// This stable v1 bootstrap signature predates the unstable v2 surface. Do not
// use duckdb_v2_library_version to decide whether v2 calls are safe.
DUCKDB_C_API const char *duckdb_library_version(void);
*/
import "C"

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

//go:embed manifest.json
var manifest []byte

var (
	bootstrapOnce sync.Once
	errBootstrap  error
	bootstrapDone atomic.Bool
)

// Bootstrap checks the linked engine before any v2 call and registers embedded
// extensions. It is explicit, concurrent-safe, and caches failures as well as
// success. Importing capi2 does not call DuckDB.
func Bootstrap() error {
	bootstrapOnce.Do(func() {
		defer bootstrapDone.Store(true)
		var pin struct {
			Version string `json:"duckdb_version"` //nolint:tagliatelle // Matches native artifact metadata.
		}
		if err := json.Unmarshal(manifest, &pin); err != nil {
			errBootstrap = err
			return
		}
		if version := C.GoString(C.duckdb_library_version()); version != pin.Version {
			errBootstrap = fmt.Errorf("capi2: linked DuckDB %s, require %s", version, pin.Version)
			return
		}
		errBootstrap = registerStaticExtensions()
	})
	return errBootstrap
}

func checkReady() error {
	if !bootstrapDone.Load() {
		return errors.New("capi2: call Bootstrap before native access")
	}
	// The atomic flag publishes errBootstrap on failure as well as success.
	return errBootstrap
}
