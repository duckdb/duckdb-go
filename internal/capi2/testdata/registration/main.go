// This isolated process intentionally poisons the static extension registry.
package main

/*
#define DUCKDB_STATIC_BUILD
#include <duckdb_static_extension.h>

static void unused_entry(void) {}
static int32_t describe_a(duckdb_extension_descriptor *d) {
	d->name = "capi2_registration_control";
	d->api_version = "v2.0.1";
	d->entry_cpp = unused_entry;
	return 0;
}
static int32_t describe_b(duckdb_extension_descriptor *d) {
	return describe_a(d);
}
static int capi2_registration_control(void) {
	if (duckdb_register_static_extension(describe_a) != 0) return 1;
	if (duckdb_register_static_extension(describe_a) != 0) return 2;
	if (duckdb_register_static_extension(describe_b) == 0) return 3;
	return 0;
}
*/
import "C"

import (
	"fmt"
	"strings"

	"github.com/duckdb/duckdb-go/v2/internal/capi2"
)

func main() {
	if err := capi2.Bootstrap(); err != nil {
		panic(err)
	}
	if code := C.capi2_registration_control(); code != 0 {
		panic(fmt.Sprintf("static registration control failed: %d", code))
	}
	env, err := capi2.NewEnvironment()
	if err != nil {
		panic(err)
	}
	instance, err := env.NewInstance()
	if err == nil || !strings.Contains(err.Error(), "two different describe functions") {
		panic(fmt.Sprintf("expected conflicting registration error, got %v", err))
	}
	if instance != nil {
		panic("instance creation returned a handle on failure")
	}
	if err := env.Close(); err != nil {
		panic(err)
	}
	fmt.Println("capi2: identical registration succeeded; conflicting registration rejected instance creation")
}
