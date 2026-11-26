//go:build !duckdb_use_lib

package capi2

/*
#cgo CPPFLAGS: -DDUCKDB_STATIC_BUILD
#include <stdint.h>
int32_t duckdb_register_static_extensions(void);
*/
import "C"

import "fmt"

func registerStaticExtensions() error {
	if code := C.duckdb_register_static_extensions(); code != 0 {
		return fmt.Errorf("capi2: static extension registration failed (%d)", code)
	}
	return nil
}
