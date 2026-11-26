// Package capi2 is the pinned, experimental native bootstrap, not a driver.
// Call Bootstrap before NewEnvironment. Handles are owned, must not be copied,
// and must be closed explicitly in dependency order. V1 handles are unrelated.
package capi2

/*
#cgo CPPFLAGS: -DDUCKDB_V2_API_ALLOW_UNSTABLE=1
#include <stdlib.h>
#include "shims.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
	"unsafe"

	_ "github.com/duckdb/duckdb-go/v2/internal/capi2/include"
)

type Code uint32

const (
	ResourceInUse Code = C.DUCKDB_V2_ERROR_RESOURCE_IN_USE
	IOGeneral     Code = C.DUCKDB_V2_ERROR_IO_GENERAL
)

// Error copies the native message before releasing the caller-owned error slot.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("capi2: %s (%d)", e.Message, e.Code) }

func check(code C.DUCKDB_V2_ERROR, info C.duckdb_v2_error_info_handle, operation string) error {
	var err error
	if code != C.DUCKDB_V2_ERROR_NONE {
		native := &Error{Code: Code(code), Message: operation}
		if info != nil {
			var text C.duckdb_v2_str
			if status := C.capi2_error_text(info, &text); status != C.DUCKDB_V2_ERROR_NONE {
				err = &Error{Code: Code(status), Message: "reading error text"}
			} else {
				native.Message = string(unsafe.Slice((*byte)(unsafe.Pointer(text.ptr)), int(text.len)))
			}
		}
		err = errors.Join(native, err)
	}
	if info != nil {
		if status := C.capi2_error_destroy(&info); status != C.DUCKDB_V2_ERROR_NONE {
			err = errors.Join(err, &Error{Code: Code(status), Message: "destroying error info"})
		}
	}
	return err
}

type (
	Environment struct {
		h C.duckdb_v2_environment_handle
	}
	Instance   struct{ h C.duckdb_v2_instance_handle }
	Connection struct{ h C.duckdb_v2_connection_handle }
	Statements struct {
		h C.duckdb_v2_statement_iterator_handle
	}
	Statement struct {
		h C.duckdb_v2_sql_statement_handle
	}
	Result struct{ h C.duckdb_v2_result_handle }
	Chunk  struct{ h C.duckdb_v2_data_chunk_handle }
)

func NewEnvironment() (*Environment, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	env := &Environment{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_environment_create(&env.h, &info)
	if err := check(code, info, "creating environment"); err != nil {
		return nil, err
	}
	return env, nil
}

func (e *Environment) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_environment_destroy(&e.h), nil, "destroying environment")
}

// NewInstance starts with autoload and autoinstall disabled. Static extension
// availability must come from Bootstrap, not the network or a local cache.
func (e *Environment) NewInstance() (*Instance, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	instance := &Instance{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_instance_create(e.h, &instance.h, &info)
	if err := check(code, info, "creating instance"); err != nil {
		return nil, err
	}
	return instance, nil
}

// Attach makes the database the default for subsequently created connections.
func (i *Instance) Attach(path string) error {
	if err := checkReady(); err != nil {
		return err
	}
	data := C.CString(path)
	defer C.free(unsafe.Pointer(data))
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_instance_attach(i.h, data, C.idx_t(len(path)), &info)
	return check(code, info, "attaching database")
}

func (i *Instance) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_instance_destroy(&i.h), nil, "destroying instance")
}

func (i *Instance) Connect() (*Connection, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	conn := &Connection{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_connection_create(i.h, &conn.h, &info)
	if err := check(code, info, "creating connection"); err != nil {
		return nil, err
	}
	return conn, nil
}

func (c *Connection) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_connection_destroy(&c.h), nil, "destroying connection")
}

func (c *Connection) Parse(sql string) (*Statements, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	if strings.IndexByte(sql, 0) >= 0 {
		return nil, errors.New("capi2: SQL contains NUL")
	}
	data := C.CString(sql)
	defer C.free(unsafe.Pointer(data))
	iterator := &Statements{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_parse(c.h, data, &iterator.h, &info)
	if err := check(code, info, "parsing SQL"); err != nil {
		return nil, err
	}
	return iterator, nil
}

// Next returns nil at end of input. Each statement is independently owned.
func (s *Statements) Next() (*Statement, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	statement := &Statement{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_iterator_next(s.h, &statement.h, &info)
	if err := check(code, info, "reading next statement"); err != nil {
		return nil, err
	}
	if statement.h == nil {
		return nil, nil
	}
	return statement, nil
}

func (s *Statements) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_iterator_destroy(&s.h), nil, "destroying statement iterator")
}

func (s *Statement) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_statement_destroy(&s.h), nil, "destroying statement")
}

// Execute borrows the statement. Execution can fail later during Drain or Fetch.
func (c *Connection) Execute(statement *Statement) (*Result, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	result := &Result{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_execute(c.h, statement.h, &result.h, &info)
	if err := check(code, info, "executing statement"); err != nil {
		return nil, err
	}
	return result, nil
}

// Close aborts unfinished execution. Call Drain to complete side effects first.
func (r *Result) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_result_destroy(&r.h), nil, "destroying result")
}

func (r *Result) Drain() (uint64, error) {
	if err := checkReady(); err != nil {
		return 0, err
	}
	var changed C.idx_t
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_drain(r.h, &changed, &info)
	if err := check(code, info, "draining result"); err != nil {
		return 0, err // changed is unspecified on failure
	}
	return uint64(changed), nil
}

// Fetch returns an owned chunk, or nil at end of stream.
func (r *Result) Fetch() (*Chunk, error) {
	if err := checkReady(); err != nil {
		return nil, err
	}
	chunk := &Chunk{}
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_fetch(r.h, &chunk.h, &info)
	if err := check(code, info, "fetching chunk"); err != nil {
		return nil, err
	}
	if chunk.h == nil {
		return nil, nil
	}
	return chunk, nil
}

func (c *Chunk) Close() error {
	if err := checkReady(); err != nil {
		return err
	}
	return check(C.capi2_chunk_destroy(&c.h), nil, "destroying chunk")
}

func (c *Chunk) Size() (uint64, error) {
	if err := checkReady(); err != nil {
		return 0, err
	}
	var size C.idx_t
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_chunk_size(c.h, &size, &info)
	if err := check(code, info, "reading chunk size"); err != nil {
		return 0, err
	}
	return uint64(size), nil
}

// Int64 is a single-cell correctness bridge, not a vectorized hot path.
func (c *Chunk) Int64(column, row uint64) (int64, error) {
	if err := checkReady(); err != nil {
		return 0, err
	}
	var vector C.duckdb_v2_vector_handle // borrowed from the chunk
	var info C.duckdb_v2_error_info_handle
	code := C.capi2_chunk_vector(c.h, C.idx_t(column), &vector, &info)
	if err := check(code, info, "reading chunk vector"); err != nil {
		return 0, err
	}
	var value C.duckdb_v2_value_handle
	info = nil
	code = C.capi2_vector_value(vector, C.idx_t(row), &value, &info)
	if err := check(code, info, "reading vector value"); err != nil {
		return 0, err
	}
	var out C.int64_t
	info = nil
	code = C.capi2_value_bigint(value, &out, &info)
	err := check(code, info, "reading BIGINT value")
	err = errors.Join(err, check(C.capi2_value_destroy(&value), nil, "destroying value"))
	if err != nil {
		return 0, err
	}
	return int64(out), nil
}
