package duckdb

import "github.com/duckdb/duckdb-go/v2/mapping"

// AppendBytes marks a []byte column value for zero-copy Appender insert.
// The slice must remain valid until Appender.Flush (or the current chunk is committed).
// Plain []byte in AppendRow still uses the copying VectorAssignStringElementLen path.
type AppendBytes []byte

// AppendBytesUnsafe skips UTF-8 validation (caller must provide valid UTF-8 JSON/text).
type AppendBytesUnsafe []byte

// UTF8Bytes is an alias for the bindings UTF8Bytes marker type.
type UTF8Bytes = mapping.UTF8Bytes

// UnsafeUTF8Bytes is an alias for the bindings UnsafeUTF8Bytes marker type.
type UnsafeUTF8Bytes = mapping.UnsafeUTF8Bytes
