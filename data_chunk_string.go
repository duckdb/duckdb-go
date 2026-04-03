package duckdb

import (
	"unsafe"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

// StringRef points at a DuckDB VARCHAR value owned by the current chunk.
// The referenced memory is invalidated by the next NextChunk or Close call.
type StringRef struct {
	ptr unsafe.Pointer
	len int
}

type duckStringPointer struct {
	length uint32
	prefix [4]byte
	ptr    *byte
}

type duckStringInlined struct {
	length  uint32
	inlined [12]byte
}

// Len returns the string length in bytes.
func (ref StringRef) Len() int {
	return ref.len
}

// UnsafeString returns a zero-copy Go string view over the DuckDB string memory.
// The returned string is invalidated by the next NextChunk or Close call.
func (ref StringRef) UnsafeString() string {
	if ref.ptr == nil || ref.len == 0 {
		return ""
	}

	return unsafe.String((*byte)(ref.ptr), ref.len)
}

// StringRefs returns zero-copy views over a VARCHAR column.
// The returned refs are only valid until the chunk is invalidated.
// For NULL rows, use IsNull to check whether the corresponding element is valid.
func (chunk *DataChunk) StringRefs(colIdx int) ([]StringRef, error) {
	values, err := typedChunkSlice[mapping.StringT](chunk, colIdx, TYPE_VARCHAR)
	if err != nil {
		return nil, err
	}

	refs := make([]StringRef, len(values))
	for idx := range values {
		refs[idx] = newStringRef(&values[idx])
	}

	return refs, nil
}

func newStringRef(strT *mapping.StringT) StringRef {
	length := int(mapping.StringTLength(*strT))
	if length == 0 {
		return StringRef{}
	}

	if mapping.StringIsInlined(*strT) {
		inlined := (*duckStringInlined)(unsafe.Pointer(strT))
		return StringRef{
			ptr: unsafe.Pointer(&inlined.inlined[0]),
			len: length,
		}
	}

	pointer := (*duckStringPointer)(unsafe.Pointer(strT))
	return StringRef{
		ptr: unsafe.Pointer(pointer.ptr),
		len: length,
	}
}
