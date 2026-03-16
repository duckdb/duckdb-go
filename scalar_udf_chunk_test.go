package duckdb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockChunk is a mock implementation of chunk for testing.
type mockChunk struct {
	rows     [][]any
	results  map[int]any
	errOnGet error
	errOnSet error
}

func newMockChunk(rows [][]any) *mockChunk {
	return &mockChunk{
		rows:    rows,
		results: make(map[int]any),
	}
}

func newMockChunkWriter() *mockChunk {
	return &mockChunk{results: make(map[int]any)}
}

func (m *mockChunk) GetSize() int {
	return len(m.rows)
}

func (m *mockChunk) ColumnCount() int {
	if len(m.rows) == 0 {
		return 0
	}
	return len(m.rows[0])
}

func (m *mockChunk) GetValue(colIdx, rowIdx int) (any, error) {
	if m.errOnGet != nil {
		return nil, m.errOnGet
	}
	if rowIdx < 0 || rowIdx >= len(m.rows) {
		return nil, errors.New("row index out of bounds")
	}
	if colIdx < 0 || colIdx >= len(m.rows[rowIdx]) {
		return nil, errors.New("column index out of bounds")
	}
	return m.rows[rowIdx][colIdx], nil
}

func (m *mockChunk) SetValue(colIdx, rowIdx int, val any) error {
	if m.errOnSet != nil {
		return m.errOnSet
	}
	if m.results == nil {
		m.results = make(map[int]any)
	}
	m.results[rowIdx] = val
	return nil
}

func TestScalarUDFChunk_RowCount(t *testing.T) {
	tests := []struct {
		name     string
		rows     [][]any
		expected int
	}{
		{"empty", [][]any{}, 0},
		{"single row", [][]any{{1, 2}}, 1},
		{"multiple rows", [][]any{{1, 2}, {3, 4}, {5, 6}}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := &ScalarUDFChunk{
				input:  newMockChunk(tt.rows),
				output: newMockChunkWriter(),
			}
			require.Equal(t, tt.expected, chunk.RowCount())
		})
	}
}

func TestScalarUDFChunk_ColumnCount(t *testing.T) {
	tests := []struct {
		name     string
		rows     [][]any
		expected int
	}{
		{"empty", [][]any{}, 0},
		{"single column", [][]any{{1}}, 1},
		{"two columns", [][]any{{1, 2}}, 2},
		{"three columns", [][]any{{1, 2, 3}}, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunk := &ScalarUDFChunk{
				input:  newMockChunk(tt.rows),
				output: newMockChunkWriter(),
			}
			require.Equal(t, tt.expected, chunk.ColumnCount())
		})
	}
}

func TestScalarUDFChunk_Rows_Basic(t *testing.T) {
	input := newMockChunk([][]any{
		{int32(1), int32(10)},
		{int32(2), int32(20)},
		{int32(3), int32(30)},
	})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	rows, onFinish := chunk.Rows()
	count := 0
	for row := range rows {
		require.Equal(t, count, row.Index())
		require.Len(t, row.Args, 2)
		require.Equal(t, input.rows[count][0], row.Args[0])
		require.Equal(t, input.rows[count][1], row.Args[1])
		count++
	}
	require.NoError(t, onFinish())
	require.Equal(t, 3, count)
}

func TestScalarUDFChunk_Rows_EarlyBreak(t *testing.T) {
	input := newMockChunk([][]any{
		{int32(1)},
		{int32(2)},
		{int32(3)},
	})

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	rows, onFinish := chunk.Rows()
	count := 0
	for range rows {
		count++
		if count == 2 {
			break
		}
	}
	require.NoError(t, onFinish())
	require.Equal(t, 2, count)
}

func TestScalarUDFChunk_Rows_NullInNullOut_SkipsNullRows(t *testing.T) {
	input := newMockChunk([][]any{
		{int32(1), int32(10)}, // no nulls - should be yielded
		{nil, int32(20)},     // has null - should be skipped
		{int32(3), nil},      // has null - should be skipped
		{int32(4), int32(40)}, // no nulls - should be yielded
	})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	rows, onFinish := chunk.Rows()
	yieldedIndices := []int{}
	for row := range rows {
		yieldedIndices = append(yieldedIndices, row.Index())
		for _, arg := range row.Args {
			require.NotNil(t, arg)
		}
	}
	require.NoError(t, onFinish())
	require.Equal(t, []int{0, 3}, yieldedIndices)

	// Verify NULL was set for skipped rows
	require.Nil(t, output.results[1])
	require.Nil(t, output.results[2])
}

func TestScalarUDFChunk_Rows_NullInNullOut_Disabled(t *testing.T) {
	input := newMockChunk([][]any{
		{int32(1), int32(10)},
		{nil, int32(20)},    // has null but should still be yielded
		{int32(3), nil},     // has null but should still be yielded
		{int32(4), int32(40)},
	})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: false, // disabled
	}

	rows, onFinish := chunk.Rows()
	count := 0
	for range rows {
		count++
	}
	require.NoError(t, onFinish())
	require.Equal(t, 4, count)
}

func TestScalarUDFChunk_Rows_NullInNullOut_SetValueErrorStopsIteration(t *testing.T) {
	// When nullInNullOut is true, NULL rows are skipped and we call output.SetValue(0, rowIdx, nil).
	// If that SetValue fails, we stop iterating and surface the error from onFinish().
	setErr := errors.New("set null error")
	input := newMockChunk([][]any{
		{nil, int32(20)}, // has null - we try to set result to NULL, then SetValue fails
		{int32(2), int32(20)},
	})
	output := &mockChunk{
		results:  make(map[int]any),
		errOnSet: setErr,
	}

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	rows, onFinish := chunk.Rows()
	count := 0
	for range rows {
		count++
	}
	// First row has NULL; we try to SetValue(0, 0, nil), it fails, we stop before yielding any row
	require.Equal(t, 0, count)
	require.ErrorIs(t, onFinish(), setErr)
}

func TestScalarUDFRow_SetResult(t *testing.T) {
	input := newMockChunk([][]any{{1}, {2}, {3}})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	rows, onFinish := chunk.Rows()
	for row := range rows {
		err := row.SetResult(row.Index() * 10)
		require.NoError(t, err)
	}
	require.NoError(t, onFinish())

	require.Equal(t, 0, output.results[0])
	require.Equal(t, 10, output.results[1])
	require.Equal(t, 20, output.results[2])
}

func TestScalarUDFRow_SetResultError(t *testing.T) {
	expectedErr := errors.New("write error")
	input := newMockChunk([][]any{{1}})
	output := &mockChunk{
		results:  make(map[int]any),
		errOnSet: expectedErr,
	}

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	rows, onFinish := chunk.Rows()
	for row := range rows {
		err := row.SetResult(42)
		require.ErrorIs(t, err, expectedErr)
	}
	require.NoError(t, onFinish())
}

func TestScalarUDFChunk_Rows_OnFinishReturnsIterationError(t *testing.T) {
	getErr := errors.New("get value error")
	input := &mockChunk{
		rows:     [][]any{{1}, {2}, {3}},
		errOnGet: getErr,
	}
	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	rows, onFinish := chunk.Rows()
	count := 0
	for range rows {
		count++
	}
	// GetValue fails on first row, so no rows are yielded
	require.Equal(t, 0, count)
	require.ErrorIs(t, onFinish(), getErr)
}

func TestScalarUDFRow_Index(t *testing.T) {
	input := newMockChunk([][]any{{1}, {2}, {3}, {4}, {5}})

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	rows, onFinish := chunk.Rows()
	var indices []int
	for row := range rows {
		indices = append(indices, row.Index())
	}
	require.NoError(t, onFinish())
	require.Equal(t, []int{0, 1, 2, 3, 4}, indices)
}

func TestScalarUDFChunk_EmptyChunk(t *testing.T) {
	input := newMockChunk([][]any{})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:  input,
		output: output,
	}

	require.Equal(t, 0, chunk.RowCount())
	require.Equal(t, 0, chunk.ColumnCount())

	rows, onFinish := chunk.Rows()
	count := 0
	for range rows {
		count++
	}
	require.NoError(t, onFinish())
	require.Equal(t, 0, count)
}

func TestScalarUDFChunk_Integration(t *testing.T) {
	// Simulate a typical UDF execution pattern
	input := newMockChunk([][]any{
		{int32(1), int32(10)},
		{int32(2), int32(20)},
		{nil, int32(30)}, // null in first column - should be skipped
		{int32(4), int32(40)},
	})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	rows, onFinish := chunk.Rows()
	for row := range rows {
		result := row.Args[0].(int32) + row.Args[1].(int32)
		err := row.SetResult(result)
		require.NoError(t, err)
	}
	require.NoError(t, onFinish())

	// Verify results
	require.Equal(t, int32(11), output.results[0])
	require.Equal(t, int32(22), output.results[1])
	require.Nil(t, output.results[2]) // null-in-null-out
	require.Equal(t, int32(44), output.results[3])
}

func TestScalarUDFChunk_Args_TypePreservation(t *testing.T) {
	input := newMockChunk([][]any{
		{int32(1), "hello", 3.14, true},
	})

	chunk := &ScalarUDFChunk{
		input:  input,
		output: newMockChunkWriter(),
	}

	rows, onFinish := chunk.Rows()
	for row := range rows {
		require.Equal(t, int32(1), row.Args[0])
		require.Equal(t, "hello", row.Args[1])
		require.Equal(t, 3.14, row.Args[2])
		require.Equal(t, true, row.Args[3])
	}
	require.NoError(t, onFinish())
}

func TestScalarUDFChunk_NullInNullOut_AllNullRow(t *testing.T) {
	input := newMockChunk([][]any{
		{nil, nil}, // all nulls
	})
	output := newMockChunkWriter()

	chunk := &ScalarUDFChunk{
		input:         input,
		output:        output,
		nullInNullOut: true,
	}

	rows, onFinish := chunk.Rows()
	count := 0
	for range rows {
		count++
	}
	require.NoError(t, onFinish())
	require.Equal(t, 0, count)        // Row should be skipped
	require.Nil(t, output.results[0]) // Result should be NULL
}
