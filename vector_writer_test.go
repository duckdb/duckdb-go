package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/duckdb/duckdb-go/v2/mapping"
)

func newVectorWriterTestState(
	t testing.TB,
	outputInfo TypeInfo,
	size int,
	nullInNullOut bool,
) (*ChunkIteratorState, *DataChunk, *DataChunk) {
	t.Helper()

	input := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_INTEGER))
	output := newVectorViewTestChunk(t, outputInfo)
	require.NoError(t, input.SetSize(size))
	require.NoError(t, output.SetSize(size))

	state := &ChunkIteratorState{
		input:         input,
		output:        &output.columns[0],
		nullInNullOut: nullInNullOut,
	}
	return state, input, output
}

func TestVarcharVectorWriter(t *testing.T) {
	values := []string{
		"",
		"twelve-bytes",
		"a VARCHAR value longer than DuckDB's inline storage",
		"embedded\x00nul",
	}
	state, _, output := newVectorWriterTestState(
		t,
		mustTypeInfo(t, TYPE_VARCHAR),
		len(values)+1,
		false,
	)
	writer, err := GetVectorWriter[string](state.GetResultVector())
	require.NoError(t, err)

	for row, value := range values {
		require.NoError(t, writer.Set(row, value))
	}
	require.NoError(t, writer.SetNull(len(values)))

	for row, expected := range values {
		actual, getErr := output.GetValue(0, row)
		require.NoError(t, getErr)
		require.Equal(t, expected, actual)
	}
	actual, err := output.GetValue(0, len(values))
	require.NoError(t, err)
	require.Nil(t, actual)

	require.NoError(t, writer.SetNull(2))
	require.NoError(t, writer.Set(2, "restored with a long VARCHAR value"))
	actual, err = output.GetValue(0, 2)
	require.NoError(t, err)
	require.Equal(t, "restored with a long VARCHAR value", actual)

	namedWriter, err := GetVectorWriter[namedVarchar](state.GetResultVector())
	require.NoError(t, err)
	require.NoError(t, namedWriter.Set(1, namedVarchar("named string type")))
	actual, err = output.GetValue(0, 1)
	require.NoError(t, err)
	require.Equal(t, "named string type", actual)
}

func TestVarcharVectorWriterPreservesInvalidUTF8AsNull(t *testing.T) {
	state, _, output := newVectorWriterTestState(
		t,
		mustTypeInfo(t, TYPE_VARCHAR),
		1,
		false,
	)
	writer, err := GetVectorWriter[string](state.GetResultVector())
	require.NoError(t, err)

	require.NoError(t, writer.Set(0, string([]byte{0xff})))
	actual, err := output.GetValue(0, 0)
	require.NoError(t, err)
	require.Nil(t, actual)

	require.NoError(t, writer.Set(0, "valid UTF-8"))
	actual, err = output.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, "valid UTF-8", actual)
}

func TestVectorWriterDefaultNullHandling(t *testing.T) {
	state, input, output := newVectorWriterTestState(
		t,
		mustTypeInfo(t, TYPE_VARCHAR),
		2,
		true,
	)
	require.NoError(t, input.SetValue(0, 0, int32(1)))
	require.NoError(t, input.SetValue(0, 1, nil))

	writer, err := GetVectorWriter[string](state.GetResultVector())
	require.NoError(t, err)
	require.NoError(t, writer.Set(0, "first"))
	require.NoError(t, writer.Set(1, "written before default NULL handling"))

	applyDefaultNullHandling(input, output)

	first, err := output.GetValue(0, 0)
	require.NoError(t, err)
	require.Equal(t, "first", first)
	second, err := output.GetValue(0, 1)
	require.NoError(t, err)
	require.Nil(t, second)
}

func TestVectorWriterRejectsReadOnlyVector(t *testing.T) {
	source := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_VARCHAR))
	require.NoError(t, source.SetSize(1))

	var readOnly DataChunk
	require.NoError(t, readOnly.initFromDuckDataChunk(source.chunk, false))
	_, err := GetVectorWriter[string](mustGetVector(t, &readOnly, 0))
	require.ErrorIs(t, err, errAPI)
	require.ErrorIs(t, err, errVectorNotWritable)
}

func TestVectorWriterValidation(t *testing.T) {
	var zero VectorWriter[string]
	require.ErrorIs(t, zero.Set(0, "x"), errUninitializedVectorWriter)
	require.ErrorIs(t, zero.SetNull(0), errUninitializedVectorWriter)

	_, err := GetVectorWriter[string](Vector{})
	require.ErrorIs(t, err, errAPI)
	require.ErrorIs(t, err, errUninitializedVectorWriter)

	integerState, _, _ := newVectorWriterTestState(t, mustTypeInfo(t, TYPE_INTEGER), 1, false)
	_, err = GetVectorWriter[string](integerState.GetResultVector())
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be written as Go string")
	_, err = GetVectorWriter[uint32](integerState.GetResultVector())
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB INTEGER cannot be written as Go uint32")

	state, _, _ := newVectorWriterTestState(t, mustTypeInfo(t, TYPE_VARCHAR), 1, false)
	writer, err := GetVectorWriter[string](state.GetResultVector())
	require.NoError(t, err)
	require.ErrorContains(t, writer.Set(-1, "x"), rowIndexErrMsg)
	require.ErrorContains(t, writer.Set(writer.Len(), "x"), rowIndexErrMsg)

	decimalInfo, err := NewDecimalInfo(9, 2)
	require.NoError(t, err)
	decimalState, _, _ := newVectorWriterTestState(t, decimalInfo, 1, false)
	_, err = GetVectorWriter[int32](decimalState.GetResultVector())
	require.ErrorIs(t, err, errAPI)
	require.ErrorContains(t, err, "DuckDB DECIMAL cannot be written as Go int32")
}

func TestVarcharVectorWriterRejectsNonVarcharTypes(t *testing.T) {
	state, _, _ := newVectorWriterTestState(t, mustTypeInfo(t, TYPE_BOOLEAN), 1, false)
	_, err := GetVectorWriter[string](state.GetResultVector())
	require.Error(t, err)
	require.ErrorContains(t, err, "DuckDB BOOLEAN cannot be written as Go string")

	jsonType := mapping.CreateLogicalType(TYPE_VARCHAR)
	mapping.LogicalTypeSetAlias(jsonType, aliasJSON)
	defer mapping.DestroyLogicalType(&jsonType)
	jsonOutput := &DataChunk{}
	require.NoError(t, jsonOutput.initFromTypes([]mapping.LogicalType{jsonType}, true))
	t.Cleanup(jsonOutput.close)
	require.NoError(t, jsonOutput.SetSize(1))
	input := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_INTEGER))
	require.NoError(t, input.SetSize(1))
	jsonState := &ChunkIteratorState{input: input, output: &jsonOutput.columns[0]}
	_, err = GetVectorWriter[string](jsonState.GetResultVector())
	require.Error(t, err)
	require.ErrorContains(t, err, "DuckDB JSON cannot be written as Go string")
}
