package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataChunkGetVector(t *testing.T) {
	chunk := newVectorViewTestChunk(
		t,
		mustTypeInfo(t, TYPE_INTEGER),
		mustTypeInfo(t, TYPE_VARCHAR),
	)
	require.NoError(t, chunk.SetSize(3))

	vec, err := chunk.GetVector(1)
	require.NoError(t, err)
	require.Same(t, &chunk.columns[1], vec.v)
	require.Equal(t, 3, vec.logicalCount)

	chunk.projection = []int{1}
	projected, err := chunk.GetVector(0)
	require.NoError(t, err)
	require.Same(t, &chunk.columns[1], projected.v)
	require.Equal(t, 3, projected.logicalCount)
}

func TestDataChunkGetVectorValidation(t *testing.T) {
	chunk := newVectorViewTestChunk(t, mustTypeInfo(t, TYPE_VARCHAR))

	_, err := chunk.GetVector(-1)
	require.ErrorIs(t, err, errAPI)
	_, err = chunk.GetVector(1)
	require.ErrorIs(t, err, errAPI)

	var nilChunk *DataChunk
	_, err = nilChunk.GetVector(0)
	require.ErrorIs(t, err, errAPI)
	require.ErrorIs(t, err, errNilDataChunk)
}

func TestChunkIteratorStateVectorAccess(t *testing.T) {
	state, input, output := newVectorWriterTestState(
		t,
		mustTypeInfo(t, TYPE_VARCHAR),
		2,
		false,
	)

	require.Same(t, input, state.GetInputChunk())
	result := state.GetResultVector()
	require.Same(t, &output.columns[0], result.v)
	require.Equal(t, 2, result.logicalCount)

	var nilState *ChunkIteratorState
	require.Nil(t, nilState.GetInputChunk())
	require.Nil(t, nilState.GetResultVector().v)

	emptyState := &ChunkIteratorState{}
	require.Nil(t, emptyState.GetInputChunk())
	require.Nil(t, emptyState.GetResultVector().v)

	inputOnlyState := &ChunkIteratorState{input: input}
	require.Nil(t, inputOnlyState.GetResultVector().v)

	outputOnlyState := &ChunkIteratorState{output: &output.columns[0]}
	require.Nil(t, outputOnlyState.GetResultVector().v)
}
