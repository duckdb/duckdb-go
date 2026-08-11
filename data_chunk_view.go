package duckdb

// DataChunkView gives read-only access to a DuckDB data chunk. It is valid as
// long as the underlying data chunk is valid.
type DataChunkView struct {
	dataChunk *DataChunk
}

func newDataChunkView(chunk *DataChunk) DataChunkView {
	return DataChunkView{dataChunk: chunk}
}

// View returns a read-only view of the data chunk.
func (chunk *DataChunk) View() DataChunkView {
	return newDataChunkView(chunk)
}

// GetSize returns the logical row count of the data chunk.
func (view DataChunkView) GetSize() int {
	if view.dataChunk == nil {
		return 0
	}
	return view.dataChunk.GetSize()
}

// ColumnCount returns the number of columns in the data chunk.
func (view DataChunkView) ColumnCount() int {
	if view.dataChunk == nil {
		return 0
	}
	return view.dataChunk.ColumnCount()
}

// GetValue returns one value from the data chunk.
func (view DataChunkView) GetValue(column, row int) (any, error) {
	if view.dataChunk == nil {
		return nil, getError(errAPI, errNilDataChunk)
	}
	return view.dataChunk.GetValue(column, row)
}
