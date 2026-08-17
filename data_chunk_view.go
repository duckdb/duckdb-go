package duckdb

// DataChunkView gives read-only access to a DuckDB data chunk. It is valid as
// long as the underlying data chunk is valid. The zero value is invalid.
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

// GetSize returns the logical row count of the data chunk, or an error if the
// view is invalid.
func (view DataChunkView) GetSize() (int, error) {
	dataChunk, err := view.getDataChunk()
	if err != nil {
		return 0, err
	}
	return dataChunk.GetSize(), nil
}

// ColumnCount returns the number of columns in the data chunk, or an error if
// the view is invalid.
func (view DataChunkView) ColumnCount() (int, error) {
	dataChunk, err := view.getDataChunk()
	if err != nil {
		return 0, err
	}
	return dataChunk.ColumnCount(), nil
}

// GetValue returns one value from the data chunk, or an error if the view is
// invalid.
func (view DataChunkView) GetValue(column, row int) (any, error) {
	dataChunk, err := view.getDataChunk()
	if err != nil {
		return nil, err
	}
	return dataChunk.GetValue(column, row)
}

func (view DataChunkView) getDataChunk() (*DataChunk, error) {
	if view.dataChunk == nil {
		return nil, getError(errAPI, errNilDataChunk)
	}
	return view.dataChunk, nil
}
