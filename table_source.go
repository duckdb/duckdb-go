package duckdb

import "context"

type (
	tableSource interface {
		// ColumnInfos returns column information for each column of the table function.
		ColumnInfos() []ColumnInfo
		// Cardinality returns the cardinality information of the table function.
		// Optionally, if no cardinality exists, it may return nil.
		Cardinality() *CardinalityInfo
	}

	parallelTableSource interface {
		tableSource
		// Init the table source.
		// Additionally, it returns information for the parallelism-aware table source.
		Init() ParallelTableSourceInfo
		// NewLocalState returns a thread-local execution state.
		// It must return a pointer or a reference type for correct state updates.
		// duckdb-go does not prevent non-reference values.
		NewLocalState() any
	}

	sequentialTableSource interface {
		tableSource
		// Init the table source.
		Init()
	}

	// A RowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the RowTableSource, duckdb-go requests the rows.
	// It sequentially calls the FillRow method with a single thread.
	RowTableSource interface {
		sequentialTableSource
		// FillRow takes a Row and fills it with values.
		// Returns true, if there are more rows to fill.
		FillRow(Row) (bool, error)
	}

	RowTableSourceContext interface {
		sequentialTableSource
		// FillRow takes a context and a Row and fills it with values.
		// Returns true, if there are more rows to fill.
		FillRow(context.Context, Row) (bool, error)
	}

	// A ParallelRowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelRowTableSource, duckdb-go requests the rows.
	// It simultaneously calls the FillRow method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillRow must use synchronisation
	// primitives to avoid race conditions.
	ParallelRowTableSource interface {
		parallelTableSource
		// FillRow takes a Row and fills it with values.
		// Returns true, if there are more rows to fill.
		FillRow(any, Row) (bool, error)
	}

	ParallelRowTableSourceContext interface {
		parallelTableSource
		// FillRow takes a context and a Row and fills it with values.
		// Returns true, if there are more rows to fill.
		FillRow(context.Context, any, Row) (bool, error)
	}

	// A ChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ChunkTableSource, duckdb-go requests the rows.
	// It sequentially calls the FillChunk method with a single thread.
	ChunkTableSource interface {
		sequentialTableSource
		// FillChunk takes a Chunk and fills it with values.
		// Set the chunk size to 0 to end the function.
		FillChunk(DataChunk) error
	}

	ChunkTableSourceContext interface {
		sequentialTableSource
		// FillChunk takes a context and a Chunk and fills it with values.
		// Set the chunk size to 0 to end the function.
		FillChunk(context.Context, DataChunk) error
	}

	// A ParallelChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelChunkTableSource, duckdb-go requests the rows.
	// It simultaneously calls the FillChunk method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillChunk must use synchronization
	// primitives to avoid race conditions.
	ParallelChunkTableSource interface {
		parallelTableSource
		// FillChunk takes a Chunk and fills it with values.
		// Set the chunk size to 0 to end the function
		FillChunk(any, DataChunk) error
	}

	ParallelChunkTableSourceContext interface {
		parallelTableSource
		// FillChunk takes a context and a Chunk and fills it with values.
		// Set the chunk size to 0 to end the function.
		FillChunk(context.Context, any, DataChunk) error
	}

	// parallelRowTSWrapper wraps a synchronous table source for a parallel context with nthreads=1
	parallelRowTSWrapper struct {
		s RowTableSource
	}

	parallelRowTSContextWrapper struct {
		s RowTableSourceContext
	}

	// parallelChunkTSWrapper wraps a synchronous table source for a parallel context with nthreads=1
	parallelChunkTSWrapper struct {
		s ChunkTableSource
	}

	parallelChunkTSContextWrapper struct {
		s ChunkTableSourceContext
	}

	// ParallelTableSourceInfo contains information for initializing a parallelism-aware table source.
	ParallelTableSourceInfo struct {
		// MaxThreads is the maximum number of threads on which to run the table source function.
		// If set to 0, it uses DuckDB's default thread configuration.
		MaxThreads int
	}
)

// ParallelRow wrapper

func (s parallelRowTSWrapper) ColumnInfos() []ColumnInfo {
	return s.s.ColumnInfos()
}

func (s parallelRowTSWrapper) Cardinality() *CardinalityInfo {
	return s.s.Cardinality()
}

func (s parallelRowTSWrapper) Init() ParallelTableSourceInfo {
	s.s.Init()
	return ParallelTableSourceInfo{
		MaxThreads: 1,
	}
}

func (s parallelRowTSWrapper) NewLocalState() any {
	return struct{}{}
}

func (s parallelRowTSWrapper) FillRow(ls any, chunk Row) (bool, error) {
	return s.s.FillRow(chunk)
}

// ParallelRowContext wrapper
func (s parallelRowTSContextWrapper) ColumnInfos() []ColumnInfo {
	return s.s.ColumnInfos()
}

func (s parallelRowTSContextWrapper) Cardinality() *CardinalityInfo {
	return s.s.Cardinality()
}

func (s parallelRowTSContextWrapper) Init() ParallelTableSourceInfo {
	s.s.Init()
	return ParallelTableSourceInfo{
		MaxThreads: 1,
	}
}

func (s parallelRowTSContextWrapper) NewLocalState() any {
	return struct{}{}
}

func (s parallelRowTSContextWrapper) FillRow(ctx context.Context, ls any, chunk Row) (bool, error) {
	return s.s.FillRow(ctx, chunk)
}

// ParallelChunk wrapper

func (s parallelChunkTSWrapper) ColumnInfos() []ColumnInfo {
	return s.s.ColumnInfos()
}

func (s parallelChunkTSWrapper) Cardinality() *CardinalityInfo {
	return s.s.Cardinality()
}

func (s parallelChunkTSWrapper) Init() ParallelTableSourceInfo {
	s.s.Init()
	return ParallelTableSourceInfo{
		MaxThreads: 1,
	}
}

func (s parallelChunkTSWrapper) NewLocalState() any {
	return struct{}{}
}

func (s parallelChunkTSWrapper) FillChunk(ls any, chunk DataChunk) error {
	return s.s.FillChunk(chunk)
}

// ParallelChunkContext wrapper
func (s parallelChunkTSContextWrapper) ColumnInfos() []ColumnInfo {
	return s.s.ColumnInfos()
}

func (s parallelChunkTSContextWrapper) Cardinality() *CardinalityInfo {
	return s.s.Cardinality()
}

func (s parallelChunkTSContextWrapper) Init() ParallelTableSourceInfo {
	s.s.Init()
	return ParallelTableSourceInfo{
		MaxThreads: 1,
	}
}

func (s parallelChunkTSContextWrapper) NewLocalState() any {
	return struct{}{}
}

func (s parallelChunkTSContextWrapper) FillChunk(ctx context.Context, ls any, chunk DataChunk) error {
	return s.s.FillChunk(ctx, chunk)
}
