package duckdb

import "context"

type streamingResultsKey struct{}

// WithStreamingResults returns a context under which QueryContext returns a streaming result:
// DuckDB produces the rows as they are fetched instead of materialising the whole result before
// the first row is returned, so memory stays bounded by the chunk being read rather than by the
// result size.
//
// While the rows are open, the connection that produced them must not run another statement:
// doing so invalidates the stream, and the next call to Next reports the error. A query that
// fails part-way through also reports its error from Next (and Rows.Err), not from QueryContext.
func WithStreamingResults(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamingResultsKey{}, true)
}

func streamingResults(ctx context.Context) bool {
	on, _ := ctx.Value(streamingResultsKey{}).(bool)
	return on
}
