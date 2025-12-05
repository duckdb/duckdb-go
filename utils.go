package duckdb

func inBounds[T any](s []T, idx int) bool {
	return idx >= 0 && idx < len(s)
}
