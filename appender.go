package duckdb

import (
	"database/sql/driver"
	"errors"

	"github.com/duckdb/duckdb-go/mapping"
)

// Appender wraps functionality around the DuckDB appender.
// It enables efficient bulk transformations.
type Appender struct {
	// The raw sql.Conn's driver connection.
	conn *Conn
	// The DuckDB appender.
	appender mapping.Appender
	// True, if the appender has been closed.
	closed bool

	// The chunk to append to.
	chunk DataChunk
	// The column types of the table to append to.
	types []mapping.LogicalType
	// The number of appended rows.
	rowCount int
}

// NewAppenderFromConn returns a new Appender for the default catalog.
// The Appender batches rows via AppendRow. Upon reaching the auto-flush threshold or
// upon calling Flush or Close, it appends these rows to the table.
// Thus, it can be used instead of INSERT INTO statements to enable bulk insertions.
// `driverConn` is the raw sql.Conn's driver connection.
// `schema` and `table` specify the table (`schema.table`) to append to.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	return NewAppender(driverConn, "", schema, table)
}

// NewAppender returns a new Appender.
// The Appender batches rows via AppendRow. Upon reaching the auto-flush threshold or
// upon calling Flush or Close, it appends these rows to the table.
// Thus, it can be used instead of INSERT INTO statements to enable bulk insertions.
// `driverConn` is the raw sql.Conn's driver connection.
// `catalog`, `schema` and `table` specify the table (`catalog.schema.table`) to append to.
func NewAppender(driverConn driver.Conn, catalog, schema, table string) (*Appender, error) {
	return newTableAppender(driverConn, catalog, schema, table, nil)
}

// NewAppenderWithColumns returns a new Appender that is restricted to a subset of columns.
// This enables more efficient appends by narrowing the appender scope to only the provided columns.
// The Appender batches rows via AppendRow.
// Each row must provide values for exactly the selected columns.
// DuckDB will fill columns not selected with their DEFAULT values (or NULL).
// Note: Changing the active column set causes a flush in DuckDB. Therefore, we cannot change them later during the
// lifetime of the Appender.
// Important: `QueryAppender` is the recommended and more performant way to append data to a table with a subset
// of columns, we expose this mostly for backwards compatibility.
func NewAppenderWithColumns(driverConn driver.Conn, catalog, schema, table string, columns []string) (*Appender, error) {
	if len(columns) == 0 {
		return nil, invalidInputError("empty array", "non-empty array")
	}
	return newTableAppender(driverConn, catalog, schema, table, columns)
}

// newTableAppender consolidates the common logic of creating an appender, optionally narrowing
// it to a subset of columns before fetching types. NewAppender and NewAppenderWithColumns delegate to this helper
func newTableAppender(driverConn driver.Conn, catalog, schema, table string, columns []string) (*Appender, error) {
	var a Appender
	err := a.appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	state := mapping.AppenderCreateExt(a.conn.conn, catalog, schema, table, &a.appender)
	if state == mapping.StateError {
		err = errorDataError(mapping.AppenderErrorData(a.appender))
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	// If a subset of columns is provided, activate only those columns on the appender
	// BEFORE fetching types, so the type enumeration reflects only the active columns.
	if len(columns) > 0 {
		if err := a.initTableColumns(columns); err != nil {
			mapping.AppenderDestroy(&a.appender)
			return nil, err
		}
		return a.initAppenderChunk()
	}

	// Get the column types for all columns when no subset is specified (already happens in C++ side when not
	// activating columns).
	// (In DuckDB, if columns are not added, BaseAppender::GetActiveTypes() will return all table columns)
	columnCount := mapping.AppenderColumnCount(a.appender)
	for i := range uint64(columnCount) {
		colType := mapping.AppenderColumnType(a.appender, mapping.IdxT(i))
		a.types = append(a.types, colType)

		// Ensure that we only create an appender for supported column types.
		t := mapping.GetTypeId(colType)
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err = addIndexToError(unsupportedTypeError(name), int(i)+1)
			destroyLogicalTypes(a.types)
			mapping.AppenderDestroy(&a.appender)
			return nil, getError(errAppenderCreation, err)
		}
	}

	return a.initAppenderChunk()
}

// When providing columns to the appender (during initialization), we activate them and fetch their types.
func (a *Appender) initTableColumns(columns []string) error {
	// When the subset is greater than all table columns, early-out with an error.
	if mapping.AppenderColumnCount(a.appender) < mapping.IdxT(len(columns)) {
		destroyLogicalTypes(a.types)
		return getError(errAppenderCreation, errors.New("column count exceeds table column count"))
	}

	activatedColumns := make(map[string]struct{}, len(columns))
	for i, col := range columns {
		if _, exists := activatedColumns[col]; exists {
			destroyLogicalTypes(a.types)
			return getError(errAppenderCreation, errors.New("duplicate column in appender columns list: "+col))
		}

		if mapping.AppenderAddColumn(a.appender, col) == mapping.StateError {
			duckError := getDuckDBError(mapping.AppenderError(a.appender))
			return getError(errAppenderCreation, duckError)
		}

		// In DuckDB, when activating a column (`AppenderAddColumn`), we push it to the active types,
		// and then `AppenderColumnType` will result in the logical type corresponding to that index
		colType := mapping.AppenderColumnType(a.appender, mapping.IdxT(i))
		a.types = append(a.types, colType)

		// Ensure that we only create an appender for supported column types.
		t := mapping.GetTypeId(colType)
		if name, found := unsupportedTypeToStringMap[t]; found {
			err := addIndexToError(unsupportedTypeError(name), i+1)
			destroyLogicalTypes(a.types)
			return getError(errAppenderCreation, err)
		}

		activatedColumns[col] = struct{}{}
	}

	// Sanity check: active column count should match provided columns
	if mapping.AppenderColumnCount(a.appender) != mapping.IdxT(len(columns)) {
		destroyLogicalTypes(a.types)
		return getError(errAppenderCreation, errors.New("duckdb: column count mismatch after activation"))
	}
	return nil
}

// NewQueryAppender returns a new query Appender.
// The Appender batches rows via AppendRow. Upon reaching the auto-flush threshold or
// upon calling Flush or Close, it executes the query, treating the batched rows as a temporary table.
// `driverConn` is the raw sql.Conn's driver connection.
// `query` is the query to execute. It can be a INSERT, DELETE, UPDATE or MERGE INTO statement.
// `table` is the (optional) table name of the temporary table containing the batched rows.
// It defaults to `appended_data`.
// `colTypes` are the column types of the temporary table.
// `colNames` are the (optional) names of the columns of the temporary table containing the batched rows.
// They default to `col1`, `col2`, ...
func NewQueryAppender(driverConn driver.Conn, query, table string, colTypes []TypeInfo, colNames []string) (*Appender, error) {
	var a Appender
	err := a.appenderConn(driverConn)
	if err != nil {
		return nil, err
	}

	if query == "" {
		return nil, getError(errAppenderEmptyQuery, nil)
	}
	if len(colTypes) == 0 {
		return nil, getError(errAppenderEmptyColumnTypes, nil)
	}
	if len(colNames) != 0 && len(colTypes) != 0 {
		if len(colNames) != len(colTypes) {
			return nil, getError(errAppenderColumnMismatch, nil)
		}
	}

	// Get the logical types via the type infos.
	for _, ct := range colTypes {
		a.types = append(a.types, ct.logicalType())
	}

	state := mapping.AppenderCreateQuery(a.conn.conn, query, a.types, table, colNames, &a.appender)
	if state == mapping.StateError {
		destroyLogicalTypes(a.types)
		err = errorDataError(mapping.AppenderErrorData(a.appender))
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	return a.initAppenderChunk()
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	if err := a.appendDataChunk(); err != nil {
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		err := getDuckDBError(mapping.AppenderError(a.appender))
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	errAppend := a.appendDataChunk()
	a.chunk.close()

	// We flush before closing to get a meaningful error message.
	var errFlush error
	if mapping.AppenderFlush(a.appender) == mapping.StateError {
		errFlush = getDuckDBError(mapping.AppenderError(a.appender))
	}

	// Destroy all appender data and the appender.
	destroyLogicalTypes(a.types)
	var errClose error
	if mapping.AppenderDestroy(&a.appender) == mapping.StateError {
		errClose = errAppenderClose
	}

	err := errors.Join(errAppend, errFlush, errClose)
	if err != nil {
		return getError(invalidatedAppenderError(err), nil)
	}

	return nil
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}

	return nil
}

func (a *Appender) appenderConn(driverConn driver.Conn) error {
	var ok bool
	a.conn, ok = driverConn.(*Conn)
	if !ok {
		return getError(errInvalidCon, nil)
	}
	if a.conn.closed {
		return getError(errClosedCon, nil)
	}

	return nil
}

func (a *Appender) initAppenderChunk() (*Appender, error) {
	if err := a.chunk.initFromTypes(a.types, true); err != nil {
		a.chunk.close()
		destroyLogicalTypes(a.types)
		mapping.AppenderDestroy(&a.appender)
		return nil, getError(errAppenderCreation, err)
	}

	return a, nil
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	// Early-out, if the number of args does not match the column count.
	if len(args) != len(a.types) {
		return columnCountError(len(args), len(a.types))
	}

	// Create a new data chunk if the current chunk is full.
	if a.rowCount == GetDataChunkCapacity() {
		if err := a.appendDataChunk(); err != nil {
			return err
		}
	}

	// Set all values.
	for i, val := range args {
		err := a.chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return err
		}
	}
	a.rowCount++

	return nil
}

func (a *Appender) appendDataChunk() error {
	if a.rowCount == 0 {
		// Nothing to append.
		return nil
	}
	if err := a.chunk.SetSize(a.rowCount); err != nil {
		return err
	}
	if mapping.AppendDataChunk(a.appender, a.chunk.chunk) == mapping.StateError {
		return getDuckDBError(mapping.AppenderError(a.appender))
	}

	a.chunk.reset(true)
	a.rowCount = 0

	return nil
}
