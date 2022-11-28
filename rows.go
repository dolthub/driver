package embedded

import (
	"database/sql/driver"
	"errors"
	"fmt"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Rows = (*doltRows)(nil)

type doltRows struct {
	sch     gms.Schema
	rowIter gms.RowIter
	gmsCtx  *gms.Context

	columns []string
}

// Columns returns the names of the columns. The number of columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty string should be returned for that entry.
func (rows *doltRows) Columns() []string {
	if rows.columns == nil {
		rows.columns = make([]string, len(rows.sch))
		for i, col := range rows.sch {
			rows.columns[i] = col.Name
		}
	}

	return rows.columns
}

// Close closes the rows iterator.
func (rows *doltRows) Close() error {
	return rows.rowIter.Close(rows.gmsCtx)
}

// Next is called to populate the next row of data into the provided slice. The provided slice will be the same size as
// the Columns() are wide. Next returns io.EOF when there are no more rows.
func (rows *doltRows) Next(dest []driver.Value) error {
	nextRow, err := rows.rowIter.Next(rows.gmsCtx)
	if err != nil {
		return err
	}

	if len(dest) != len(nextRow) {
		return errors.New("mismatch between expected column count and actual column count")
	}

	for i := range nextRow {
		src := nextRow[i]
		switch v := nextRow[i].(type) {
		case gms.JSONDocument:
			src, err = v.Value()
		}

		if err != nil {
			return fmt.Errorf("error processing column %d: %w", i, err)
		}

		dest[i] = src
	}

	return nil
}
