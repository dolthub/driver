package embedded

import (
	"database/sql/driver"
	"errors"
	"github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Rows = (*DoltRows)(nil)

type DoltRows struct {
	Schema  sql.Schema
	RowIter sql.RowIter
	SqlCtx  *sql.Context

	columns []string
}

func (rows *DoltRows) Columns() []string {
	if rows.columns == nil {
		rows.columns = make([]string, len(rows.Schema))
		for i, col := range rows.Schema {
			rows.columns[i] = col.Name
		}
	}

	return rows.columns
}

func (rows *DoltRows) Close() error {
	return rows.RowIter.Close(rows.SqlCtx)
}

func (rows *DoltRows) Next(dest []driver.Value) error {
	nextRow, err := rows.RowIter.Next(rows.SqlCtx)
	if err != nil {
		return err
	}

	if len(dest) != len(nextRow) {
		return errors.New("mismatch between expected column count and actual column count")
	}

	for i := range nextRow {
		dest[i] = nextRow[i]
	}

	return nil
}
