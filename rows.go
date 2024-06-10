package embedded

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	return translateError(rows.rowIter.Close(rows.gmsCtx))
}

// Next is called to populate the next row of data into the provided slice. The provided slice will be the same size as
// the Columns() are wide. Next returns io.EOF when there are no more rows.
func (rows *doltRows) Next(dest []driver.Value) error {
	nextRow, err := rows.rowIter.Next(rows.gmsCtx)
	if err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return translateError(err)
	}

	if len(dest) != len(nextRow) {
		return errors.New("mismatch between expected column count and actual column count")
	}

	for i := range nextRow {
		if v, ok := nextRow[i].(driver.Valuer); ok {
			dest[i], err = v.Value()

			if err != nil {
				return fmt.Errorf("error processing column %d: %w", i, err)
			}
		} else if geomValue, ok := nextRow[i].(types.GeometryValue); ok {
			dest[i] = geomValue.Serialize()
		} else if enumType, ok := rows.sch[i].Type.(gms.EnumType); ok {
			if v, _, err := enumType.Convert(nextRow[i]); err != nil {
				return fmt.Errorf("could not convert to expected enum type for column %d: %w", i, err)
			} else if enumStr, ok := enumType.At(int(v.(uint16))); !ok {
				return fmt.Errorf("not a valid enum index for column %d: %v", i, v)
			} else {
				dest[i] = enumStr
			}
		} else if setType, ok := rows.sch[i].Type.(gms.SetType); ok {
			if v, _, err := setType.Convert(nextRow[i]); err != nil {
				return fmt.Errorf("could not convert to expected set type for column %d: %w", i, err)
			} else if setStr, err := setType.BitsToString(v.(uint64)); err != nil {
				return fmt.Errorf("could not convert value to set string for column %d: %w", i, err)
			} else {
				dest[i] = setStr
			}
		} else {
			dest[i] = nextRow[i]
		}
	}

	return nil
}
