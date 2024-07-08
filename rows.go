package embedded

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// doltMultiRows implements driver.RowsNextResultSet by aggregating a set of individual
// doltRows instances.
type doltMultiRows struct {
	rowSets       []*doltRows
	currentRowSet int
}

var _ driver.RowsNextResultSet = (*doltMultiRows)(nil)

func (d *doltMultiRows) Columns() []string {
	return d.rowSets[d.currentRowSet].Columns()
}

// Close implements the driver.Rows interface. When Close is called on a doltMultiRows instance,
// it will close all individual doltRows instances that it contains. If any errors are encountered
// while closing the individual row sets, the first error will be returned, after attempting to close
// all row sets.
func (d *doltMultiRows) Close() error {
	var retErr error
	for _, rowSet := range d.rowSets {
		if err := rowSet.Close(); err != nil {
			retErr = err
		}
	}
	return retErr
}

func (d *doltMultiRows) Next(dest []driver.Value) error {
	return d.rowSets[d.currentRowSet].Next(dest)
}

func (d *doltMultiRows) HasNextResultSet() bool {
	return d.currentRowSet < len(d.rowSets)-1
}

func (d *doltMultiRows) NextResultSet() error {
	if d.currentRowSet+1 >= len(d.rowSets) {
		return io.EOF
	}

	// Move to the next row set. If we encountered an error running the statement earlier and saved
	// an error in the row set, return that error now that the result set with the error has been requested.
	// This is to match the MySQL driver's behavior.
	d.currentRowSet += 1
	return d.rowSets[d.currentRowSet].err
}

type doltRows struct {
	sch     gms.Schema
	rowIter gms.RowIter
	gmsCtx  *gms.Context

	columns []string

	// err holds the error encountered while trying to retrieve this result set
	err error
}

var _ driver.Rows = (*doltRows)(nil)

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
	if rows.rowIter == nil {
		return nil
	}

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

// peekableRowIter wrap another gms.RowIter and allows the caller to peek at results, without disturbing the order
// that results are returned from the Next() method.
type peekableRowIter struct {
	iter  gms.RowIter
	peeks []gms.Row
}

var _ gms.RowIter = (*peekableRowIter)(nil)

// Peek returns the next row from this row iterator, without causing that row to be skipped from future calls
// to Next(). There is no limit on how many rows can be peeked.
func (p *peekableRowIter) Peek(ctx *gms.Context) (gms.Row, error) {
	next, err := p.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	p.peeks = append(p.peeks, next)

	return next, nil
}

// Next implements gms.RowIter
func (p *peekableRowIter) Next(ctx *gms.Context) (gms.Row, error) {
	// TODO: If peek returned an error, we need to return that error here
	//       Although... calling Next() again *should* still return the error?
	if len(p.peeks) > 0 {
		peek := p.peeks[0]
		p.peeks = p.peeks[1:]
		return peek, nil
	}

	return p.iter.Next(ctx)
}

// Close implements gms.RowIter
func (p *peekableRowIter) Close(ctx *gms.Context) error {
	return p.iter.Close(ctx)
}
