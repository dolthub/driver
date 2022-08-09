package embedded

import (
	"io"

	"database/sql/driver"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Result = (*doltResult)(nil)

type doltResult struct {
	affected int64
	last     int64
	err      error
}

func newResult(gmsCtx *gms.Context, sch gms.Schema, rowItr gms.RowIter) *doltResult {
	var resultErr error
	var affected int64
	var last int64

	for {
		r, err := rowItr.Next(gmsCtx)
		if err != nil {
			if err != io.EOF {
				resultErr = err
			}
			break
		}

		for i := range r {
			if res, ok := r[i].(gms.OkResult); ok {
				affected += int64(res.RowsAffected)
				last = int64(res.InsertID)
			}
		}
	}

	return &doltResult{
		affected: affected,
		last:     last,
		err:      resultErr,
	}
}

// LastInsertId returns the database's auto-generated ID after, for example, an INSERT into a table with primary key.
func (result *doltResult) LastInsertId() (int64, error) {
	if result.err != nil {
		return 0, result.err
	}

	return result.last, nil
}

// RowsAffected returns the number of rows affected by the query.
func (result *doltResult) RowsAffected() (int64, error) {
	if result.err != nil {
		return 0, result.err
	}

	return result.affected, nil
}
