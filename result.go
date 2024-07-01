package embedded

import (
	"database/sql/driver"
	"io"

	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

//var _ driver.Result = (*doltResult)(nil)
var _ driver.RowsNextResultSet = (*doltResult)(nil)

type doltResult struct {
	affected int64
	last     int64
	err      error
}

func (result *doltResult) Columns() []string {
	//TODO implement me
	panic("implement me")
}

func (result *doltResult) Close() error {
	//TODO implement me
	panic("implement me")
}

func (result *doltResult) Next(dest []driver.Value) error {
	//TODO implement me
	panic("implement me")
}

func (result *doltResult) HasNextResultSet() bool {
	//TODO implement me
	panic("implement me")
}

func (result *doltResult) NextResultSet() error {
	//TODO implement me
	panic("implement me")
}


func newResult(gmsCtx *gms.Context, sch gms.Schema, rowItr gms.RowIter) *doltResult {
	var resultErr error
	var affected int64
	var last int64

	for {
		r, err := rowItr.Next(gmsCtx)
		if err != nil {
			if err != io.EOF {
				resultErr = translateError(err)
			}
			break
		}

		for i := range r {
			if res, ok := r[i].(types.OkResult); ok {
				affected += int64(res.RowsAffected)
				last = int64(res.InsertID)
			}
		}
	}

	if err := rowItr.Close(gmsCtx); err != nil {
		return &doltResult{err: err}
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
