package embedded

import (
	"database/sql/driver"
	gms "github.com/dolthub/go-mysql-server/sql"
	"io"
)

var _ driver.Result = (*doltResult)(nil)

func int64Ptr(val int64) *int64 {
	return &val
}

type doltResult struct {
	sch      gms.Schema
	rowItr   gms.RowIter
	sqlCtx   *gms.Context
	affected *int64
	last     *int64
}

func NewResult(sqlCtx *gms.Context, sch gms.Schema, rowItr gms.RowIter) *doltResult {
	return &doltResult{
		sch:    sch,
		rowItr: rowItr,
		sqlCtx: sqlCtx,
	}
}

func (result *doltResult) processRows() error {
	if result.affected == nil {
		var affected int64
		for {
			_, err := result.rowItr.Next(result.sqlCtx)
			if err != nil {
				if err != io.EOF {
					return err
				}

				break
			}
			affected++
		}

		result.affected = int64Ptr(affected)
		result.last = int64Ptr(0)
	}

	return nil
}

func (result *doltResult) LastInsertId() (int64, error) {
	err := result.processRows()
	if err != nil {
		return 0, err
	}

	return *result.last, nil
}

func (result *doltResult) RowsAffected() (int64, error) {
	err := result.processRows()
	if err != nil {
		return 0, err
	}

	return *result.affected, nil
}
