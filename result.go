package embedded

import "database/sql/driver"

var _ driver.Result = (*DoltResult)(nil)

type DoltResult struct{}

func (result *DoltResult) LastInsertId() (int64, error) {
	panic("not implemented")
}

func (result *DoltResult) RowsAffected() (int64, error) {
	panic("not implemented")
}
