package embedded

import (
	"database/sql/driver"
)

var _ driver.Tx = (*doltTx)(nil)

type doltTx struct {
	conn *DoltConn
}

// Commit finishes the transaction.
func (tx *doltTx) Commit() error {
	_, _, _, err := tx.conn.se.Query(tx.conn.gmsCtx, "COMMIT;")
	tx.conn.endTx()
	return translateError(err)
}

// Rollback cancels the transaction.
func (tx *doltTx) Rollback() error {
	_, _, _, err := tx.conn.se.Query(tx.conn.gmsCtx, "ROLLBACK;")
	tx.conn.endTx()
	return translateError(err)
}
