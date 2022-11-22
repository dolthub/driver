package embedded

import (
	"database/sql/driver"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

var _ driver.Tx = (*doltTx)(nil)

type doltTx struct {
	id     string
	gmsCtx *gms.Context
	se     *engine.SqlEngine
}

// Commit finishes the transaction.
func (tx *doltTx) Commit() error {
	_, _, err := tx.se.Query(tx.gmsCtx, "COMMIT;")
	if err != nil {
		fmt.Printf("DUSTIN: TX COMMIT: id: %s, err: %s\n", tx.id, err.Error())
	}
	fmt.Println("DUSTIN: TX COMMIT: id", tx.id)
	return err
}

// Rollback cancels the transaction.
func (tx *doltTx) Rollback() error {
	_, _, err := tx.se.Query(tx.gmsCtx, "ROLLBACK;")
	if err != nil {
		fmt.Printf("DUSTIN: TX ROLLBACK: id: %s err: %s \n", tx.id, err.Error())
	}
	fmt.Println("DUSTIN: TX ROLLBACK: id:", tx.id)
	return err
}
