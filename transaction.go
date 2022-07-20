package embedded

type DoltTx struct {
}

func (tx DoltTx) Commit() error {
	return nil
}

func (tx DoltTx) Rollback() error {
	return nil
}
