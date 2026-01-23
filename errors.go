package embedded

import (
	"errors"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/go-sql-driver/mysql"
)

// translateError converts a go-mysql-server error into a go-sql-driver/mysql
// *MySQLError. This improves compatibility with clients that program against
// embedded and sql-server Dolt.
func translateError(err error) error {
	if err == nil {
		return nil
	}
	vitessErr := sql.CastSQLError(err)
	return &mysql.MySQLError{
		Number:  uint16(vitessErr.Num),
		Message: vitessErr.Message,
	}
}

// translateIfNeeded preserves existing *mysql.MySQLError instances, otherwise converts
// go-mysql-server errors into *mysql.MySQLError.
func translateIfNeeded(err error) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return err
	}
	return translateError(err)
}
