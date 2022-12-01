package embedded

import (
	"errors"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestTranslateError(t *testing.T) {
	tests := []struct {
		original       error
		expectedNumber uint16
	}{
		{
			sql.ErrLockDeadlock.New("could not serialize transaction"),
			1213,
		},
		{
			nil,
			0,
		},
	}
	for _, test := range tests {
		err := translateError(test.original)
		if err == nil {
			require.Equal(t, uint16(0), test.expectedNumber)
		} else {
			var mysqlErr *mysql.MySQLError
			require.True(t, errors.As(err, &mysqlErr))
			require.Equal(t, test.expectedNumber, mysqlErr.Number)
		}
	}
}
