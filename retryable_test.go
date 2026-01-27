package embedded

import (
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsRetryableEmbeddedErr(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.False(t, isRetryableEmbeddedErr(nil))
	})

	t.Run("mysql deadlock", func(t *testing.T) {
		require.True(t, isRetryableEmbeddedErr(&mysql.MySQLError{Number: 1213, Message: "could not serialize transaction"}))
	})

	t.Run("mysql readonly manifest", func(t *testing.T) {
		require.True(t, isRetryableEmbeddedErr(&mysql.MySQLError{Number: 1105, Message: "cannot update manifest: database is read only"}))
	})

	t.Run("raw readonly string", func(t *testing.T) {
		require.True(t, isRetryableEmbeddedErr(errString("database is read only")))
	})

	t.Run("non-retryable table not found", func(t *testing.T) {
		require.False(t, isRetryableEmbeddedErr(&mysql.MySQLError{Number: 1146, Message: "table not found: nope"}))
	})
}

type errString string

func (e errString) Error() string { return string(e) }
