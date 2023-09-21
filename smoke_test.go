package embedded

import (
	"context"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
)

func TestMultiStatements(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t)
	defer cleanupFunc()

	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, "show tables like 'schema_migrations'")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	res, err := conn.ExecContext(ctx, "create table testtable (id int primary key, name varchar(256))")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	rows, err = conn.QueryContext(ctx, "insert into testtable values (1, 'aaron'),(2, 'brian'); insert into testtable values (3, 'tim'); select * from testtable")
	require.NoError(t, err)
	var id int
	var name string

	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 1, id)
	require.Equal(t, "aaron", name)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 2, id)
	require.Equal(t, "brian", name)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &name))
	require.Equal(t, 3, id)
	require.Equal(t, "tim", name)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	_, err = conn.QueryContext(ctx, "select * from testtable; select * from doesnotexist; select * from testtable")
	require.Error(t, err)

	require.NoError(t, conn.Close())
}

// TestQueryContextInitialization asserts that the context is correctly initialized for each query, including
// setting the current time at query execution start.
func TestQueryContextInitialization(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t)
	defer cleanupFunc()

	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, "select NOW()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	var s1, s2 string
	err = rows.Scan(&s1)
	require.NoError(t, err)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// Pause for 1s, then select NOW() and assert that the two times are different
	time.Sleep(1 * time.Second)
	rows, err = conn.QueryContext(ctx, "SELECT NOW()")
	require.NoError(t, err)
	require.True(t, rows.Next())
	err = rows.Scan(&s2)
	require.NoError(t, err)
	assert.NotEqual(t, s1, s2)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.NoError(t, conn.Close())
}

// initializeTestDatabaseConnection create a test database called testdb and initialize a database/sql connection
// using the Dolt driver. The connection, |conn|, is returned, and |cleanupFunc| is a function that the test function
// should defer in order to properly dispose of test resources.
func initializeTestDatabaseConnection(t *testing.T) (conn *sql.Conn, cleanUpFunc func()) {
	dir, err := os.MkdirTemp("", "dolthub-driver-tests-db*")
	require.NoError(t, err)

	cleanUpFunc = func() {
		os.RemoveAll(dir)
	}

	ctx := context.Background()

	db, err := sql.Open(DoltDriverName, "file://"+dir+"?commitname=Billy%20Batson&commitemail=shazam@gmail.com&database=testdb&multistatements=true")
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))

	conn, err = db.Conn(ctx)
	require.NoError(t, err)

	res, err := conn.ExecContext(ctx, "create database testdb")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, "commit;")
	require.NoError(t, err)

	return conn, cleanUpFunc
}
