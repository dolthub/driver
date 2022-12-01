package embedded

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiStatements(t *testing.T) {
	dir, err := os.MkdirTemp("", "dolthub-driver-tests-db*")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	ctx := context.Background()

	db, err := sql.Open(DoltDriverName, "file://"+dir+"?commitname=Billy%20Batson&commitemail=shazam@gmail.com&database=testdb&multistatements=true")
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	res, err := conn.ExecContext(ctx, "create database testdb")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "show tables like 'schema_migrations'")
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	res, err = conn.ExecContext(ctx, "create table testtable (id int primary key, name varchar(256))")
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
	require.NoError(t, db.Close())
}
