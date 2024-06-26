package embedded

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiStatements(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
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

func TestMultiStatementsStoredProc(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
	rows, err := conn.QueryContext(ctx, "create procedure p() begin select 1; end; call p(); call p(); call p();")
	require.NoError(t, err)
	for rows.Next() {
		var i int
		err = rows.Scan(&i)
		require.NoError(t, err)
		require.Equal(t, 1, i)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}

func TestMultiStatementsTrigger(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
	res, err := conn.ExecContext(ctx, "create table t (i int primary key, j int);")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "create trigger trig before insert on t for each row begin set new.j = new.j * 100; end; insert into t values (1, 2); select * from t;")
	require.NoError(t, err)
	for rows.Next() {
		var i, j int
		err = rows.Scan(&i, &j)
		require.NoError(t, err)
		require.Equal(t, 1, i)
		require.Equal(t, 200, j)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
}

// TestClientFoundRows asserts that the number of affected rows reported for a query
// correctly reflects whether the CLIENT_FOUND_ROWS capability is set or not.
func TestClientFoundRows(t *testing.T) {
	queries := []string{
		"insert into testtable values (1,'aaron'),(2,'brian'),(3,'tim')",
		"insert into testtable values (4,'aaron2'),(2,'brian2'),(3,'tim') on duplicate key update name = VALUES(name)",
		"update testtable set name = (case id when 1 then 'aaron' when 2 then 'tim2' when 4 then 'aaron2' end) where id in (1,3,4)",
	}

	tests := []struct {
		name            string
		clientFoundRows bool
		expectedRows    []int64
	}{
		{
			name:            "client_found_rows_disabled",
			clientFoundRows: false,
			expectedRows:    []int64{3, 3, 1},
		},
		{
			name:            "client_found_rows_enabled",
			clientFoundRows: true,
			expectedRows:    []int64{3, 4, 3},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, cleanupFunc := initializeTestDatabaseConnection(t, test.clientFoundRows)
			defer cleanupFunc()
			ctx := context.Background()

			res, err := conn.ExecContext(ctx, "create table testtable (id int primary key, name varchar(256))")
			require.NoError(t, err)
			_, err = res.RowsAffected()
			require.NoError(t, err)

			for i, query := range queries {
				res, err := conn.ExecContext(ctx, query)
				require.NoError(t, err)
				rowsAffected, err := res.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, test.expectedRows[i], rowsAffected)
			}
		})
	}
}

// TestQueryContextInitialization asserts that the context is correctly initialized for each query, including
// setting the current time at query execution start.
func TestQueryContextInitialization(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
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

// TestTypes asserts that various MySQL types are returned as the expected Go type by the driver.
func TestTypes(t *testing.T) {
	conn, cleanupFunc := initializeTestDatabaseConnection(t, false)
	defer cleanupFunc()

	ctx := context.Background()
	_, err := conn.ExecContext(ctx, `
create table testtable (
	enum_col ENUM('a', 'b', 'c'),
	set_col SET('a', 'b', 'c'),
	json_col JSON,
	blob_col BLOB,
	text_col TEXT,
	geom_col POINT,
	date_col DATETIME
);

insert into testtable values ('b', 'a,c', '{"key": 42}', 'data', 'text', Point(5, -5), NOW());
`)
	require.NoError(t, err)

	row := conn.QueryRowContext(ctx, "select * from testtable")
	vals := make([]any, 7)
	ptrs := make([]any, 7)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	require.NoError(t, row.Scan(ptrs...))
	require.Equal(t, "b", vals[0])
	require.Equal(t, "a,c", vals[1])
	require.Equal(t, `{"key": 42}`, vals[2])
	require.Equal(t, []byte(`data`), vals[3])
	require.Equal(t, "text", vals[4])
	require.IsType(t, []byte(nil), vals[5])
	require.IsType(t, time.Time{}, vals[6])
}

// initializeTestDatabaseConnection create a test database called testdb and initialize a database/sql connection
// using the Dolt driver. The connection, |conn|, is returned, and |cleanupFunc| is a function that the test function
// should defer in order to properly dispose of test resources.
func initializeTestDatabaseConnection(t *testing.T, clientFoundRows bool) (conn *sql.Conn, cleanUpFunc func()) {
	dir, err := os.MkdirTemp("", "dolthub-driver-tests-db*")
	require.NoError(t, err)

	cleanUpFunc = func() {
		os.RemoveAll(dir)
	}

	ctx := context.Background()

	query := url.Values{
		"commitname":      []string{"Billy Batson"},
		"commitemail":     []string{"shazam@gmail.com"},
		"database":        []string{"testdb"},
		"multistatements": []string{"true"},
	}
	if clientFoundRows {
		query["clientfoundrows"] = []string{"true"}
	}
	dsn := url.URL{Scheme: "file", Path: encodeDir(dir), RawQuery: query.Encode()}
	db, err := sql.Open(DoltDriverName, dsn.String())
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))

	conn, err = db.Conn(ctx)
	require.NoError(t, err)

	res, err := conn.ExecContext(ctx, "create database testdb")
	require.NoError(t, err)
	_, err = res.RowsAffected()
	require.NoError(t, err)

	return conn, cleanUpFunc
}

func encodeDir(dir string) string {
	// encodeDir translate a given path to a URL compatible path, mostly for windows compatibility
	if os.PathSeparator == '\\' {
		dir = strings.ReplaceAll(dir, `\`, `/`)
	}
	return dir
}
