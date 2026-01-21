package embedded

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/stretchr/testify/require"
)

func makeTempDSN(t *testing.T) (dsn string, cleanup func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "dolthub-driver-tests-db*")
	require.NoError(t, err)

	cleanup = func() {
		_ = os.RemoveAll(dir)
	}

	query := url.Values{
		CommitNameParam:  []string{"Billy Batson"},
		CommitEmailParam: []string{"shazam@gmail.com"},
		// Optional, but provides a realistic session default.
		DatabaseParam: []string{"testdb"},
	}
	u := url.URL{Scheme: "file", Path: encodeDir(dir), RawQuery: query.Encode()}
	return u.String(), cleanup
}

func TestEngineNotClosedOnConnChurn(t *testing.T) {
	dsn, cleanup := makeTempDSN(t)
	defer cleanup()

	db, err := sql.Open(DoltDriverName, dsn)
	require.NoError(t, err)
	defer db.Close()

	// Force underlying conns to be closed when returned to pool.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(0)

	ctx := context.Background()
	require.NoError(t, db.PingContext(ctx))

	for i := 0; i < 5; i++ {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)

		// A trivial query that doesn't depend on any schema.
		_, err = conn.ExecContext(ctx, "SELECT 1;")
		require.NoError(t, err)

		require.NoError(t, conn.Close())
	}
}

func TestEngineInitOnceUnderConcurrency(t *testing.T) {
	dsn, cleanup := makeTempDSN(t)
	defer cleanup()

	orig := newSqlEngine
	defer func() { newSqlEngine = orig }()

	var calls atomic.Int32
	newSqlEngine = func(ctx context.Context, mrEnv *env.MultiRepoEnv, cfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		calls.Add(1)
		return orig(ctx, mrEnv, cfg)
	}

	db, err := sql.Open(DoltDriverName, dsn)
	require.NoError(t, err)
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(0)

	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 25; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.ExecContext(ctx, "SELECT 1;")
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), calls.Load(), "expected exactly one SqlEngine initialization")
}

func TestDBCloseTriggersConnectorClose(t *testing.T) {
	dsn, cleanup := makeTempDSN(t)
	defer cleanup()

	origClose := closeSqlEngine
	defer func() { closeSqlEngine = origClose }()

	var closes atomic.Int32
	closeSqlEngine = func(se *engine.SqlEngine) error {
		closes.Add(1)
		return origClose(se)
	}

	d := &doltDriver{}
	connector, err := d.OpenConnector(dsn)
	require.NoError(t, err)

	db := sql.OpenDB(connector)
	ctx := context.Background()
	require.NoError(t, db.PingContext(ctx))

	require.NoError(t, db.Close())
	require.Equal(t, int32(1), closes.Load(), "expected engine to be closed exactly once")
}
