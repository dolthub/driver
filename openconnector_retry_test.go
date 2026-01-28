package embedded

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/stretchr/testify/require"
)

func TestOpenConnectorRetriesWhenEnabled(t *testing.T) {
	dir := t.TempDir()

	var calls int32
	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 3 {
			return nil, nbs.ErrDatabaseLocked
		}
		return &engine.SqlEngine{}, nil
	}

	dsn := fmt.Sprintf(
		"file://%s?commitname=Test&commitemail=test@example.com&open_retry=true&open_retry_max_tries=10&open_retry_max_elapsed=2s",
		dir,
	)

	c, err := (&doltDriver{}).OpenConnector(dsn)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(4))
	dc, ok := c.(*doltConnector)
	require.True(t, ok)
	require.NoError(t, dc.Close())
}

func TestOpenConnectorDoesNotRetryWhenDisabled(t *testing.T) {
	dir := t.TempDir()

	var calls int32
	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		atomic.AddInt32(&calls, 1)
		return nil, nbs.ErrDatabaseLocked
	}

	dsn := fmt.Sprintf("file://%s?commitname=Test&commitemail=test@example.com", dir)
	_, err := (&doltDriver{}).OpenConnector(dsn)
	require.Error(t, err)
	require.True(t, errors.Is(err, nbs.ErrDatabaseLocked))
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestOpenConnectorRetryRespectsMaxElapsed(t *testing.T) {
	dir := t.TempDir()

	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		return nil, nbs.ErrDatabaseLocked
	}

	start := time.Now()
	dsn := fmt.Sprintf(
		"file://%s?commitname=Test&commitemail=test@example.com&open_retry=true&open_retry_max_elapsed=150ms",
		dir,
	)
	_, err := (&doltDriver{}).OpenConnector(dsn)
	elapsed := time.Since(start)

	require.Error(t, err)
	// It may return the last underlying lock error, but it should not run unbounded.
	require.Less(t, elapsed, 2*time.Second)
}
