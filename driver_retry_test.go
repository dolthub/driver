package embedded

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/store/nbs"
	"github.com/stretchr/testify/require"
)

func TestEnsureEngineRetriesOnDatabaseLockedUntilContextDeadline(t *testing.T) {
	var calls int32
	dc := &doltConnector{
		openEngineFunc: func(ctx context.Context) (*engine.SqlEngine, error) {
			atomic.AddInt32(&calls, 1)
			return nil, nbs.ErrDatabaseLocked
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	se, err := dc.ensureEngine(ctx)
	require.Nil(t, se)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1))
}

func TestEnsureEngineSingleOpenerAcrossConcurrentCallers(t *testing.T) {
	var calls int32
	started := make(chan struct{})
	release := make(chan struct{})

	dc := &doltConnector{
		openEngineFunc: func(ctx context.Context) (*engine.SqlEngine, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				close(started)
			}
			select {
			case <-release:
				return &engine.SqlEngine{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}

	const n = 20
	var wg sync.WaitGroup
	wg.Add(n)

	errs := make([]error, n)
	engs := make([]*engine.SqlEngine, n)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			engs[i], errs[i] = dc.ensureEngine(context.Background())
		}()
	}

	<-started
	close(release)
	wg.Wait()

	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, engs[i])
		require.Same(t, engs[0], engs[i])
	}
}

func TestCloseCancelsInFlightOpen(t *testing.T) {
	var calls int32
	started := make(chan struct{})

	dc := &doltConnector{
		openEngineFunc: func(ctx context.Context) (*engine.SqlEngine, error) {
			atomic.AddInt32(&calls, 1)
			select {
			case <-started:
			default:
				close(started)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	done := make(chan error, 1)
	go func() {
		_, err := dc.ensureEngine(context.Background())
		done <- err
	}()

	<-started
	require.NoError(t, dc.Close())

	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("ensureEngine did not return after Close()")
	}

	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(1))
}

