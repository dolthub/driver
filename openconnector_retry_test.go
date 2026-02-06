// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

func TestOpenConnectorRetriesWhenEnabled(t *testing.T) {
	// disable metrics during test runs
	// no need to set it back to false since no test should have it set to true
	metricsDisabled.Store(true)

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

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	dsn := fmt.Sprintf("file://%s?commitname=Test&commitemail=test@example.com", dir)
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	cfg.BackOff = backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 10)

	c, err := NewConnector(cfg)
	require.NoError(t, err)
	require.NotNil(t, c)
	_, err = c.Connect(context.Background())
	require.NoError(t, err)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(4))
	require.NoError(t, c.Close())
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

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	dsn := fmt.Sprintf("file://%s?commitname=Test&commitemail=test@example.com", dir)
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	c, err := NewConnector(cfg)
	require.NoError(t, err)
	_, err = c.Connect(context.Background())
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

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	dsn := fmt.Sprintf("file://%s?commitname=Test&commitemail=test@example.com", dir)
	cfg, err := ParseDSN(dsn)
	require.NoError(t, err)
	cfg.BackOff = backoff.NewConstantBackOff(10 * time.Millisecond)
	c, err := NewConnector(cfg)
	require.NoError(t, err)

	openCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = c.Connect(openCtx)
	elapsed := time.Since(start)

	require.Error(t, err)
	// It may return the last underlying lock error, but it should not run unbounded.
	require.Less(t, elapsed, 2*time.Second)
}
