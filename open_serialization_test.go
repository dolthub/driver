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
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
)

// Independent connectors must be able to enter engine initialization together.
func TestIndependentConnectorsOpenConcurrently(t *testing.T) {
	entered := make(chan *engine.SqlEngineConfig, 2)
	release := make(chan struct{})

	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		entered <- seCfg
		<-release
		return &engine.SqlEngine{}, nil
	}

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	connectors := make([]*Connector, 2)
	for i := range connectors {
		var err error
		connectors[i], err = NewConnector(Config{
			Directory:   t.TempDir(),
			CommitName:  "test",
			CommitEmail: "test@test.com",
		})
		require.NoError(t, err)
	}
	results := make(chan error, len(connectors))
	for _, connector := range connectors {
		go func() {
			conn, err := connector.Connect(context.Background())
			if err == nil {
				err = conn.Close()
			}
			if closeErr := connector.Close(); err == nil {
				err = closeErr
			}
			results <- err
		}()
	}

	var configs [2]*engine.SqlEngineConfig
	overlapped := true
waitForOpens:
	for i := range configs {
		select {
		case configs[i] = <-entered:
		case <-time.After(10 * time.Second):
			overlapped = false
			break waitForOpens
		}
	}
	close(release)
	for range connectors {
		require.NoError(t, <-results)
	}
	require.True(t, overlapped, "independent engine opens did not overlap")
	require.NotNil(t, configs[0].EngineVariables)
	require.NotNil(t, configs[1].EngineVariables)
	require.NotSame(t, configs[0].EngineVariables, configs[1].EngineVariables)
}

// A second caller of the same connector can cancel while its first open is in progress.
func TestConnectorOpenWaiterCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32

	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return &engine.SqlEngine{}, nil
	}

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	connector, err := NewConnector(Config{
		Directory:   t.TempDir(),
		CommitName:  "test",
		CommitEmail: "test@test.com",
	})
	require.NoError(t, err)
	firstResult := make(chan error, 1)
	go func() {
		conn, err := connector.Connect(context.Background())
		if err == nil {
			err = conn.Close()
		}
		firstResult <- err
	}()
	<-entered

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, waitErr := connector.Connect(ctx)
	close(release)
	firstErr := <-firstResult
	require.ErrorIs(t, waitErr, context.DeadlineExceeded)
	require.NoError(t, firstErr)
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, connector.Close())
}
