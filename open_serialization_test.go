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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	gms "github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenSemSerializesConcurrentOpens verifies that concurrent openSqlEngine
// calls from different connectors are serialized by the process-level semaphore.
func TestOpenSemSerializesConcurrentOpens(t *testing.T) {
	var concurrent int32
	var maxConcurrent int32

	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		cur := atomic.AddInt32(&concurrent, 1)
		defer atomic.AddInt32(&concurrent, -1)
		for {
			old := atomic.LoadInt32(&maxConcurrent)
			if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		return &engine.SqlEngine{}, nil
	}

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	dir := t.TempDir()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := NewConnector(Config{
				Directory:   dir,
				CommitName:  "test",
				CommitEmail: "test@test.com",
			})
			assert.NoError(t, err)
			if err != nil {
				return
			}
			conn, err := c.Connect(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, conn)
			_ = c.Close()
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&maxConcurrent),
		"expected at most 1 concurrent openSqlEngine call")
}

// TestOpenSemRespectsContextCancellation verifies that a caller blocked on the
// semaphore returns ctx.Err() when the context is cancelled.
func TestOpenSemRespectsContextCancellation(t *testing.T) {
	prev := openSqlEngineForConnector
	t.Cleanup(func() { openSqlEngineForConnector = prev })

	// acquired signals that the first goroutine holds the semaphore.
	acquired := make(chan struct{})
	// Install a stub that signals acquisition, then blocks until released.
	release := make(chan struct{})
	openSqlEngineForConnector = func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
		close(acquired)
		<-release
		return &engine.SqlEngine{}, nil
	}

	prevNewCtx := newLocalContextForConnector
	t.Cleanup(func() { newLocalContextForConnector = prevNewCtx })
	newLocalContextForConnector = func(se *engine.SqlEngine, ctx context.Context) (*gms.Context, error) {
		return &gms.Context{}, nil
	}

	dir := t.TempDir()
	doltCfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  "test",
		config.UserEmailKey: "test@test.com",
	})
	seCfg := &engine.SqlEngineConfig{}

	// Start a goroutine that holds the semaphore.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = openSqlEngine(context.Background(), doltCfg, filesys.LocalFS, dir, "0.40.17", seCfg)
	}()

	// Wait until the first goroutine has actually acquired the semaphore.
	<-acquired

	// Second call with a short timeout should fail with context error.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := openSqlEngine(ctx, doltCfg, filesys.LocalFS, dir, "0.40.17", seCfg)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)
	wg.Wait()
}
