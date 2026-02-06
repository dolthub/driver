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
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

var _ driver.Connector = (*Connector)(nil)

const defaultDoltVersion = "0.40.17"

// newLocalContextForConnector exists to make Connector.Connect testable without needing
// to construct a fully initialized Dolt engine / session. Production code should leave this nil.
var newLocalContextForConnector func(se *engine.SqlEngine, ctx context.Context) (*gmssql.Context, error)

func newLocalContext(se *engine.SqlEngine, ctx context.Context) (*gmssql.Context, error) {
	if newLocalContextForConnector != nil {
		return newLocalContextForConnector(se, ctx)
	}
	return se.NewLocalContext(ctx)
}

// Connector is a database/sql driver connector for embedded Dolt.
//
// Callers should construct it with NewConnector and then pass it to sql.OpenDB.
// The connector shares a single underlying embedded engine across connections,
// and creates a per-connection session context on each Connect.
type Connector struct {
	cfg    Config
	driver *doltDriver

	mu     sync.Mutex
	se     *engine.SqlEngine
	openCh chan struct{}
	closed bool
}

// NewConnector constructs a new connector for embedded Dolt. The embedded engine
// is opened lazily on first Connect (and shared thereafter).
//
// If cfg.BackOff is non-nil, opening the engine will be retried for retryable
// open errors (e.g. lock contention) using that backoff, bounded by the Connect
// context.
func NewConnector(cfg Config) (*Connector, error) {
	if cfg.Directory == "" {
		return nil, errors.New("config.Directory is required")
	}
	if cfg.CommitName == "" {
		return nil, errors.New("config.CommitName is required")
	}
	if cfg.CommitEmail == "" {
		return nil, errors.New("config.CommitEmail is required")
	}
	if cfg.Version == "" {
		cfg.Version = defaultDoltVersion
	}
	// Validate directory exists (ParseDSN does this, but callers can build Config directly).
	var fs filesys.Filesys = filesys.LocalFS
	exists, isDir := fs.Exists(cfg.Directory)
	if !exists {
		return nil, fmt.Errorf("'%s' does not exist", cfg.Directory)
	} else if !isDir {
		return nil, fmt.Errorf("%s: is a file. need to specify a directory", cfg.Directory)
	}

	return &Connector{
		cfg:    cfg,
		driver: &doltDriver{},
	}, nil
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Connect implements driver.Connector.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	se, err := c.getOrOpenEngine(ctx)
	if err != nil {
		return nil, err
	}

	gmsCtx, err := newLocalContext(se, ctx)
	if err != nil {
		return nil, err
	}

	if c.cfg.Database != "" {
		gmsCtx.SetCurrentDatabase(c.cfg.Database)
	}

	if c.cfg.ClientFoundRows {
		client := gmsCtx.Client()
		gmsCtx.SetClient(gmssql.Client{
			User:         client.User,
			Address:      client.Address,
			Capabilities: client.Capabilities | mysql.CapabilityClientFoundRows,
		})
	}

	return &DoltConn{
		se:         se,
		gmsCtx:     gmsCtx,
		DataSource: nil,
		cfg:        &c.cfg,
	}, nil
}

// Close closes the shared embedded engine, if it has been opened.
// It is safe to call multiple times.
func (c *Connector) Close() error {
	c.mu.Lock()
	c.closed = true
	se := c.se
	c.se = nil
	ch := c.openCh
	c.openCh = nil
	c.mu.Unlock()

	// If an open is in progress, let it finish; Close doesn't block on it.
	// (Connect calls will see closed=true and fail.)
	if ch != nil {
		select {
		case <-ch:
		default:
		}
	}

	if se != nil {
		return se.Close()
	}
	return nil
}

func (c *Connector) getOrOpenEngine(ctx context.Context) (*engine.SqlEngine, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, errors.New("connector is closed")
		}
		if c.se != nil {
			se := c.se
			c.mu.Unlock()
			return se, nil
		}
		if c.openCh != nil {
			ch := c.openCh
			c.mu.Unlock()
			select {
			case <-ch:
				// Loop and re-check se/closed.
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		// Become the opener.
		ch := make(chan struct{})
		c.openCh = ch
		c.mu.Unlock()

		se, err := c.openEngineWithRetry(ctx)

		c.mu.Lock()
		// If we got a successful engine and connector isn't closed, store it.
		if err == nil && !c.closed {
			c.se = se
		} else if se != nil {
			// If open succeeded but connector is closed, immediately close it.
			_ = se.Close()
		}
		c.openCh = nil
		close(ch)
		c.mu.Unlock()

		if err != nil {
			return nil, err
		}
		// Loop to return the stored engine (or error if closed).
	}
}

func (c *Connector) openEngineWithRetry(ctx context.Context) (*engine.SqlEngine, error) {
	// Dolt user config (commit metadata).
	doltCfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  c.cfg.CommitName,
		config.UserEmailKey: c.cfg.CommitEmail,
	})

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	// For deterministic retries on lock contention.
	if c.cfg.BackOff != nil {
		if seCfg.DBLoadParams == nil {
			seCfg.DBLoadParams = make(map[string]interface{})
		}
		seCfg.DBLoadParams[dbfactory.DisableSingletonCacheParam] = struct{}{}
		seCfg.DBLoadParams[dbfactory.FailOnJournalLockTimeoutParam] = struct{}{}
	}

	var fs filesys.Filesys = filesys.LocalFS
	wd, err := fs.WithWorkingDir(c.cfg.Directory)
	if err != nil {
		return nil, err
	}

	open := func(openCtx context.Context) (*engine.SqlEngine, error) {
		return openSqlEngine(openCtx, doltCfg, wd, c.cfg.Directory, c.cfg.Version, seCfg)
	}

	if c.cfg.BackOff == nil {
		return open(ctx)
	}

	// BackOff is stateful; reset before use.
	c.cfg.BackOff.Reset()
	bo := backoff.WithContext(c.cfg.BackOff, ctx)

	var lastErr error
	var se *engine.SqlEngine
	op := func() error {
		s, err := open(ctx)
		if err == nil {
			se = s
			return nil
		}
		lastErr = err
		if isRetryableOpenErr(err) {
			return err
		}
		return backoff.Permanent(err)
	}

	if err := backoff.Retry(op, bo); err != nil {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, err
	}
	return se, nil
}

// Two tracking vars to ensure we only emit metrics once per process, and that we don't emit if the env var is set.
// These are atomic bools to avoid races in test, or when there are multiple connectors in the same process.
var metricsDisabled = &atomic.Bool{}
var metricsSent = &atomic.Bool{}

const metricsDisabledEnvKey = "DOLT_METRICS_DISABLED"

func init() {
	if _, disabled := os.LookupEnv(metricsDisabledEnvKey); disabled {
		metricsDisabled.Store(true)
	}
}

// emitUsageEvent emits a usage event to the event server, then one every 24 hours the process is alive.
// This happens once per process.
func emitUsageEvent(ctx context.Context, mrEnv *env.MultiRepoEnv) {
	defer func() {
		recover()
	}()

	if metricsDisabled.Load() || !metricsSent.CompareAndSwap(false, true) {
		return
	}

	var dEnv *env.DoltEnv
	mrEnv.Iter(func(name string, d *env.DoltEnv) (stop bool, err error) {
		dEnv = d
		return true, nil
	})

	// no dolt db created yet, which means we can't create a GRPC dialer
	if dEnv == nil {
		return
	}

	dir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return
	}

	mtimeFile, tooSoon := tooSoonToEmitMetrics(dir)
	if tooSoon {
		return
	}

	emitter, closeFunc, err := commands.GRPCEmitterForConfig(dEnv, events.WithApplication(eventsapi.AppID_APP_DOLT_EMBEDDED))
	if err != nil {
		return
	}
	defer closeFunc()

	evt := events.NewEvent(eventsapi.ClientEventType_SQL_SERVER)
	evtCollector := events.NewCollector(doltversion.Version, emitter)
	evtCollector.CloseEventAndAdd(evt)
	clientEvents := evtCollector.Close()
	_ = emitter.LogEvents(ctx, doltversion.Version, clientEvents)

	// update the last modified time
	_ = os.Chtimes(mtimeFile, time.Now(), time.Now())
}

const metricsInterval = time.Hour * 24

// tooSoonToEmitMetrics checks if it's been less than 24 hours since the last metrics event was emitted, by checking
// the mod time of a file in the given directory.
// Returns the path to the file used for tracking mod time, and whether it's too soon to emit metrics.
func tooSoonToEmitMetrics(dir string) (string, bool) {
	mtimeFile := filepath.Join(dir, "dolt_embedded_metrics")
	f, err := os.OpenFile(mtimeFile, os.O_CREATE, 0644)
	if err != nil {
		return "", true
	}

	info, err := f.Stat()
	if err != nil {
		return "", true
	}

	if time.Now().Sub(info.ModTime()) < metricsInterval {
		return "", true
	}
	return mtimeFile, false
}
