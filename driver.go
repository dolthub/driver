package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/nbs"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

const (
	DoltDriverName = "dolt"

	CommitNameParam      = "commitname"
	CommitEmailParam     = "commitemail"
	DatabaseParam        = "database"
	MultiStatementsParam = "multistatements"
	ClientFoundRowsParam = "clientfoundrows"

	// The following params are passed through to Dolt's local DB loading layer via
	// engine.SqlEngineConfig.DBLoadParams. They are presence-based flags (values are ignored).
	DisableSingletonCacheParam    = "disable_singleton_cache"
	FailOnJournalLockTimeoutParam = "fail_on_journal_lock_timeout"
)

var _ driver.Driver = (*doltDriver)(nil)
var _ driver.DriverContext = (*doltDriver)(nil)

func init() {
	sql.Register(DoltDriverName, &doltDriver{})
}

// doltDriver is a driver.Driver implementation which provides access to a dolt database on the local filesystem
type doltDriver struct {
}

// Open opens and returns a connection to the datasource referenced by the string provided using the options provided.
// datasources must be in file url format:
//
//	file:///User/brian/driver/example/path?commitname=Billy%20Bob&commitemail=bb@gmail.com&database=dbname
//
// The path needs to point to a directory whose subdirectories are dolt databases.  If a "Create Database" command is
// run a new subdirectory will be created in this path.
func (d *doltDriver) Open(dsn string) (driver.Conn, error) {
	return nil, errors.New("dolt SQL driver does not support Open()")
}

func (d *doltDriver) OpenConnector(dsn string) (driver.Connector, error) {
	var fs filesys.Filesys = filesys.LocalFS

	ds, err := ParseDataSource(dsn)
	if err != nil {
		return nil, err
	}

	exists, isDir := fs.Exists(ds.Directory)
	if !exists {
		return nil, fmt.Errorf("'%s' does not exist", ds.Directory)
	} else if !isDir {
		return nil, fmt.Errorf("%s: is a file.  Need to specify a directory", ds.Directory)
	}

	fs, err = fs.WithWorkingDir(ds.Directory)
	if err != nil {
		return nil, err
	}

	name := ds.Params[CommitNameParam]
	if name == nil {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s'", dsn, CommitNameParam)
	}

	email := ds.Params[CommitEmailParam]
	if email == nil {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s'", dsn, CommitEmailParam)
	}

	cfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  name[0],
		config.UserEmailKey: email[0],
	})

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	// Optional embedded-driver flags that influence low-level storage open behavior.
	// These are used to support higher-level retry policies when a database cannot
	// acquire its exclusive lock.
	if _, ok := ds.Params[DisableSingletonCacheParam]; ok {
		if seCfg.DBLoadParams == nil {
			seCfg.DBLoadParams = make(map[string]interface{})
		}
		seCfg.DBLoadParams[dbfactory.DisableSingletonCacheParam] = struct{}{}
	}
	if _, ok := ds.Params[FailOnJournalLockTimeoutParam]; ok {
		if seCfg.DBLoadParams == nil {
			seCfg.DBLoadParams = make(map[string]interface{})
		}
		seCfg.DBLoadParams[dbfactory.FailOnJournalLockTimeoutParam] = struct{}{}
	}

	var database string
	if databases, ok := ds.Params[DatabaseParam]; ok && len(databases) == 1 {
		database = databases[0]
	}

	return &doltConnector{
		DataSource: ds,
		fs:         fs,
		cfg:        cfg,
		dir:        ds.Directory,
		version:    "0.40.17",
		seCfg:      seCfg,
		database: database,
		driver: d,
	}, nil
}

type doltConnector struct {
	DataSource *DoltDataSource
	gmsCtx     *gmssql.Context
	se         *engine.SqlEngine
	openEngineFunc func(context.Context) (*engine.SqlEngine, error)
	fs         filesys.Filesys
	cfg        config.ReadWriteConfig
	dir        string
	version    string
	seCfg      *engine.SqlEngineConfig
	database   string
	driver     *doltDriver

	mu         sync.Mutex
	opening    chan struct{}
	openCancel context.CancelFunc
}

func (dc *doltConnector) openEngine(ctx context.Context) (*engine.SqlEngine, error) {
	if dc.openEngineFunc != nil {
		return dc.openEngineFunc(ctx)
	}
	mrEnv, err := LoadMultiEnvFromDir(ctx, dc.cfg, dc.fs, dc.dir, dc.version)
	if err != nil {
		return nil, err
	}

	se, err := engine.NewSqlEngine(ctx, mrEnv, dc.seCfg)
	if err != nil {
		return nil, err
	}

	return se, nil
}

func (dc *doltConnector) openEngineWithRetry(ctx context.Context) (*engine.SqlEngine, error) {
	var se *engine.SqlEngine
	op := func() error {
		s, err := dc.openEngine(ctx)
		if err == nil {
			se = s
			return nil
		}
		// Don't retry on caller cancellation / deadline.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return backoff.Permanent(err)
		}
		if errors.Is(err, nbs.ErrDatabaseLocked) {
			return err
		}
		// Some Dolt / filesystem code paths may surface timeouts as os.ErrDeadlineExceeded.
		// Treat these as retryable, matching retry behavior used elsewhere in our stack.
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return err
		}
		return backoff.Permanent(err)
	}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = 50 * time.Millisecond
	bo.MaxInterval = 2 * time.Second
	bo.Multiplier = 2.0
	bo.RandomizationFactor = 0.2
	// MaxElapsedTime is intentionally left unset here; ctx controls overall retry duration.
	boCtx := backoff.WithContext(bo, ctx)

	if err := backoff.Retry(op, boCtx); err != nil {
		return nil, err
	}
	return se, nil
}

func (dc *doltConnector) ensureEngine(ctx context.Context) (*engine.SqlEngine, error) {
	for {
		dc.mu.Lock()
		if dc.se != nil {
			se := dc.se
			dc.mu.Unlock()
			return se, nil
		}

		if dc.opening != nil {
			ch := dc.opening
			dc.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		ch := make(chan struct{})
		dc.opening = ch
		openCtx, cancel := context.WithCancel(ctx)
		dc.openCancel = cancel
		dc.mu.Unlock()

		se, err := dc.openEngineWithRetry(openCtx)

		dc.mu.Lock()
		if err == nil {
			dc.se = se
		}
		close(ch)
		dc.opening = nil
		dc.openCancel = nil
		dc.mu.Unlock()

		if err != nil {
			return nil, err
		}
		return se, nil
	}
}

func (dc *doltConnector) Connect(ctx context.Context) (driver.Conn, error) {
	se, err := dc.ensureEngine(ctx)
	if err != nil {
		return nil, err
	}

	gmsCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		return nil, err
	}
	if dc.database != "" {
		gmsCtx.SetCurrentDatabase(dc.database)
	}
	if dc.DataSource.ParamIsTrue(ClientFoundRowsParam) {
		client := gmsCtx.Client()
		gmsCtx.SetClient(gmssql.Client{
			User:         client.User,
			Address:      client.Address,
			Capabilities: client.Capabilities | mysql.CapabilityClientFoundRows,
		})
	}
	return &DoltConn{
		DataSource: dc.DataSource,
		se:         se,
		gmsCtx:     gmsCtx,
	}, nil
}

func (dc *doltConnector) Driver() driver.Driver {
	return dc.driver
}

func (dc *doltConnector) Close() error {
	dc.mu.Lock()
	ch := dc.opening
	cancel := dc.openCancel
	dc.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if ch != nil {
		<-ch
	}

	dc.mu.Lock()
	se := dc.se
	dc.se = nil
	dc.mu.Unlock()

	if se == nil {
		return nil
	}
	return se.Close()
}

// LoadMultiEnvFromDir looks at each subfolder of the given path as a Dolt repository and attempts to return a MultiRepoEnv
// with initialized environments for each of those subfolder data repositories. subfolders whose name starts with '.' are
// skipped.
func LoadMultiEnvFromDir(
	ctx context.Context,
	cfg config.ReadWriteConfig,
	fs filesys.Filesys,
	path, version string,
) (*env.MultiRepoEnv, error) {

	multiDbDirFs, err := fs.WithWorkingDir(path)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	return env.MultiEnvForDirectory(ctx, cfg, multiDbDirFs, version, nil)
}
