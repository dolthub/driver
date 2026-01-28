package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strconv"
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

	// OpenConnector retry params (used when opening the embedded engine).
	// These are intended for use-cases where the DB's exclusive lock may be contended
	// and the caller wants bounded retries during OpenConnector.
	OpenRetryParam          = "open_retry"
	OpenRetryMaxElapsed     = "open_retry_max_elapsed"
	OpenRetryInitialBackoff = "open_retry_initial"
	OpenRetryMaxInterval    = "open_retry_max_interval"
	OpenRetryMaxTries       = "open_retry_max_tries"
)

var _ driver.Driver = (*doltDriver)(nil)
var _ driver.DriverContext = (*doltDriver)(nil)

func init() {
	sql.Register(DoltDriverName, &doltDriver{})
}

// doltDriver is a driver.Driver implementation which provides access to a dolt database on the local filesystem
type doltDriver struct {
}

// openSqlEngineForConnector exists to make OpenConnector retry behavior testable without
// needing to take actual filesystem locks. Production code should leave this nil.
var openSqlEngineForConnector func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error)

func openSqlEngine(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
	if openSqlEngineForConnector != nil {
		return openSqlEngineForConnector(ctx, cfg, fs, dir, version, seCfg)
	}
	mrEnv, err := LoadMultiEnvFromDir(ctx, cfg, fs, dir, version)
	if err != nil {
		return nil, err
	}
	return engine.NewSqlEngine(ctx, mrEnv, seCfg)
}

type openRetryConfig struct {
	enabled    bool
	maxElapsed time.Duration
	initial    time.Duration
	maxInterval time.Duration
	maxTries   int // 0 means unlimited (bounded by maxElapsed)
}

func defaultOpenRetryConfig() openRetryConfig {
	return openRetryConfig{
		enabled:     false,
		maxElapsed:  2 * time.Second,
		initial:     50 * time.Millisecond,
		maxInterval: 2 * time.Second,
		maxTries:    0,
	}
}

func parseBoolParam(ds *DoltDataSource, param string) (val bool, ok bool, err error) {
	values, ok := ds.Params[param]
	if !ok || len(values) == 0 {
		return false, false, nil
	}
	// Treat presence without value as true.
	if len(values) == 1 && values[0] == "" {
		return true, true, nil
	}
	if len(values) != 1 {
		return false, true, fmt.Errorf("param %q must have exactly one value", param)
	}
	switch v := values[0]; v {
	case "true", "TRUE", "True":
		return true, true, nil
	case "false", "FALSE", "False":
		return false, true, nil
	default:
		return false, true, fmt.Errorf("param %q must be true or false, got %q", param, v)
	}
}

func parseDurationParam(ds *DoltDataSource, param string) (dur time.Duration, ok bool, err error) {
	values, ok := ds.Params[param]
	if !ok || len(values) == 0 {
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, true, fmt.Errorf("param %q must have exactly one value", param)
	}
	d, err := time.ParseDuration(values[0])
	if err != nil {
		return 0, true, fmt.Errorf("param %q must be a duration (e.g. 250ms, 2s), got %q: %w", param, values[0], err)
	}
	return d, true, nil
}

func parseIntParam(ds *DoltDataSource, param string) (n int, ok bool, err error) {
	values, ok := ds.Params[param]
	if !ok || len(values) == 0 {
		return 0, false, nil
	}
	if len(values) != 1 {
		return 0, true, fmt.Errorf("param %q must have exactly one value", param)
	}
	i, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, true, fmt.Errorf("param %q must be an integer, got %q: %w", param, values[0], err)
	}
	return i, true, nil
}

func parseOpenRetryConfig(ds *DoltDataSource) (openRetryConfig, error) {
	cfg := defaultOpenRetryConfig()

	if v, ok, err := parseBoolParam(ds, OpenRetryParam); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.enabled = v
	}

	if d, ok, err := parseDurationParam(ds, OpenRetryMaxElapsed); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.maxElapsed = d
	}

	if d, ok, err := parseDurationParam(ds, OpenRetryInitialBackoff); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.initial = d
	}

	if d, ok, err := parseDurationParam(ds, OpenRetryMaxInterval); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.maxInterval = d
	}

	if n, ok, err := parseIntParam(ds, OpenRetryMaxTries); err != nil {
		return openRetryConfig{}, err
	} else if ok {
		cfg.maxTries = n
	}

	if cfg.maxElapsed < 0 || cfg.initial < 0 || cfg.maxInterval < 0 {
		return openRetryConfig{}, fmt.Errorf("open retry durations must be non-negative")
	}
	if cfg.maxTries < 0 {
		return openRetryConfig{}, fmt.Errorf("param %q must be non-negative", OpenRetryMaxTries)
	}
	if cfg.maxInterval > 0 && cfg.initial > cfg.maxInterval {
		return openRetryConfig{}, fmt.Errorf("param %q must be <= %q", OpenRetryInitialBackoff, OpenRetryMaxInterval)
	}

	return cfg, nil
}

func isRetryableOpenErr(err error) bool {
	if err == nil {
		return false
	}
	// The intended primary retry signal for embedded usage: DB couldn't acquire its exclusive lock.
	if errors.Is(err, nbs.ErrDatabaseLocked) {
		return true
	}
	// Some filesystem / lower-level paths can surface timeouts as os.ErrDeadlineExceeded.
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	return false
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
	ctx := context.Background()
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

	// Validate and record retry configuration (used in a later step when moving retries into OpenConnector).
	openRetryCfg, err := parseOpenRetryConfig(ds)
	if err != nil {
		return nil, err
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

	// Open the engine eagerly (OpenConnector is not context-aware), optionally with bounded retries.
	version := "0.40.17"
	openEngine := func(openCtx context.Context) (*engine.SqlEngine, error) {
		return openSqlEngine(openCtx, cfg, fs, ds.Directory, version, seCfg)
	}

	var se *engine.SqlEngine
	if !openRetryCfg.enabled {
		se, err = openEngine(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		retryCtx := ctx
		var cancel context.CancelFunc
		if openRetryCfg.maxElapsed > 0 {
			retryCtx, cancel = context.WithTimeout(ctx, openRetryCfg.maxElapsed)
			defer cancel()
		}

		bo := backoff.NewExponentialBackOff()
		bo.InitialInterval = openRetryCfg.initial
		bo.MaxInterval = openRetryCfg.maxInterval
		bo.Multiplier = 2.0
		bo.RandomizationFactor = 0.2
		var boff backoff.BackOff = backoff.WithContext(bo, retryCtx)
		if openRetryCfg.maxTries > 0 {
			boff = backoff.WithMaxRetries(boff, uint64(openRetryCfg.maxTries))
		}

		var lastErr error
		op := func() error {
			s, err := openEngine(retryCtx)
			if err == nil {
				se = s
				return nil
			}
			lastErr = err
			// Retry only on lock / transient timeout conditions.
			if isRetryableOpenErr(err) {
				return err
			}
			return backoff.Permanent(err)
		}
		if err := backoff.Retry(op, boff); err != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
	}

	return &doltConnector{
		DataSource: ds,
		se:         se,
		database: database,
		driver: d,
	}, nil
}

type doltConnector struct {
	DataSource *DoltDataSource
	se         *engine.SqlEngine
	database   string
	driver     *doltDriver

	closeOnce sync.Once
	closeErr  error
}

func (dc *doltConnector) Connect(ctx context.Context) (driver.Conn, error) {
	se := dc.se
	if se == nil {
		return nil, errors.New("dolt connector has no engine (OpenConnector failed)")
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
	dc.closeOnce.Do(func() {
		if dc.se == nil {
			return
		}
		dc.closeErr = dc.se.Close()
		dc.se = nil
	})
	return dc.closeErr
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
