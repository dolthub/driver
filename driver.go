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

	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/doltversion"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	gmssql "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/google/uuid"
)

const (
	DoltDriverName = "dolt"

	CommitNameParam      = "commitname"
	CommitEmailParam     = "commitemail"
	DatabaseParam        = "database"
	MultiStatementsParam = "multistatements"
	ClientFoundRowsParam = "clientfoundrows"

	doltVersion = "0.40.17"
)

var _ driver.Driver = (*doltDriver)(nil)
var _ driver.DriverContext = (*doltDriver)(nil)

var errConnectorClosed = errors.New("dolt connector is closed")

var newSqlEngine = engine.NewSqlEngine
var closeSqlEngine = func(se *engine.SqlEngine) error { return se.Close() }

func init() {
	sql.Register(DoltDriverName, &doltDriver{})
}

// doltDriver is a driver.Driver implementation which provides access to a dolt database on the local filesystem
type doltDriver struct {
}

// legacyConn wraps a DoltConn created via a one-off Connector in doltDriver.Open.
// This ensures resources (the shared SqlEngine) are released when the connection is closed.
type legacyConn struct {
	*DoltConn
	closeConnector func() error
}

var _ driver.Conn = (*legacyConn)(nil)

func (c *legacyConn) Close() error {
	// DoltConn.Close intentionally does not close the engine (pool-safe).
	// For the legacy Open() path, we created a private connector, so we close it here.
	if c.closeConnector == nil {
		return nil
	}
	err := c.closeConnector()
	if err != context.Canceled {
		return err
	}
	return nil
}

type doltConnector struct {
	driver *doltDriver

	ds    *DoltDataSource
	fs    filesys.Filesys
	cfg   config.ReadWriteConfig
	seCfg *engine.SqlEngineConfig

	mu     sync.Mutex
	closed bool
	seInit bool
	se     *engine.SqlEngine
	seErr  error
}

var _ driver.Connector = (*doltConnector)(nil)

func (c *doltConnector) Connect(ctx context.Context) (driver.Conn, error) {
	se, err := c.sqlEngine()
	if err != nil {
		return nil, err
	}

	gmsCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		return nil, err
	}
	if database, ok := c.ds.Params[DatabaseParam]; ok && len(database) > 0 {
		// OpenConnector validates that if DatabaseParam is present, it has exactly one value.
		gmsCtx.SetCurrentDatabase(database[0])
	}
	if c.ds.ParamIsTrue(ClientFoundRowsParam) {
		client := gmsCtx.Client()
		gmsCtx.SetClient(gmssql.Client{
			User:         client.User,
			Address:      client.Address,
			Capabilities: client.Capabilities | mysql.CapabilityClientFoundRows,
		})
	}

	return &DoltConn{
		DataSource: c.ds,
		se:         se,
		gmsCtx:     gmsCtx,
	}, nil
}

func (c *doltConnector) Driver() driver.Driver {
	return c.driver
}

// Close releases resources held by this Connector. When used via database/sql, this is called by (*sql.DB).Close.
func (c *doltConnector) Close() error {
	var se *engine.SqlEngine

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	// Only close the engine if it was successfully initialized.
	if c.seInit && c.seErr == nil {
		se = c.se
	}
	c.mu.Unlock()

	if se == nil {
		return nil
	}

	err := closeSqlEngine(se)
	if err != context.Canceled {
		return err
	}
	return nil
}

func (c *doltConnector) sqlEngine() (*engine.SqlEngine, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, errConnectorClosed
	}
	if c.seInit {
		return c.se, c.seErr
	}
	c.seInit = true

	// Use Background here so a canceled Connect ctx doesn't permanently poison engine init.
	// Per-connection contexts still apply to queries via gmsCtx.
	initCtx := context.Background()

	mrEnv, err := LoadMultiEnvFromDir(initCtx, c.cfg, c.fs, c.ds.Directory, doltVersion)
	if err != nil {
		c.seErr = err
		return nil, err
	}

	c.se, c.seErr = newSqlEngine(initCtx, mrEnv, c.seCfg)

	var dEnv *env.DoltEnv
	mrEnv.Iter(func(_ string, e *env.DoltEnv) (stop bool, err error) {
		dEnv = e
		return true, nil
	})

	// emit usage event asynchronously
	go emitUsageEvent(context.Background(), dEnv)

	return c.se, c.seErr
}

// OpenConnector implements driver.DriverContext. It parses and validates the DSN once, and returns a Connector
// that can create individual connections as needed by database/sql.
func (d *doltDriver) OpenConnector(dataSource string) (driver.Connector, error) {
	ds, err := ParseDataSource(dataSource)
	if err != nil {
		return nil, err
	}

	// Validate required parameters up-front so sql.Open can fail fast.
	name := ds.Params[CommitNameParam]
	if name == nil || len(name) != 1 {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s'", dataSource, CommitNameParam)
	}

	email := ds.Params[CommitEmailParam]
	if email == nil || len(email) != 1 {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s'", dataSource, CommitEmailParam)
	}

	if database, ok := ds.Params[DatabaseParam]; ok && len(database) != 1 {
		return nil, fmt.Errorf("datasource '%s' must include the parameter '%s' at most once", dataSource, DatabaseParam)
	}

	// Validate the directory exists and is a directory.
	var fs filesys.Filesys = filesys.LocalFS
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

	cfg := config.NewMapConfig(map[string]string{
		config.UserNameKey:  name[0],
		config.UserEmailKey: email[0],
	})

	seCfg := &engine.SqlEngineConfig{
		IsReadOnly: false,
		ServerUser: "root",
		Autocommit: true,
	}

	c := &doltConnector{
		driver: d,
		ds:     ds,
		fs:     fs,
		cfg:    cfg,
		seCfg:  seCfg,
	}

	// Eagerly initialize the shared engine so sql.Open fails fast on initialization errors.
	if _, err := c.sqlEngine(); err != nil {
		return nil, err
	}

	return c, nil
}

// Open opens and returns a connection to the datasource referenced by the string provided using the options provided.
// datasources must be in file url format:
//
//	file:///User/brian/driver/example/path?commitname=Billy%20Bob&commitemail=bb@gmail.com&database=dbname
//
// The path needs to point to a directory whose subdirectories are dolt databases.  If a "Create Database" command is
// run a new subdirectory will be created in this path.
func (d *doltDriver) Open(dataSource string) (driver.Conn, error) {
	// Legacy path: database/sql may call Driver.Open if it isn't using DriverContext.
	// Create a one-off connector and ensure it is closed when the connection is closed to avoid leaking resources.
	connector, err := d.OpenConnector(dataSource)
	if err != nil {
		return nil, err
	}

	conn, err := connector.Connect(context.Background())
	if err != nil {
		if c, ok := connector.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return nil, err
	}

	doltConn, ok := conn.(*DoltConn)
	if !ok {
		// Should never happen for this driver, but if it does, best effort cleanup.
		if c, ok := connector.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return nil, fmt.Errorf("internal error: unexpected connection type %T", conn)
	}

	if c, ok := connector.(interface{ Close() error }); ok {
		return &legacyConn{DoltConn: doltConn, closeConnector: c.Close}, nil
	}

	return doltConn, nil
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

var metricsDisabled = false
var metricsSent bool
var metricsSentSync sync.Once

func init() {
	if _, disabled := os.LookupEnv("DOLT_METRICS_DISABLED"); disabled {
		metricsDisabled = true
	}
}

// emitUsageEvent emits a usage event to the event server, then one every 24 hours the process is alive.
// This happens once per process.
func emitUsageEvent(ctx context.Context, dEnv *env.DoltEnv) {
	if metricsDisabled {
		return
	}

	metricsSentSync.Do(func() {
		metricsSent = true
	})

	if metricsSent {
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

	// Emit a heart-beat event once every 24 hours
	duration, _ := time.ParseDuration("24h")
	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t := events.NowTimestamp()
			_ = emitter.LogEvents(ctx, doltversion.Version, []*eventsapi.ClientEvent{
				{
					Id:        uuid.New().String(),
					StartTime: t,
					EndTime:   t,
					Type:      eventsapi.ClientEventType_SQL_SERVER_HEARTBEAT,
				},
			})
		}
	}
}
