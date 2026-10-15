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
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
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

// openSqlEngineForConnector exists to make OpenConnector retry behavior testable without
// needing to take actual filesystem locks. Production code should leave this nil.
var openSqlEngineForConnector func(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error)

func openSqlEngine(ctx context.Context, cfg config.ReadWriteConfig, fs filesys.Filesys, dir, version string, seCfg *engine.SqlEngineConfig) (*engine.SqlEngine, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A retry starts with a fresh bundle, and all bootstrap work for this open
	// uses the same bundle that NewSqlEngine will adopt.
	seCfg.EngineVariables = sqle.NewEngineVariables()

	if openSqlEngineForConnector != nil {
		return openSqlEngineForConnector(ctx, cfg, fs, dir, version, seCfg)
	}

	// fs already switched to dir, so passing "." as a path
	mrEnv, err := LoadMultiEnvFromDir(ctx, cfg, fs, ".", version, seCfg.DBLoadParams)
	if err != nil {
		return nil, err
	}
	if err := mrEnv.Iter(func(name string, _ *env.DoltEnv) (bool, error) {
		dsess.DefineSystemVariablesForDBInRegistry(seCfg.EngineVariables.System, name)
		return false, nil
	}); err != nil {
		mrEnv.Close(ctx)
		return nil, err
	}
	if dEnv := mrEnv.GetEnv(mrEnv.GetFirstDatabase()); dEnv != nil {
		if err := dsess.InitPersistedSystemVarsWithRegistry(seCfg.EngineVariables.System, dEnv); err != nil {
			mrEnv.Close(ctx)
			return nil, err
		}
	} else {
		// An empty data directory can still have persisted globals in its
		// root configuration, before any database has been created.
		persisted := config.NewPrefixConfig(mrEnv.Config(), env.SqlServerGlobalsPrefix)
		definitions, _, err := dsess.SystemVariablesInConfigWithRegistry(seCfg.EngineVariables.System, persisted)
		if err != nil {
			mrEnv.Close(ctx)
			return nil, err
		}
		seCfg.EngineVariables.System.AddSystemVariables(definitions)
	}

	go emitUsageEvent(context.Background(), mrEnv)
	se, err := engine.NewSqlEngine(ctx, mrEnv, seCfg)
	if err != nil {
		mrEnv.Close(ctx)
		return nil, err
	}
	return se, nil
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
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return NewConnector(cfg)
}

// LoadMultiEnvFromDir looks at each subfolder of the given path as a Dolt repository and attempts to return a MultiRepoEnv
// with initialized environments for each of those subfolder data repositories. subfolders whose name starts with '.' are
// skipped.
func LoadMultiEnvFromDir(
	ctx context.Context,
	cfg config.ReadWriteConfig,
	fs filesys.Filesys,
	path, version string,
	dbLoadParams map[string]any,
) (*env.MultiRepoEnv, error) {

	multiDbDirFs, err := fs.WithWorkingDir(path)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	return env.MultiEnvForConfigAndDirectory(ctx, cfg, multiDbDirFs, dbLoadParams)
}
