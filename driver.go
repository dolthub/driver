package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gmssql "github.com/dolthub/go-mysql-server/sql"
)

const (
	DoltDriverName = "dolt"

	CommitNameParam      = "commitname"
	CommitEmailParam     = "commitemail"
	DatabaseParam        = "database"
	MultiStatementsParam = "multistatements"
	ClientFoundRowsParam = "clientfoundrows"

	// Dolt embedded mode tuning
	NoCacheParam           = "nocache"           // disable Dolt in-process singleton DB cache (default false)
	FailOnLockTimeoutParam = "failonlocktimeout" // fail-fast on journal lock timeout vs read-only fallback (default false)

	// Retry policy parameters (embedded mode contention handling)
	RetryParam             = "retry"             // true|false (default false)
	RetryTimeoutParam      = "retrytimeout"      // e.g. "2s" (default 2s)
	RetryMaxAttemptsParam  = "retrymaxattempts"  // e.g. "10" (default 10)
	RetryInitialDelayParam = "retryinitialdelay" // e.g. "25ms" (default 25ms)
	RetryMaxDelayParam     = "retrymaxdelay"     // e.g. "250ms" (default 250ms)
)

var _ driver.Driver = (*doltDriver)(nil)

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
func (d *doltDriver) Open(dataSource string) (driver.Conn, error) {
	ctx := context.Background()

	ds, err := ParseDataSource(dataSource)
	if err != nil {
		return nil, err
	}

	// Parse retry policy up-front so we can apply it to open-time contention failures.
	rp, err := ParseRetryPolicy(ds)
	if err != nil {
		return nil, err
	}

	var se *engine.SqlEngine
	var gmsCtx *gmssql.Context
	var cleanup func()
	var lastErr error

	if rp.Enabled {
		bo := newRetryBackOff(ctx, rp)
		err = backoff.Retry(func() error {
			se, gmsCtx, _, cleanup, err = openEmbeddedEngine(ctx, ds)
			if err == nil {
				lastErr = nil
				return nil
			}

			terr := translateIfNeeded(err)
			if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
				lastErr = err
				return backoff.Permanent(err)
			}

			lastErr = err
			return err
		}, bo)
		if err != nil {
			if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && lastErr != nil {
				return nil, lastErr
			}
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, err
		}
	} else {
		se, gmsCtx, _, cleanup, err = openEmbeddedEngine(ctx, ds)
		if err != nil {
			return nil, err
		}
	}

	return &DoltConn{
		DataSource:  ds,
		se:          se,
		gmsCtx:      gmsCtx,
		retryPolicy: rp,
		cleanup:     cleanup,
	}, nil
}
