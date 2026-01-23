package embedded

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

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
	RetryParam            = "retry"            // true|false (default false)
	RetryTimeoutParam     = "retrytimeout"     // e.g. "2s" (default 2s)
	RetryMaxAttemptsParam = "retrymaxattempts" // e.g. "10" (default 10)
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

	start := time.Now()
	attempt := 0
	delay := rp.InitialDelay
	if delay <= 0 {
		delay = 10 * time.Millisecond
	}

	var se *engine.SqlEngine
	var gmsCtx *gmssql.Context
	for {
		attempt++

		se, gmsCtx, _, err = openEmbeddedEngine(ctx, ds)
		if err == nil {
			break
		}

		terr := translateIfNeeded(err)
		if !(rp.Enabled && (isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr))) {
			return nil, err
		}

		if rp.MaxAttempts > 0 && attempt >= rp.MaxAttempts {
			return nil, err
		}
		if rp.Timeout > 0 && time.Since(start) >= rp.Timeout {
			return nil, err
		}

		sleep := delay
		if rp.MaxDelay > 0 && sleep > rp.MaxDelay {
			sleep = rp.MaxDelay
		}
		if rp.Timeout > 0 {
			remaining := rp.Timeout - time.Since(start)
			if remaining <= 0 {
				return nil, err
			}
			if sleep > remaining {
				sleep = remaining
			}
		}
		sleep = jitterDuration(sleep)
		time.Sleep(sleep)
	}

	return &DoltConn{
		DataSource:   ds,
		se:           se,
		gmsCtx:       gmsCtx,
		retryPolicy:  rp,
	}, nil
}
