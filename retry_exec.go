package embedded

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

func newRetryBackOff(ctx context.Context, p RetryPolicy) backoff.BackOff {
	bo := backoff.NewExponentialBackOff()

	// Match previous defaults and semantics.
	if p.InitialDelay > 0 {
		bo.InitialInterval = p.InitialDelay
	} else {
		bo.InitialInterval = 10 * time.Millisecond
	}
	if p.MaxDelay > 0 {
		bo.MaxInterval = p.MaxDelay
	}
	if p.Timeout > 0 {
		bo.MaxElapsedTime = p.Timeout
	} else {
		bo.MaxElapsedTime = 0 // unlimited
	}
	// Approximate existing 0.5x..1.5x jitter.
	bo.RandomizationFactor = 0.5

	var out backoff.BackOff = bo
	if p.MaxAttempts > 0 {
		// MaxRetries is the number of retry attempts after the first try.
		out = backoff.WithMaxRetries(out, uint64(p.MaxAttempts-1))
	}

	return backoff.WithContext(out, ctx)
}

// runQueryWithRetry executes op and retries (with reopen + backoff) when enabled and the error is retryable.
// Returns the gms context used for the successful attempt (or last attempt).
func (d *DoltConn) runQueryWithRetry(
	ctx context.Context,
	op func(se *engine.SqlEngine, gmsCtx *gms.Context) (gms.Schema, gms.RowIter, error),
) (gms.Schema, gms.RowIter, *gms.Context, error) {
	p := d.getRetryPolicy()
	if !p.Enabled || d.inTransaction() {
		se, gmsCtx := d.getEngineAndContext()
		sch, itr, err := op(se, gmsCtx)
		return sch, itr, gmsCtx, err
	}

	bo := newRetryBackOff(ctx, p)

	var attempt int
	var lastErr error
	var lastCtx *gms.Context
	var sch gms.Schema
	var itr gms.RowIter

	err := backoff.Retry(func() error {
		attempt++

		// Reopen before retrying to avoid a "stuck read-only" instance.
		// (This runs on attempts 2..N; backoff sleeps between attempts.)
		if attempt > 1 {
			if err := d.reopenEngine(ctx); err != nil {
				terr := translateIfNeeded(err)
				if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
					lastErr = err
					return backoff.Permanent(err)
				}
				lastErr = err
				return err
			}
		}

		se, gmsCtx := d.getEngineAndContext()
		lastCtx = gmsCtx
		if gmsCtx != nil {
			gmsCtx.SetQueryTime(time.Now())
		}

		var e error
		sch, itr, e = op(se, gmsCtx)
		if e == nil {
			lastErr = nil
			return nil
		}

		terr := translateIfNeeded(e)
		if !(isRetryableEmbeddedErr(e) || isRetryableEmbeddedErr(terr)) {
			lastErr = e
			return backoff.Permanent(e)
		}

		lastErr = e
		return e
	}, bo)

	if err == nil {
		return sch, itr, lastCtx, nil
	}

	// Preserve previous behavior: if context is canceled after one or more failures, return the last error.
	if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && lastErr != nil {
		return nil, nil, nil, lastErr
	}
	if lastErr != nil {
		return nil, nil, nil, lastErr
	}
	return nil, nil, nil, err
}

func (d *DoltConn) runExecWithRetry(
	ctx context.Context,
	op func(se *engine.SqlEngine, gmsCtx *gms.Context) (driver.Result, error),
) (driver.Result, error) {
	p := d.getRetryPolicy()
	if !p.Enabled || d.inTransaction() {
		se, gmsCtx := d.getEngineAndContext()
		return op(se, gmsCtx)
	}

	bo := newRetryBackOff(ctx, p)

	var attempt int
	var lastErr error
	var res driver.Result

	err := backoff.Retry(func() error {
		attempt++

		if attempt > 1 {
			if err := d.reopenEngine(ctx); err != nil {
				terr := translateIfNeeded(err)
				if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
					lastErr = err
					return backoff.Permanent(err)
				}
				lastErr = err
				return err
			}
		}

		se, gmsCtx := d.getEngineAndContext()
		if gmsCtx != nil {
			gmsCtx.SetQueryTime(time.Now())
		}

		var e error
		res, e = op(se, gmsCtx)
		if e == nil {
			lastErr = nil
			return nil
		}

		terr := translateIfNeeded(e)
		if !(isRetryableEmbeddedErr(e) || isRetryableEmbeddedErr(terr)) {
			lastErr = e
			return backoff.Permanent(e)
		}

		lastErr = e
		return e
	}, bo)

	if err == nil {
		return res, nil
	}
	if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && lastErr != nil {
		return nil, lastErr
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, err
}
