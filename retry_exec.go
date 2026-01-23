package embedded

import (
	"context"
	"database/sql/driver"
	"math"
	"math/rand"
	"time"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	gms "github.com/dolthub/go-mysql-server/sql"
)

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

	start := time.Now()
	attempt := 0
	delay := p.InitialDelay
	if delay <= 0 {
		delay = 10 * time.Millisecond
	}

	var lastErr error
	for {
		attempt++

		// Respect context cancellation.
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, nil, nil, lastErr
			}
			return nil, nil, nil, ctx.Err()
		default:
		}

		se, gmsCtx := d.getEngineAndContext()
		if gmsCtx != nil {
			gmsCtx.SetQueryTime(time.Now())
		}
		sch, itr, err := op(se, gmsCtx)
		if err == nil {
			return sch, itr, gmsCtx, nil
		}

		// Retry classification on both the raw error and the translated MySQL error.
		terr := translateIfNeeded(err)
		if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
			return nil, nil, nil, err
		}
		lastErr = err

		// Limits
		if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
			return nil, nil, nil, lastErr
		}
		if p.Timeout > 0 && time.Since(start) >= p.Timeout {
			return nil, nil, nil, lastErr
		}

		// Backoff sleep (bounded by timeout remaining).
		sleep := delay
		if p.MaxDelay > 0 && sleep > p.MaxDelay {
			sleep = p.MaxDelay
		}
		if p.Timeout > 0 {
			remaining := p.Timeout - time.Since(start)
			if remaining <= 0 {
				return nil, nil, nil, lastErr
			}
			if sleep > remaining {
				sleep = remaining
			}
		}
		sleep = jitterDuration(sleep)

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, nil, lastErr
		case <-timer.C:
		}

		// Reopen engine before retry to avoid a "stuck read-only" instance.
		// If reopen fails, treat that as the new error and continue retrying if it is retryable.
		if err := d.reopenEngine(ctx); err != nil {
			lastErr = err
			terr := translateIfNeeded(err)
			if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
				return nil, nil, nil, err
			}
		}

		// Exponential backoff.
		if p.MaxDelay > 0 {
			next := time.Duration(float64(delay) * 2)
			if next > p.MaxDelay {
				next = p.MaxDelay
			}
			delay = next
		} else {
			// Avoid overflow.
			delay = time.Duration(math.Min(float64(delay)*2, float64(2*time.Second)))
		}
	}
}

func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// 0.5x .. 1.5x jitter
	f := 0.5 + rand.Float64()
	return time.Duration(float64(d) * f)
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

	start := time.Now()
	attempt := 0
	delay := p.InitialDelay
	if delay <= 0 {
		delay = 10 * time.Millisecond
	}

	var lastErr error
	for {
		attempt++

		select {
		case <-ctx.Done():
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, ctx.Err()
		default:
		}

		se, gmsCtx := d.getEngineAndContext()
		if gmsCtx != nil {
			gmsCtx.SetQueryTime(time.Now())
		}
		res, err := op(se, gmsCtx)
		if err == nil {
			return res, nil
		}

		terr := translateIfNeeded(err)
		if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
			return nil, err
		}
		lastErr = err

		if p.MaxAttempts > 0 && attempt >= p.MaxAttempts {
			return nil, lastErr
		}
		if p.Timeout > 0 && time.Since(start) >= p.Timeout {
			return nil, lastErr
		}

		sleep := delay
		if p.MaxDelay > 0 && sleep > p.MaxDelay {
			sleep = p.MaxDelay
		}
		if p.Timeout > 0 {
			remaining := p.Timeout - time.Since(start)
			if remaining <= 0 {
				return nil, lastErr
			}
			if sleep > remaining {
				sleep = remaining
			}
		}
		sleep = jitterDuration(sleep)

		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, lastErr
		case <-timer.C:
		}

		if err := d.reopenEngine(ctx); err != nil {
			lastErr = err
			terr := translateIfNeeded(err)
			if !(isRetryableEmbeddedErr(err) || isRetryableEmbeddedErr(terr)) {
				return nil, err
			}
		}

		if p.MaxDelay > 0 {
			next := time.Duration(float64(delay) * 2)
			if next > p.MaxDelay {
				next = p.MaxDelay
			}
			delay = next
		} else {
			delay = time.Duration(math.Min(float64(delay)*2, float64(2*time.Second)))
		}
	}
}

