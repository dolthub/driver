package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	embedded "github.com/dolthub/driver"
)

func runSQL(ctx context.Context, cfg config, emit func(name string, fields any)) error {
	if cfg.DSN == "" {
		return fmt.Errorf("--dsn is required when --mode=sql")
	}
	if cfg.DBName == "" {
		return fmt.Errorf("--db is required when --mode=sql")
	}
	if cfg.TableName == "" {
		return fmt.Errorf("--table is required when --mode=sql")
	}
	if cfg.OpInterval <= 0 {
		return fmt.Errorf("--op-interval must be > 0")
	}
	if cfg.OpTimeout <= 0 {
		return fmt.Errorf("--op-timeout must be > 0")
	}

	if cfg.SQLSession != "per_op" && cfg.SQLSession != "per_run" {
		return fmt.Errorf("--sql-session must be per_op or per_run")
	}

	type stats struct {
		opsOK   int64
		opsErr  int64
		lastErr string
	}
	s := stats{}

	// Heartbeat loop (reports stats).
	hb := time.NewTicker(cfg.HeartbeatInterval)
	defer hb.Stop()

	// Operation loop.
	op := time.NewTicker(cfg.OpInterval)
	defer op.Stop()

	end := (<-chan time.Time)(nil)
	if cfg.Duration > 0 {
		t := time.NewTimer(cfg.Duration)
		defer t.Stop()
		end = t.C
	}

	start := time.Now()
	var db *sql.DB
	var closeDB func()
	if cfg.SQLSession == "per_run" {
		d, closer, err := openDB(ctx, cfg)
		if err != nil {
			return err
		}
		db = d
		closeDB = closer
		defer closeDB()
	}

	opID := int64(0)
	for {
		select {
		case <-op.C:
			opID++
			var err error
			if cfg.SQLSession == "per_op" {
				opCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
				err = doOneOpWithRetry(opCtx, cfg, opID, emit)
				cancel()
			} else {
				opCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
				err = doOneOpWithRetryWithDB(opCtx, db, cfg, opID, emit)
				cancel()
			}
			if err != nil {
				s.opsErr++
				s.lastErr = err.Error()
				ei := classifyErr(err)
				emit("op_error", map[string]any{"op_id": opID, "error": ei})
			} else {
				s.opsOK++
			}
		case <-hb.C:
			emit("heartbeat", map[string]any{
				"ops_ok":     s.opsOK,
				"ops_err":    s.opsErr,
				"last_error": s.lastErr,
				"elapsed_ms": time.Since(start).Milliseconds(),
			})
		case <-end:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func doOneOpWithRetry(opCtx context.Context, cfg config, opID int64, emit func(name string, fields any)) error {
	start := time.Now()
	attempt := 0
	backoffDelay := 5 * time.Millisecond
	for {
		attempt++
		d, closer, err := openDB(opCtx, cfg)
		if err != nil {
			ei := classifyErr(err)
			if cfg.OpenRetry && ei.Retryable && opCtx.Err() == nil {
				emit("open_retry", map[string]any{
					"op_id":      opID,
					"attempt":    attempt,
					"elapsed_ms": time.Since(start).Milliseconds(),
					"backoff_ms": backoffDelay.Milliseconds(),
					"last_error": ei,
				})
				sleepWithCtx(opCtx, backoffDelay)
				backoffDelay = minDur(backoffDelay*2, 200*time.Millisecond)
				continue
			}
			return err
		}

		err = doOneOp(opCtx, d, cfg)
		closer()
		if err == nil {
			emit("op_ok", map[string]any{
				"op_id":      opID,
				"attempt":    attempt,
				"elapsed_ms": time.Since(start).Milliseconds(),
			})
			return nil
		}

		ei := classifyErr(err)
		if ei.Retryable && opCtx.Err() == nil {
			emit("op_retry", map[string]any{
				"op_id":      opID,
				"attempt":    attempt,
				"elapsed_ms": time.Since(start).Milliseconds(),
				"backoff_ms": backoffDelay.Milliseconds(),
				"last_error": ei,
			})
			sleepWithCtx(opCtx, backoffDelay)
			backoffDelay = minDur(backoffDelay*2, 200*time.Millisecond)
			continue
		}
		return err
	}
}

func doOneOpWithRetryWithDB(opCtx context.Context, db *sql.DB, cfg config, opID int64, emit func(name string, fields any)) error {
	start := time.Now()
	attempt := 0
	backoffDelay := 5 * time.Millisecond
	for {
		attempt++
		err := doOneOp(opCtx, db, cfg)
		if err == nil {
			emit("op_ok", map[string]any{
				"op_id":      opID,
				"attempt":    attempt,
				"elapsed_ms": time.Since(start).Milliseconds(),
			})
			return nil
		}
		ei := classifyErr(err)
		if ei.Retryable && opCtx.Err() == nil {
			emit("op_retry", map[string]any{
				"op_id":      opID,
				"attempt":    attempt,
				"elapsed_ms": time.Since(start).Milliseconds(),
				"backoff_ms": backoffDelay.Milliseconds(),
				"last_error": ei,
			})
			sleepWithCtx(opCtx, backoffDelay)
			backoffDelay = minDur(backoffDelay*2, 200*time.Millisecond)
			continue
		}
		return err
	}
}

func openDB(ctx context.Context, cfg config) (*sql.DB, func(), error) {
	ecfg, err := embedded.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, nil, err
	}
	ecfg.Database = ""

	if cfg.OpenRetry {
		bo := backoff.NewExponentialBackOff()
		bo.MaxElapsedTime = 0 // bounded by ctx
		ecfg.BackOff = bo
	}

	connector, err := embedded.NewConnector(ecfg)
	if err != nil {
		return nil, nil, err
	}

	db := sql.OpenDB(connector)
	closeFn := func() {
		_ = db.Close()
		_ = connector.Close()
	}

	// Select DB (created by orchestrator).
	if err := execCtx(ctx, db, fmt.Sprintf("USE %s", ident(cfg.DBName))); err != nil {
		closeFn()
		return nil, nil, err
	}
	return db, closeFn, nil
}
