package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

func sleepWithCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func execCtx(ctx context.Context, db *sql.DB, q string) error {
	_, err := db.ExecContext(ctx, q)
	return err
}

func ident(s string) string {
	// Minimal identifier safety: allow [A-Za-z0-9_], else return as-is and let SQL error.
	// (We avoid backticks here since Dolt/MySQL should accept plain identifiers.)
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		default:
			return s
		}
	}
	return s
}

func classifyErr(err error) errInfo {
	if err == nil {
		return errInfo{Kind: "other", Retryable: false, Message: ""}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errInfo{Kind: "timeout", Retryable: true, Message: err.Error()}
	}
	msg := err.Error()
	l := strings.ToLower(msg)

	// Embedded dolt frequently surfaces these strings for process-level lock contention.
	if strings.Contains(l, "database is locked") ||
		strings.Contains(l, "locked by another dolt process") ||
		strings.Contains(l, "cannot update manifest: database is read only") {
		return errInfo{Kind: "lock", Retryable: true, Message: msg}
	}

	// SQL-ish errors (not necessarily retryable).
	if strings.Contains(l, "error ") || strings.Contains(l, "sql") {
		return errInfo{Kind: "sql", Retryable: false, Message: msg}
	}

	return errInfo{Kind: "other", Retryable: false, Message: msg}
}

func doOneOp(ctx context.Context, db *sql.DB, cfg config) error {
	switch cfg.Role {
	case "writer":
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO %s (worker_id, ts) VALUES (?, ?)", ident(cfg.TableName)),
			cfg.WorkerID,
			time.Now().UnixMilli(),
		)
		return err
	case "reader":
		var n int64
		return db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", ident(cfg.TableName))).Scan(&n)
	default:
		return fmt.Errorf("unknown role %q", cfg.Role)
	}
}
