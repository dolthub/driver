package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func defaultPolicy() gatingPolicy {
	return gatingPolicy{
		SetupFailure:    "attempt_teardown_then_fail",
		RunFailure:      "attempt_teardown_then_fail",
		TeardownFailure: "fail",
	}
}

func runPhases(ctx context.Context, cfg harnessConfig, runID, runPath string, emit func(phase, name string, fields any)) (int, map[string]phaseMeta) {
	// Policy:
	// - setup failure: do not enter run; attempt teardown best-effort; then fail
	// - run failure: attempt teardown best-effort; then fail
	// - teardown failure: fail

	phases := map[string]phaseMeta{}
	manifest := newWorkerManifest(cfg.Seed, cfg.Readers, cfg.Writers)

	setupErr, setupMeta := runPhase(ctx, "setup", cfg.SetupTimeout, emit, func(pctx context.Context) error {
		if !cfg.DryRun && cfg.WorkerMode == "sql" {
			if err := ensureSQLSchema(pctx, cfg); err != nil {
				emit("setup", "sql_schema_error", map[string]any{"error": err.Error()})
				return err
			}
			emit("setup", "sql_schema_ready", map[string]any{"db": cfg.WorkerDB, "table": cfg.WorkerTable})
		}
		emit("setup", "manifest", manifest)
		return sleepWithContext(pctx, cfg.SetupDelay)
	})
	phases["setup"] = setupMeta

	var runErr error
	var runMeta phaseMeta
	if setupErr == nil {
		runErr, runMeta = runPhase(ctx, "run", cfg.RunTimeout, emit, func(pctx context.Context) error {
			if cfg.DryRun {
				return runWithTicks(pctx, cfg.Duration, cfg.TickInterval, emit)
			}
			return runWorkersAndTicks(pctx, cfg, runID, runPath, manifest, emit)
		})
		phases["run"] = runMeta
	}

	// Teardown must still be able to run even if ctx is canceled (e.g. SIGINT).
	teardownErr, teardownMeta := runPhase(context.Background(), "teardown", cfg.TeardownTimeout, emit, func(pctx context.Context) error {
		return sleepWithContext(pctx, cfg.TeardownDelay)
	})
	phases["teardown"] = teardownMeta

	if setupErr != nil {
		return 1, phases
	}
	if runErr != nil {
		return 1, phases
	}
	if teardownErr != nil {
		return 1, phases
	}
	return 0, phases
}

func runPhase(
	ctx context.Context,
	phase string,
	timeout time.Duration,
	emit func(phase, name string, fields any),
	fn func(context.Context) error,
) (error, phaseMeta) {
	emit(phase, "phase_start", nil)

	start := time.Now().UTC()
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := fn(pctx)
	end := time.Now().UTC()

	endFields := phaseEndFields{
		OK:        err == nil,
		ElapsedMs: end.Sub(start).Milliseconds(),
	}
	if err != nil {
		endFields.Error = err.Error()
	}
	emit(phase, "phase_end", endFields)

	meta := phaseMeta{
		Start: start,
		End:   end,
		OK:    err == nil,
	}
	if err != nil {
		meta.Error = err.Error()
	}
	return err, meta
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func runWithTicks(ctx context.Context, duration, interval time.Duration, emit func(phase, name string, fields any)) error {
	if duration <= 0 {
		return nil
	}
	if interval <= 0 {
		interval = time.Second
	}

	start := time.Now()
	doneTimer := time.NewTimer(duration)
	ticker := time.NewTicker(interval)
	defer doneTimer.Stop()
	defer ticker.Stop()

	count := 0
	for {
		select {
		case <-ticker.C:
			count++
			emit("run", "tick", tickFields{
				Count:     count,
				ElapsedMs: time.Since(start).Milliseconds(),
			})
		case <-doneTimer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func runWithTicksOrWorkerExit(
	ctx context.Context,
	duration, interval time.Duration,
	emit func(phase, name string, fields any),
	exitNotify <-chan workerExit,
) error {
	if duration <= 0 {
		return nil
	}
	if interval <= 0 {
		interval = time.Second
	}

	start := time.Now()
	doneTimer := time.NewTimer(duration)
	ticker := time.NewTicker(interval)
	defer doneTimer.Stop()
	defer ticker.Stop()

	count := 0
	for {
		select {
		case we := <-exitNotify:
			emit("run", "worker_exit_early", we)
			if we.Error == "" {
				return fmt.Errorf("worker %s exited early", we.WorkerID)
			}
			return fmt.Errorf("worker %s exited early: %s", we.WorkerID, we.Error)
		case <-ticker.C:
			count++
			emit("run", "tick", tickFields{
				Count:     count,
				ElapsedMs: time.Since(start).Milliseconds(),
			})
		case <-doneTimer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func isTimeoutLike(phases map[string]phaseMeta, exitCode int) bool {
	// Only attempt diagnostics on failing runs.
	if exitCode == 0 {
		return false
	}
	for _, pm := range phases {
		if isTimeoutErrString(pm.Error) {
			return true
		}
	}
	return false
}

func isTimeoutErrString(s string) bool {
	if s == "" {
		return false
	}
	l := strings.ToLower(s)
	return strings.Contains(l, "deadline exceeded") ||
		strings.Contains(l, "timeout waiting") ||
		strings.Contains(l, "context deadline")
}
