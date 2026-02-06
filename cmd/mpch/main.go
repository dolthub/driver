package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfg, showHelp, err := parseArgs(os.Args[1:])
	if showHelp {
		os.Exit(0)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(2)
	}

	runID := cfg.RunID
	if runID == "" {
		runID, err = newRunID()
		if err != nil {
			fmt.Fprintln(os.Stderr, "failed to generate run id:", err)
			os.Exit(1)
		}
	}

	// If mpch is piped to a consumer like `head`, the consumer may exit early and close
	// the pipe. Ignore SIGPIPE so writes fail with EPIPE instead, which we treat as a
	// clean termination.
	signal.Ignore(syscall.SIGPIPE)

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)

	emit := func(phase, name string, fields any) {
		if err := enc.Encode(event{
			TS:     time.Now().UTC(),
			RunID:  runID,
			Phase:  phase,
			Event:  name,
			Fields: fields,
		}); err != nil {
			// If the consumer of stdout exits early (e.g. piped to `head`), treat EPIPE as
			// a clean termination: continuing would just spam stderr and exit non-zero.
			if isBrokenPipe(err) {
				os.Exit(0)
			}
			fmt.Fprintln(os.Stderr, "failed to write event:", err)
			os.Exit(1)
		}
	}

	emit("plan", "plan", planFields{
		DSN:                     cfg.DSN,
		Readers:                 cfg.Readers,
		Writers:                 cfg.Writers,
		Duration:                cfg.Duration.String(),
		TickInterval:            cfg.TickInterval.String(),
		Seed:                    cfg.Seed,
		RunDir:                  cfg.RunDir,
		DryRun:                  cfg.DryRun,
		RunIDOverride:           cfg.RunID,
		Overwrite:               cfg.Overwrite,
		WorkerBin:               cfg.WorkerBin,
		WorkerHeartbeatInterval: cfg.WorkerHeartbeatInterval.String(),
		WorkerShutdownGrace:     cfg.WorkerShutdownGrace.String(),
		WorkerShutdownKillWait:  cfg.WorkerShutdownKillWait.String(),
		WorkerIgnoreInterrupt:   cfg.WorkerIgnoreInterrupt,
		WorkerMode:              cfg.WorkerMode,
		WorkerDB:                cfg.WorkerDB,
		WorkerTable:             cfg.WorkerTable,
		WorkerOpInterval:        cfg.WorkerOpInterval.String(),
		WorkerOpenRetry:         cfg.WorkerOpenRetry,
		WorkerSQLSession:        cfg.WorkerSQLSession,
		WorkerOpTimeout:         cfg.WorkerOpTimeout.String(),
		DiagnosticsTailLines:    cfg.DiagnosticsTailLines,
		DumpStacksOnTimeout:     cfg.DumpStacksOnTimeout,

		SetupTimeout:    cfg.SetupTimeout.String(),
		RunTimeout:      cfg.RunTimeout.String(),
		TeardownTimeout: cfg.TeardownTimeout.String(),

		SetupDelay:    cfg.SetupDelay.String(),
		TeardownDelay: cfg.TeardownDelay.String(),

		Policy: defaultPolicy(),
	})

	var runPath string
	if !cfg.DryRun {
		p, err := prepareRunDir(cfg.RunDir, runID, cfg.Overwrite)
		if err != nil {
			emit("artifacts", "run_dir_error", map[string]any{"error": err.Error()})
			os.Exit(1)
		}
		runPath = p
		emit("artifacts", "run_dir_ready", map[string]any{"path": runPath})
	}

	// Signal handling:
	// - first SIGINT/SIGTERM: cancel run context (attempt graceful teardown)
	// - second SIGINT/SIGTERM: force immediate exit
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	var stopSignalsOnce sync.Once
	stopSignals := func() {
		stopSignalsOnce.Do(func() {
			signal.Stop(sigCh)
			close(done)
		})
	}

	go func() {
		var sig os.Signal
		select {
		case sig = <-sigCh:
		case <-done:
			return
		}
		emit("control", "interrupt", map[string]any{"signal": sig.String()})
		cancel()

		select {
		case sig = <-sigCh:
		case <-done:
			return
		}
		emit("control", "interrupt_force_exit", map[string]any{"signal": sig.String()})
		os.Exit(130)
	}()

	code, phases := runPhases(runCtx, cfg, runID, runPath, emit)
	stopSignals()

	meta := runMeta{
		RunID:     runID,
		CreatedAt: time.Now().UTC(),
		Plan: planFields{
			DSN:                     cfg.DSN,
			Readers:                 cfg.Readers,
			Writers:                 cfg.Writers,
			Duration:                cfg.Duration.String(),
			TickInterval:            cfg.TickInterval.String(),
			Seed:                    cfg.Seed,
			RunDir:                  cfg.RunDir,
			DryRun:                  cfg.DryRun,
			RunIDOverride:           cfg.RunID,
			Overwrite:               cfg.Overwrite,
			WorkerBin:               cfg.WorkerBin,
			WorkerHeartbeatInterval: cfg.WorkerHeartbeatInterval.String(),
			WorkerShutdownGrace:     cfg.WorkerShutdownGrace.String(),
			WorkerShutdownKillWait:  cfg.WorkerShutdownKillWait.String(),
			WorkerIgnoreInterrupt:   cfg.WorkerIgnoreInterrupt,
			WorkerMode:              cfg.WorkerMode,
			WorkerDB:                cfg.WorkerDB,
			WorkerTable:             cfg.WorkerTable,
			WorkerOpInterval:        cfg.WorkerOpInterval.String(),
			WorkerOpenRetry:         cfg.WorkerOpenRetry,
			WorkerSQLSession:        cfg.WorkerSQLSession,
			WorkerOpTimeout:         cfg.WorkerOpTimeout.String(),
			DiagnosticsTailLines:    cfg.DiagnosticsTailLines,
			DumpStacksOnTimeout:     cfg.DumpStacksOnTimeout,

			SetupTimeout:    cfg.SetupTimeout.String(),
			RunTimeout:      cfg.RunTimeout.String(),
			TeardownTimeout: cfg.TeardownTimeout.String(),

			SetupDelay:    cfg.SetupDelay.String(),
			TeardownDelay: cfg.TeardownDelay.String(),

			Policy: defaultPolicy(),
		},
		Phases:   phases,
		ExitCode: code,
	}

	if !cfg.DryRun {
		if err := writeMeta(runPath, meta); err != nil {
			emit("artifacts", "meta_write_error", map[string]any{"error": err.Error()})
			code = 1
		} else {
			emit("artifacts", "meta_written", map[string]any{"path": filepath.Join(runPath, "meta.json")})
		}

		manifest := newWorkerManifest(cfg.Seed, cfg.Readers, cfg.Writers)
		if err := writeManifest(runPath, manifest); err != nil {
			emit("artifacts", "manifest_write_error", map[string]any{"error": err.Error()})
			code = 1
		} else {
			emit("artifacts", "manifest_written", map[string]any{"path": filepath.Join(runPath, "manifest.json")})
		}

		if isTimeoutLike(phases, meta.ExitCode) {
			if err := writeTimeoutDiagnostics(runPath, manifest, cfg.DiagnosticsTailLines, cfg.DumpStacksOnTimeout); err != nil {
				emit("artifacts", "diagnostics_write_error", map[string]any{"error": err.Error()})
			} else {
				emit("artifacts", "diagnostics_written", map[string]any{"path": filepath.Join(runPath, "diagnostics")})
			}
		}
	} else {
		emit("artifacts", "skipped", map[string]any{"reason": "dry_run"})
	}

	if code != 0 {
		os.Exit(code)
	}
}
