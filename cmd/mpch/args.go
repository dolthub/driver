package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func parseArgs(args []string) (cfg harnessConfig, showHelp bool, _ error) {
	fs := flag.NewFlagSet("mpch", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // we'll print our own usage/errors

	fs.StringVar(&cfg.DSN, "dsn", "", "Target Dolt DSN (required), e.g. file:///path/to/dbs?commitname=...&commitemail=...")
	fs.IntVar(&cfg.Readers, "readers", 0, "Number of reader worker processes (future)")
	fs.IntVar(&cfg.Writers, "writers", 0, "Number of writer worker processes (future)")
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Second, "Run duration (print-only for now)")
	fs.DurationVar(&cfg.TickInterval, "tick-interval", 1*time.Second, "Run tick interval (print-only for now)")
	fs.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "Seed for deterministic planning (future)")
	fs.StringVar(&cfg.RunDir, "run-dir", "./runs", "Directory for run artifacts (future)")
	fs.BoolVar(&cfg.DryRun, "dry-run", true, "If true, perform no side effects (default true for now)")
	fs.StringVar(&cfg.RunID, "run-id", "", "Explicit run id (optional). If set and artifacts exist, run fails unless --overwrite")
	fs.BoolVar(&cfg.Overwrite, "overwrite", false, "Overwrite existing run directory when --run-id collides")

	fs.StringVar(&cfg.WorkerBin, "worker-bin", "worker", "Worker executable path (Phase 5+). Must be resolvable in PATH or be a filepath.")
	fs.DurationVar(&cfg.WorkerHeartbeatInterval, "worker-heartbeat-interval", 1*time.Second, "Worker heartbeat interval (Phase 5+)")
	fs.DurationVar(&cfg.WorkerShutdownGrace, "worker-shutdown-grace", 2*time.Second, "How long to wait after SIGINT before escalating to SIGKILL (Phase 5+)")
	fs.DurationVar(&cfg.WorkerShutdownKillWait, "worker-shutdown-kill-wait", 2*time.Second, "How long to wait after SIGKILL for workers to exit (Phase 5+)")
	fs.BoolVar(&cfg.WorkerIgnoreInterrupt, "worker-ignore-interrupt", false, "Pass --ignore-interrupt=true to workers (for testing orchestrator kill paths)")
	fs.StringVar(&cfg.WorkerMode, "worker-mode", "stub", "Worker mode: stub|sql")
	fs.StringVar(&cfg.WorkerDB, "worker-db", "mpch", "Worker database name (sql mode)")
	fs.StringVar(&cfg.WorkerTable, "worker-table", "harness_events", "Worker table name (sql mode)")
	fs.DurationVar(&cfg.WorkerOpInterval, "worker-op-interval", 100*time.Millisecond, "Worker operation interval (sql mode)")
	fs.BoolVar(&cfg.WorkerOpenRetry, "worker-open-retry", true, "Enable retry on embedded engine open in workers (sql mode)")
	fs.StringVar(&cfg.WorkerSQLSession, "worker-sql-session", "per_op", "Worker SQL session scope: per_op|per_run (sql mode)")
	fs.DurationVar(&cfg.WorkerOpTimeout, "worker-op-timeout", 500*time.Millisecond, "Worker timeout per operation (sql mode)")

	fs.IntVar(&cfg.DiagnosticsTailLines, "diagnostics-tail-lines", 200, "On timeouts, write last N lines of worker logs to diagnostics/")
	fs.BoolVar(&cfg.DumpStacksOnTimeout, "dump-stacks-on-timeout", true, "On timeouts, write mpch stack dump to diagnostics/")

	fs.DurationVar(&cfg.SetupTimeout, "setup-timeout", 10*time.Second, "Setup phase timeout")
	fs.DurationVar(&cfg.RunTimeout, "run-timeout", 0, "Run phase timeout (0 = duration + 5s)")
	fs.DurationVar(&cfg.TeardownTimeout, "teardown-timeout", 10*time.Second, "Teardown phase timeout")

	fs.DurationVar(&cfg.SetupDelay, "setup-delay", 0, "Print-only delay injected into setup phase (for testing timeouts)")
	fs.DurationVar(&cfg.TeardownDelay, "teardown-delay", 0, "Print-only delay injected into teardown phase (for testing timeouts)")

	fs.Usage = func() { printUsage(os.Stdout, fs) }

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			// The flag package already invoked fs.Usage() for ErrHelp.
			return harnessConfig{}, true, nil
		}
		printUsage(os.Stderr, fs)
		return harnessConfig{}, false, fmt.Errorf("invalid arguments: %w", err)
	}

	if cfg.DSN == "" {
		printUsage(os.Stderr, fs)
		return harnessConfig{}, false, fmt.Errorf("missing required --dsn")
	}
	if cfg.Readers < 0 {
		return harnessConfig{}, false, fmt.Errorf("--readers must be >= 0")
	}
	if cfg.Writers < 0 {
		return harnessConfig{}, false, fmt.Errorf("--writers must be >= 0")
	}
	if cfg.Duration < 0 {
		return harnessConfig{}, false, fmt.Errorf("--duration must be >= 0")
	}
	if cfg.TickInterval <= 0 {
		return harnessConfig{}, false, fmt.Errorf("--tick-interval must be > 0")
	}
	if cfg.WorkerHeartbeatInterval <= 0 {
		return harnessConfig{}, false, fmt.Errorf("--worker-heartbeat-interval must be > 0")
	}
	if cfg.WorkerShutdownGrace <= 0 {
		return harnessConfig{}, false, fmt.Errorf("--worker-shutdown-grace must be > 0")
	}
	if cfg.WorkerShutdownKillWait <= 0 {
		return harnessConfig{}, false, fmt.Errorf("--worker-shutdown-kill-wait must be > 0")
	}
	if cfg.WorkerMode != "stub" && cfg.WorkerMode != "sql" {
		return harnessConfig{}, false, fmt.Errorf("--worker-mode must be stub or sql")
	}
	if cfg.WorkerMode == "sql" {
		if cfg.WorkerDB == "" {
			return harnessConfig{}, false, fmt.Errorf("--worker-db cannot be empty")
		}
		if cfg.WorkerTable == "" {
			return harnessConfig{}, false, fmt.Errorf("--worker-table cannot be empty")
		}
		if cfg.WorkerOpInterval <= 0 {
			return harnessConfig{}, false, fmt.Errorf("--worker-op-interval must be > 0")
		}
		if cfg.WorkerOpTimeout <= 0 {
			return harnessConfig{}, false, fmt.Errorf("--worker-op-timeout must be > 0")
		}
		if cfg.WorkerSQLSession != "per_op" && cfg.WorkerSQLSession != "per_run" {
			return harnessConfig{}, false, fmt.Errorf("--worker-sql-session must be per_op or per_run")
		}
	}
	if cfg.DiagnosticsTailLines < 0 {
		return harnessConfig{}, false, fmt.Errorf("--diagnostics-tail-lines must be >= 0")
	}
	if cfg.SetupTimeout <= 0 {
		return harnessConfig{}, false, fmt.Errorf("--setup-timeout must be > 0")
	}
	if cfg.RunTimeout < 0 {
		return harnessConfig{}, false, fmt.Errorf("--run-timeout must be >= 0")
	}
	if cfg.TeardownTimeout <= 0 {
		return harnessConfig{}, false, fmt.Errorf("--teardown-timeout must be > 0")
	}
	if cfg.SetupDelay < 0 {
		return harnessConfig{}, false, fmt.Errorf("--setup-delay must be >= 0")
	}
	if cfg.TeardownDelay < 0 {
		return harnessConfig{}, false, fmt.Errorf("--teardown-delay must be >= 0")
	}
	if cfg.RunDir == "" {
		return harnessConfig{}, false, fmt.Errorf("--run-dir cannot be empty")
	}
	if err := validateRunID(cfg.RunID); err != nil {
		return harnessConfig{}, false, err
	}

	if cfg.RunTimeout == 0 {
		cfg.RunTimeout = cfg.Duration + 5*time.Second
		if cfg.RunTimeout <= 0 {
			cfg.RunTimeout = 5 * time.Second
		}
	}

	return cfg, false, nil
}

func printUsage(w io.Writer, fs *flag.FlagSet) {
	fmt.Fprintln(w, "Usage: mpch --dsn <file:///...> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Flags:")

	// flag.FlagSet only prints to its configured output.
	prev := fs.Output()
	fs.SetOutput(w)
	fs.PrintDefaults()
	fs.SetOutput(prev)
}

func validateRunID(id string) error {
	if id == "" {
		return nil
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_':
		default:
			return fmt.Errorf("--run-id may only contain [A-Za-z0-9_-]")
		}
	}
	return nil
}
