package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	embedded "github.com/dolthub/driver"
)

type harnessConfig struct {
	DSN      string
	Readers  int
	Writers  int
	Duration time.Duration
	TickInterval time.Duration
	Seed     int64
	RunDir   string
	DryRun   bool
	RunID     string
	Overwrite bool
	WorkerBin string
	WorkerHeartbeatInterval time.Duration
	WorkerShutdownGrace time.Duration
	WorkerShutdownKillWait time.Duration
	WorkerIgnoreInterrupt bool
	WorkerMode string
	WorkerDB string
	WorkerTable string
	WorkerOpInterval time.Duration
	WorkerOpenRetry bool
	WorkerSQLSession string
	WorkerOpTimeout time.Duration

	SetupTimeout    time.Duration
	RunTimeout      time.Duration
	TeardownTimeout time.Duration

	// Print-only knobs to exercise the phase engine deterministically.
	SetupDelay    time.Duration
	TeardownDelay time.Duration
}

type event struct {
	TS    time.Time `json:"ts"`
	RunID string    `json:"run_id"`
	Phase string    `json:"phase"`
	Event string    `json:"event"`

	Fields any `json:"fields,omitempty"`
}

type planFields struct {
	DSN      string `json:"dsn"`
	Readers  int    `json:"readers"`
	Writers  int    `json:"writers"`
	Duration string `json:"duration"`
	TickInterval string `json:"tick_interval"`
	Seed     int64  `json:"seed"`
	RunDir   string `json:"run_dir"`
	DryRun   bool   `json:"dry_run"`
	RunIDOverride string `json:"run_id_override,omitempty"`
	Overwrite     bool   `json:"overwrite"`
	WorkerBin string `json:"worker_bin"`
	WorkerHeartbeatInterval string `json:"worker_heartbeat_interval"`
	WorkerShutdownGrace string `json:"worker_shutdown_grace"`
	WorkerShutdownKillWait string `json:"worker_shutdown_kill_wait"`
	WorkerIgnoreInterrupt bool `json:"worker_ignore_interrupt"`
	WorkerMode string `json:"worker_mode"`
	WorkerDB string `json:"worker_db"`
	WorkerTable string `json:"worker_table"`
	WorkerOpInterval string `json:"worker_op_interval"`
	WorkerOpenRetry bool `json:"worker_open_retry"`
	WorkerSQLSession string `json:"worker_sql_session"`
	WorkerOpTimeout string `json:"worker_op_timeout"`

	SetupTimeout    string `json:"setup_timeout"`
	RunTimeout      string `json:"run_timeout"`
	TeardownTimeout string `json:"teardown_timeout"`

	SetupDelay    string `json:"setup_delay"`
	TeardownDelay string `json:"teardown_delay"`

	Policy gatingPolicy `json:"policy"`
}

type phaseMeta struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	OK    bool      `json:"ok"`
	Error string    `json:"error,omitempty"`
}

type runMeta struct {
	RunID     string     `json:"run_id"`
	CreatedAt time.Time  `json:"created_at"`
	Plan      planFields `json:"plan"`

	Phases map[string]phaseMeta `json:"phases"`

	ExitCode int `json:"exit_code"`
}

type workerRole string

const (
	roleReader workerRole = "reader"
	roleWriter workerRole = "writer"
)

type workerSpec struct {
	ID    string     `json:"id"`
	Role  workerRole `json:"role"`
	Index int        `json:"index"`

	// Reserved for Phase 5+.
	Args []string          `json:"args,omitempty"`
	Env  map[string]string `json:"env,omitempty"`
}

type workerManifest struct {
	Seed    int64 `json:"seed"`
	Readers int   `json:"readers"`
	Writers int   `json:"writers"`

	Workers []workerSpec `json:"workers"`
}

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
		DSN:      cfg.DSN,
		Readers:  cfg.Readers,
		Writers:  cfg.Writers,
		Duration: cfg.Duration.String(),
		TickInterval: cfg.TickInterval.String(),
		Seed:     cfg.Seed,
		RunDir:   cfg.RunDir,
		DryRun:   cfg.DryRun,
		RunIDOverride: cfg.RunID,
		Overwrite: cfg.Overwrite,
		WorkerBin: cfg.WorkerBin,
		WorkerHeartbeatInterval: cfg.WorkerHeartbeatInterval.String(),
		WorkerShutdownGrace: cfg.WorkerShutdownGrace.String(),
		WorkerShutdownKillWait: cfg.WorkerShutdownKillWait.String(),
		WorkerIgnoreInterrupt: cfg.WorkerIgnoreInterrupt,
		WorkerMode: cfg.WorkerMode,
		WorkerDB: cfg.WorkerDB,
		WorkerTable: cfg.WorkerTable,
		WorkerOpInterval: cfg.WorkerOpInterval.String(),
		WorkerOpenRetry: cfg.WorkerOpenRetry,
		WorkerSQLSession: cfg.WorkerSQLSession,
		WorkerOpTimeout: cfg.WorkerOpTimeout.String(),

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
			DSN:          cfg.DSN,
			Readers:      cfg.Readers,
			Writers:      cfg.Writers,
			Duration:     cfg.Duration.String(),
			TickInterval: cfg.TickInterval.String(),
			Seed:         cfg.Seed,
			RunDir:       cfg.RunDir,
			DryRun:       cfg.DryRun,
			RunIDOverride: cfg.RunID,
			Overwrite: cfg.Overwrite,
			WorkerBin: cfg.WorkerBin,
			WorkerHeartbeatInterval: cfg.WorkerHeartbeatInterval.String(),
			WorkerShutdownGrace: cfg.WorkerShutdownGrace.String(),
			WorkerShutdownKillWait: cfg.WorkerShutdownKillWait.String(),
			WorkerIgnoreInterrupt: cfg.WorkerIgnoreInterrupt,
			WorkerMode: cfg.WorkerMode,
			WorkerDB: cfg.WorkerDB,
			WorkerTable: cfg.WorkerTable,
			WorkerOpInterval: cfg.WorkerOpInterval.String(),
			WorkerOpenRetry: cfg.WorkerOpenRetry,
			WorkerSQLSession: cfg.WorkerSQLSession,
			WorkerOpTimeout: cfg.WorkerOpTimeout.String(),

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
	} else {
		emit("artifacts", "skipped", map[string]any{"reason": "dry_run"})
	}

	if code != 0 {
		os.Exit(code)
	}
}

type phaseEndFields struct {
	OK        bool  `json:"ok"`
	ElapsedMs int64 `json:"elapsed_ms"`
	Error     any   `json:"error,omitempty"`
}

type gatingPolicy struct {
	SetupFailure    string `json:"setup_failure"`
	RunFailure      string `json:"run_failure"`
	TeardownFailure string `json:"teardown_failure"`
}

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

type tickFields struct {
	Count     int   `json:"count"`
	ElapsedMs int64 `json:"elapsed_ms"`
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

func newRunID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "run-" + hex.EncodeToString(b[:]), nil
}

func isBrokenPipe(err error) bool {
	// We care about the common case: write to a closed pipe.
	return errors.Is(err, syscall.EPIPE)
}

func writeMeta(runPath string, meta runMeta) error {
	b, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(runPath, "meta.json"), b, 0o644)
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

func writeManifest(runPath string, manifest workerManifest) error {
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(runPath, "manifest.json"), b, 0o644)
}

func prepareRunDir(runDir, runID string, overwrite bool) (string, error) {
	runPath := filepath.Join(runDir, runID)
	if _, err := os.Stat(runPath); err == nil {
		if !overwrite {
			return "", fmt.Errorf("run directory already exists: %s (use --overwrite to replace)", runPath)
		}
		if err := os.RemoveAll(runPath); err != nil {
			return "", err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(runPath, "workers"), 0o755); err != nil {
		return "", err
	}
	return runPath, nil
}

func ensureSQLSchema(ctx context.Context, cfg harnessConfig) error {
	if cfg.DSN == "" {
		return fmt.Errorf("--dsn is required for --worker-mode=sql")
	}
	if cfg.WorkerDB == "" || cfg.WorkerTable == "" {
		return fmt.Errorf("worker db/table cannot be empty")
	}

	ecfg, err := embedded.ParseDSN(cfg.DSN)
	if err != nil {
		return err
	}
	// Avoid selecting DB before it exists.
	ecfg.Database = ""
	// Ensure schema setup doesn't hold onto singleton-cached engine instances.
	// (A non-nil BackOff triggers DisableSingletonCache in the connector open path.)
	ecfg.BackOff = backoff.WithMaxRetries(backoff.NewConstantBackOff(0), 0)

	connector, err := embedded.NewConnector(ecfg)
	if err != nil {
		return err
	}
	defer connector.Close()

	db := sql.OpenDB(connector)
	defer db.Close()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", cfg.WorkerDB)); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("USE %s", cfg.WorkerDB)); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id BIGINT PRIMARY KEY AUTO_INCREMENT, worker_id VARCHAR(64), ts BIGINT)",
		cfg.WorkerTable,
	)); err != nil {
		return err
	}

	return nil
}

type workerProc struct {
	spec workerSpec
	cmd  *exec.Cmd

	stdin io.WriteCloser
	workerDir string

	readyCh chan struct{}
	doneCh chan struct{}

	mu      sync.Mutex
	waitErr error
}

type workerLaunchArgs struct {
	Mode       string
	DSN        string
	DB         string
	Table      string
	OpInterval time.Duration
	OpenRetry  bool
	SQLSession string
	OpTimeout  time.Duration

	Heartbeat       time.Duration
	IgnoreInterrupt bool
}

type workerExit struct {
	WorkerID string     `json:"worker_id"`
	Role     workerRole `json:"role"`
	Error    string     `json:"error,omitempty"`
}

type workerTermination struct {
	TS time.Time `json:"ts"`
	WorkerID string `json:"worker_id"`
	Role workerRole `json:"role"`
	Reason string `json:"reason"`
	Signal string `json:"signal,omitempty"`
	Killed bool `json:"killed"`
	ExitError string `json:"exit_error,omitempty"`
}

func runWorkersAndTicks(
	ctx context.Context,
	cfg harnessConfig,
	runID, runPath string,
	manifest workerManifest,
	emit func(phase, name string, fields any),
) (retErr error) {
	workerBin, err := exec.LookPath(cfg.WorkerBin)
	if err != nil {
		return fmt.Errorf("failed to find worker binary %q: %w", cfg.WorkerBin, err)
	}

	procs := make([]*workerProc, 0, len(manifest.Workers))
	defer func() {
		if err := shutdownWorkers(procs, cfg.WorkerShutdownGrace, cfg.WorkerShutdownKillWait, emit); err != nil {
			if retErr == nil {
				retErr = err
			} else {
				retErr = fmt.Errorf("%v; shutdown error: %w", retErr, err)
			}
		}
	}()

	for _, w := range manifest.Workers {
		p, err := startWorker(ctx, workerBin, runID, runPath, w, workerLaunchArgs{
			Mode:            cfg.WorkerMode,
			DSN:             cfg.DSN,
			DB:              cfg.WorkerDB,
			Table:           cfg.WorkerTable,
			OpInterval:      cfg.WorkerOpInterval,
			OpenRetry:       cfg.WorkerOpenRetry,
			SQLSession:      cfg.WorkerSQLSession,
			OpTimeout:       cfg.WorkerOpTimeout,
			Heartbeat:       cfg.WorkerHeartbeatInterval,
			IgnoreInterrupt: cfg.WorkerIgnoreInterrupt,
		}, emit)
		if err != nil {
			return err
		}
		procs = append(procs, p)
	}
	emit("run", "workers_spawned", map[string]any{"count": len(procs)})

	// Ready barrier.
	for _, p := range procs {
		select {
		case <-p.readyCh:
		case <-p.doneCh:
			err := workerWaitErr(p)
			if err == nil {
				return fmt.Errorf("worker %s exited before ready", p.spec.ID)
			}
			return fmt.Errorf("worker %s exited before ready: %w", p.spec.ID, err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	emit("run", "all_ready", map[string]any{"count": len(procs)})

	// Start signal.
	for _, p := range procs {
		if _, err := io.WriteString(p.stdin, "start\n"); err != nil {
			return fmt.Errorf("failed to send start to worker %s: %w", p.spec.ID, err)
		}
	}
	emit("run", "all_started", map[string]any{"count": len(procs)})

	// Watch for workers exiting early.
	exitNotify := make(chan workerExit, len(procs))
	for _, p := range procs {
		pp := p
		go func() {
			<-pp.doneCh
			err := workerWaitErr(pp)
			we := workerExit{WorkerID: pp.spec.ID, Role: pp.spec.Role}
			if err != nil {
				we.Error = err.Error()
			}
			exitNotify <- we
		}()
	}

	// Run ticks for duration (or until cancel), failing fast on worker exit.
	runErr := runWithTicksOrWorkerExit(ctx, cfg.Duration, cfg.TickInterval, emit, exitNotify)
	// cleanup() will stop workers.
	return runErr
}

func startWorker(
	ctx context.Context,
	workerBin string,
	runID string,
	runPath string,
	spec workerSpec,
	args workerLaunchArgs,
	emit func(phase, name string, fields any),
) (*workerProc, error) {
	workerDir := filepath.Join(runPath, "workers", spec.ID)
	if err := os.MkdirAll(workerDir, 0o755); err != nil {
		return nil, err
	}

	argv := []string{
		"--run-id", runID,
		"--worker-id", spec.ID,
		"--role", string(spec.Role),
		"--heartbeat-interval", args.Heartbeat.String(),
		"--wait-for-start=true",
		"--mode", args.Mode,
		"--dsn", args.DSN,
		"--db", args.DB,
		"--table", args.Table,
		"--op-interval", args.OpInterval.String(),
		"--open-retry=" + fmt.Sprintf("%t", args.OpenRetry),
		"--sql-session", args.SQLSession,
		"--op-timeout", args.OpTimeout.String(),
	}
	if args.IgnoreInterrupt {
		argv = append(argv, "--ignore-interrupt=true")
	}

	cmd := exec.CommandContext(ctx, workerBin, argv...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	stdoutFile, err := os.Create(filepath.Join(workerDir, "stdout.jsonl"))
	if err != nil {
		return nil, err
	}
	stderrFile, err := os.Create(filepath.Join(workerDir, "stderr.log"))
	if err != nil {
		_ = stdoutFile.Close()
		return nil, err
	}

	p := &workerProc{
		spec:    spec,
		cmd:     cmd,
		stdin:   stdin,
		workerDir: workerDir,
		readyCh: make(chan struct{}),
		doneCh:  make(chan struct{}),
	}

	go func() {
		defer func() {
			_ = stdoutFile.Close()
		}()
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			line := sc.Text()
			_, _ = stdoutFile.WriteString(line + "\n")

			// Parse ready event.
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				if ev, _ := m["event"].(string); ev == "ready" {
					select {
					case <-p.readyCh:
					default:
						close(p.readyCh)
						emit("run", "worker_ready", map[string]any{"worker_id": spec.ID, "role": spec.Role})
					}
				}
			}
		}
	}()

	go func() {
		defer func() { _ = stderrFile.Close() }()
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			_, _ = stderrFile.WriteString(sc.Text() + "\n")
		}
	}()

	if err := cmd.Start(); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, err
	}
	emit("run", "worker_spawned", map[string]any{"worker_id": spec.ID, "role": spec.Role})

	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.waitErr = err
		p.mu.Unlock()
		close(p.doneCh)
	}()

	return p, nil
}

func workerWaitErr(p *workerProc) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func shutdownWorkers(procs []*workerProc, grace, killWait time.Duration, emit func(phase, name string, fields any)) error {
	if len(procs) == 0 {
		return nil
	}

	// Close stdin (best-effort) to avoid hanging on input.
	for _, p := range procs {
		if p.stdin != nil {
			_ = p.stdin.Close()
		}
	}

	// First: SIGINT.
	for _, p := range procs {
		if p.cmd != nil && p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(os.Interrupt)
		}
	}
	emit("run", "workers_interrupt_sent", map[string]any{"count": len(procs)})

	deadline := time.NewTimer(grace)
	defer deadline.Stop()

	done := make(map[*workerProc]struct{}, len(procs))
	for len(done) < len(procs) {
		progress := false
		for _, p := range procs {
			if _, ok := done[p]; ok {
				continue
			}
			select {
			case <-p.doneCh:
				done[p] = struct{}{}
				progress = true
			default:
			}
		}
		if progress {
			continue
		}
		select {
		case <-deadline.C:
			goto KILL
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	emit("run", "workers_exited", map[string]any{"count": len(procs), "killed": 0})
	for p := range done {
		err := workerWaitErr(p)
		_ = writeTermination(p, workerTermination{
			TS: time.Now().UTC(),
			WorkerID: p.spec.ID,
			Role: p.spec.Role,
			Reason: "graceful_exit",
			Signal: "SIGINT",
			Killed: false,
			ExitError: exitErrString(err),
		})
	}
	return nil

KILL:
	// Escalate: SIGKILL process group.
	killed := 0
	for _, p := range procs {
		if _, ok := done[p]; ok {
			continue
		}
		if p.cmd == nil || p.cmd.Process == nil {
			continue
		}
		pid := p.cmd.Process.Pid
		// Kill the whole process group (negative pid) when available.
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = p.cmd.Process.Kill()
		killed++
	}
	emit("run", "workers_kill_sent", map[string]any{"count": killed})

	killDeadline := time.NewTimer(killWait)
	defer killDeadline.Stop()
	for len(done) < len(procs) {
		progress := false
		for _, p := range procs {
			if _, ok := done[p]; ok {
				continue
			}
			select {
			case <-p.doneCh:
				done[p] = struct{}{}
				progress = true
			default:
			}
		}
		if progress {
			continue
		}
		select {
		case <-killDeadline.C:
			emit("run", "workers_kill_timeout", map[string]any{"remaining": len(procs) - len(done)})
			for _, p := range procs {
				if _, ok := done[p]; ok {
					continue
				}
				_ = writeTermination(p, workerTermination{
					TS: time.Now().UTC(),
					WorkerID: p.spec.ID,
					Role: p.spec.Role,
					Reason: "kill_timeout",
					Signal: "SIGKILL",
					Killed: true,
				})
			}
			return fmt.Errorf("timeout waiting for %d worker(s) to exit after SIGKILL", len(procs)-len(done))
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	emit("run", "workers_exited", map[string]any{"count": len(procs), "killed": killed})
	for _, p := range procs {
		err := workerWaitErr(p)
		term := workerTermination{
			TS: time.Now().UTC(),
			WorkerID: p.spec.ID,
			Role: p.spec.Role,
			Killed: false,
			ExitError: exitErrString(err),
		}
		if err == nil {
			term.Reason = "graceful_exit"
			term.Signal = "SIGINT"
		} else if killed > 0 {
			term.Reason = "killed_after_grace_timeout"
			term.Signal = "SIGKILL"
			term.Killed = true
		} else {
			term.Reason = "exited_with_error"
		}
		_ = writeTermination(p, term)
	}
	return fmt.Errorf("one or more workers required SIGKILL after grace timeout")
}

func writeTermination(p *workerProc, term workerTermination) error {
	if p == nil || p.workerDir == "" {
		return nil
	}
	b, err := json.MarshalIndent(term, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(p.workerDir, "termination.json"), b, 0o644)
}

func exitErrString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func newWorkerManifest(seed int64, readers, writers int) workerManifest {
	workers := make([]workerSpec, 0, readers+writers)

	for i := 0; i < writers; i++ {
		workers = append(workers, workerSpec{
			ID:    workerID(seed, roleWriter, i),
			Role:  roleWriter,
			Index: i,
		})
	}
	for i := 0; i < readers; i++ {
		workers = append(workers, workerSpec{
			ID:    workerID(seed, roleReader, i),
			Role:  roleReader,
			Index: i,
		})
	}

	return workerManifest{
		Seed:    seed,
		Readers: readers,
		Writers: writers,
		Workers: workers,
	}
}

func workerID(seed int64, role workerRole, index int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%d", seed, role, index)))
	// Short but stable. Enough uniqueness for a single manifest.
	return fmt.Sprintf("%s-%s", string(role)[0:1], hex.EncodeToString(sum[:4]))
}

