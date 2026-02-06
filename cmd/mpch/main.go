package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	SetupTimeout    string `json:"setup_timeout"`
	RunTimeout      string `json:"run_timeout"`
	TeardownTimeout string `json:"teardown_timeout"`

	SetupDelay    string `json:"setup_delay"`
	TeardownDelay string `json:"teardown_delay"`

	Policy gatingPolicy `json:"policy"`
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

	runID, err := newRunID()
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to generate run id:", err)
		os.Exit(1)
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

		SetupTimeout:    cfg.SetupTimeout.String(),
		RunTimeout:      cfg.RunTimeout.String(),
		TeardownTimeout: cfg.TeardownTimeout.String(),

		SetupDelay:    cfg.SetupDelay.String(),
		TeardownDelay: cfg.TeardownDelay.String(),

		Policy: defaultPolicy(),
	})

	// Signal handling:
	// - first SIGINT/SIGTERM: cancel run context (attempt graceful teardown)
	// - second SIGINT/SIGTERM: force immediate exit
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	done := make(chan struct{})
	defer close(done)

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

	if code := runPhases(runCtx, cfg, emit); code != 0 {
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

func runPhases(ctx context.Context, cfg harnessConfig, emit func(phase, name string, fields any)) int {
	// Policy:
	// - setup failure: do not enter run; attempt teardown best-effort; then fail
	// - run failure: attempt teardown best-effort; then fail
	// - teardown failure: fail

	setupErr := runPhase(ctx, "setup", cfg.SetupTimeout, emit, func(pctx context.Context) error {
		return sleepWithContext(pctx, cfg.SetupDelay)
	})

	var runErr error
	if setupErr == nil {
		runErr = runPhase(ctx, "run", cfg.RunTimeout, emit, func(pctx context.Context) error {
			return runWithTicks(pctx, cfg.Duration, cfg.TickInterval, emit)
		})
	}

	// Teardown must still be able to run even if ctx is canceled (e.g. SIGINT).
	teardownErr := runPhase(context.Background(), "teardown", cfg.TeardownTimeout, emit, func(pctx context.Context) error {
		return sleepWithContext(pctx, cfg.TeardownDelay)
	})

	if setupErr != nil {
		return 1
	}
	if runErr != nil {
		return 1
	}
	if teardownErr != nil {
		return 1
	}
	return 0
}

func runPhase(
	ctx context.Context,
	phase string,
	timeout time.Duration,
	emit func(phase, name string, fields any),
	fn func(context.Context) error,
) error {
	emit(phase, "phase_start", nil)

	start := time.Now()
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := fn(pctx)

	endFields := phaseEndFields{
		OK:        err == nil,
		ElapsedMs: time.Since(start).Milliseconds(),
	}
	if err != nil {
		endFields.Error = err.Error()
	}
	emit(phase, "phase_end", endFields)
	return err
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

