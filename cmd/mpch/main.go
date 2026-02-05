package main

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

type harnessConfig struct {
	DSN      string
	Readers  int
	Writers  int
	Duration time.Duration
	Seed     int64
	RunDir   string
	DryRun   bool
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

	// Phase 0: print-only skeleton. No filesystem writes, no child processes, no DB calls.
	fmt.Printf(
		"run_id=%s dsn=%q readers=%d writers=%d duration=%s seed=%d run_dir=%q dry_run=%t\n",
		runID,
		cfg.DSN,
		cfg.Readers,
		cfg.Writers,
		cfg.Duration,
		cfg.Seed,
		cfg.RunDir,
		cfg.DryRun,
	)
}

func parseArgs(args []string) (cfg harnessConfig, showHelp bool, _ error) {
	fs := flag.NewFlagSet("mpch", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // we'll print our own usage/errors

	fs.StringVar(&cfg.DSN, "dsn", "", "Target Dolt DSN (required), e.g. file:///path/to/dbs?commitname=...&commitemail=...")
	fs.IntVar(&cfg.Readers, "readers", 0, "Number of reader worker processes (future)")
	fs.IntVar(&cfg.Writers, "writers", 0, "Number of writer worker processes (future)")
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Second, "Run duration (print-only for now)")
	fs.Int64Var(&cfg.Seed, "seed", time.Now().UnixNano(), "Seed for deterministic planning (future)")
	fs.StringVar(&cfg.RunDir, "run-dir", "./runs", "Directory for run artifacts (future)")
	fs.BoolVar(&cfg.DryRun, "dry-run", true, "If true, perform no side effects (default true for now)")

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
	if cfg.RunDir == "" {
		return harnessConfig{}, false, fmt.Errorf("--run-dir cannot be empty")
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

