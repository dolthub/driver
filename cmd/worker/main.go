package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	RunID             string
	WorkerID          string
	Role              string
	HeartbeatInterval time.Duration
	Duration          time.Duration
	WaitForStart      bool
	IgnoreInterrupt   bool
}

type event struct {
	TS       time.Time `json:"ts"`
	RunID    string    `json:"run_id,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	Role     string    `json:"role,omitempty"`

	Event  string `json:"event"`
	Fields any    `json:"fields,omitempty"`
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

	// Same pipeline robustness as mpch.
	signal.Ignore(syscall.SIGPIPE)

	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)

	emit := func(name string, fields any) {
		if err := enc.Encode(event{
			TS:       time.Now().UTC(),
			RunID:    cfg.RunID,
			WorkerID: cfg.WorkerID,
			Role:     cfg.Role,
			Event:    name,
			Fields:   fields,
		}); err != nil {
			// Best effort; if stdout is closed, just exit.
			os.Exit(0)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		emit("interrupt", map[string]any{"signal": sig.String()})
		if !cfg.IgnoreInterrupt {
			cancel()
		}
	}()

	emit("ready", map[string]any{
		"heartbeat_interval": cfg.HeartbeatInterval.String(),
		"duration":           cfg.Duration.String(),
		"wait_for_start":     cfg.WaitForStart,
	})

	if cfg.WaitForStart {
		emit("waiting_for_start", nil)
		if err := waitForStart(ctx); err != nil {
			emit("exit", map[string]any{"ok": false, "error": err.Error()})
			os.Exit(1)
		}
		emit("started", nil)
	}

	if err := runLoop(ctx, cfg.HeartbeatInterval, cfg.Duration, emit); err != nil {
		emit("exit", map[string]any{"ok": false, "error": err.Error()})
		os.Exit(1)
	}
	emit("exit", map[string]any{"ok": true})
}

func waitForStart(ctx context.Context) error {
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		r := bufio.NewReader(os.Stdin)
		line, err := r.ReadString('\n')
		ch <- result{line: line, err: err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			return res.err
		}
		_ = res.line // any line means "start" for now
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func runLoop(
	ctx context.Context,
	heartbeat time.Duration,
	duration time.Duration,
	emit func(name string, fields any),
) error {
	if heartbeat <= 0 {
		heartbeat = time.Second
	}

	var done <-chan time.Time
	if duration > 0 {
		t := time.NewTimer(duration)
		defer t.Stop()
		done = t.C
	}

	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()

	start := time.Now()
	count := 0
	for {
		select {
		case <-ticker.C:
			count++
			emit("heartbeat", map[string]any{
				"count":      count,
				"elapsed_ms": time.Since(start).Milliseconds(),
			})
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func parseArgs(args []string) (cfg config, showHelp bool, _ error) {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	fs.StringVar(&cfg.RunID, "run-id", "", "Run id (optional, for correlation)")
	fs.StringVar(&cfg.WorkerID, "worker-id", "", "Worker id (required)")
	fs.StringVar(&cfg.Role, "role", "", "Worker role (required): reader|writer")
	fs.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", 1*time.Second, "Heartbeat interval")
	fs.DurationVar(&cfg.Duration, "duration", 0, "Optional duration before clean exit (0 = run until signal)")
	fs.BoolVar(&cfg.WaitForStart, "wait-for-start", false, "If true, emit ready then wait for a start signal on stdin")
	fs.BoolVar(&cfg.IgnoreInterrupt, "ignore-interrupt", false, "If true, emit interrupt event but do not stop (for testing orchestrator kill paths)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stdout, "Usage: worker --worker-id <id> --role <reader|writer> [flags]")
		fmt.Fprintln(os.Stdout, "")
		fmt.Fprintln(os.Stdout, "Flags:")
		prev := fs.Output()
		fs.SetOutput(os.Stdout)
		fs.PrintDefaults()
		fs.SetOutput(prev)
	}

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			// flag package already invoked usage for ErrHelp.
			return config{}, true, nil
		}
		fs.Usage()
		return config{}, false, fmt.Errorf("invalid arguments: %w", err)
	}

	if cfg.WorkerID == "" {
		fs.Usage()
		return config{}, false, fmt.Errorf("missing required --worker-id")
	}
	if cfg.Role != "reader" && cfg.Role != "writer" {
		fs.Usage()
		return config{}, false, fmt.Errorf("invalid --role %q (must be reader or writer)", cfg.Role)
	}
	if cfg.HeartbeatInterval <= 0 {
		return config{}, false, fmt.Errorf("--heartbeat-interval must be > 0")
	}
	if cfg.Duration < 0 {
		return config{}, false, fmt.Errorf("--duration must be >= 0")
	}

	return cfg, false, nil
}

