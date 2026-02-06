package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
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
		"mode":               cfg.Mode,
		"sql_session":        cfg.SQLSession,
		"op_interval":        cfg.OpInterval.String(),
		"op_timeout":         cfg.OpTimeout.String(),
		"open_retry":         cfg.OpenRetry,
	})

	if cfg.WaitForStart {
		emit("waiting_for_start", nil)
		if err := waitForStart(ctx); err != nil {
			emit("exit", map[string]any{"ok": false, "error": err.Error()})
			os.Exit(1)
		}
		emit("started", nil)
	}

	var runErr error
	switch cfg.Mode {
	case "stub":
		runErr = runStub(ctx, cfg.HeartbeatInterval, cfg.Duration, emit)
	case "sql":
		runErr = runSQL(ctx, cfg, emit)
	default:
		runErr = fmt.Errorf("unknown mode %q", cfg.Mode)
	}

	if runErr != nil {
		emit("exit", map[string]any{"ok": false, "error": runErr.Error()})
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
