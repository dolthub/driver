package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	embedded "github.com/dolthub/driver"
)

type config struct {
	RunID             string
	WorkerID          string
	Role              string
	HeartbeatInterval time.Duration
	Duration          time.Duration
	WaitForStart      bool
	IgnoreInterrupt   bool

	Mode       string
	DSN        string
	DBName     string
	TableName  string
	OpInterval time.Duration

	OpenRetry bool
	SQLSession string
	OpTimeout time.Duration
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
		"mode":               cfg.Mode,
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

func runStub(
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

	for {
		select {
		case <-op.C:
			var err error
			if cfg.SQLSession == "per_op" {
				opCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
				d, closer, oerr := openDB(opCtx, cfg)
				if oerr != nil {
					err = oerr
				} else {
					err = doOneOp(opCtx, d, cfg)
				}
				if closer != nil {
					closer()
				}
				cancel()
			} else {
				opCtx, cancel := context.WithTimeout(ctx, cfg.OpTimeout)
				err = doOneOp(opCtx, db, cfg)
				cancel()
			}
			if err != nil {
				s.opsErr++
				s.lastErr = err.Error()
				emit("op_error", map[string]any{"error": err.Error()})
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

	fs.StringVar(&cfg.Mode, "mode", "stub", "Worker mode: stub|sql")
	fs.StringVar(&cfg.DSN, "dsn", "", "Target Dolt DSN (required for --mode=sql)")
	fs.StringVar(&cfg.DBName, "db", "mpch", "Database name to use/create (sql mode)")
	fs.StringVar(&cfg.TableName, "table", "harness_events", "Table name to use/create (sql mode)")
	fs.DurationVar(&cfg.OpInterval, "op-interval", 100*time.Millisecond, "Operation interval (sql mode)")
	fs.DurationVar(&cfg.OpTimeout, "op-timeout", 500*time.Millisecond, "Timeout per operation (sql mode)")
	fs.BoolVar(&cfg.OpenRetry, "open-retry", false, "Enable retry on embedded engine open (sql mode)")
	fs.StringVar(&cfg.SQLSession, "sql-session", "per_op", "SQL session scope: per_op (open/close per op) or per_run (single open)")

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
	if cfg.Mode != "stub" && cfg.Mode != "sql" {
		return config{}, false, fmt.Errorf("--mode must be stub or sql")
	}
	if cfg.OpInterval <= 0 {
		return config{}, false, fmt.Errorf("--op-interval must be > 0")
	}
	if cfg.OpTimeout <= 0 {
		return config{}, false, fmt.Errorf("--op-timeout must be > 0")
	}
	if cfg.SQLSession != "per_op" && cfg.SQLSession != "per_run" {
		return config{}, false, fmt.Errorf("--sql-session must be per_op or per_run")
	}

	return cfg, false, nil
}

