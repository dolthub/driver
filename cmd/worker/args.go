package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

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
