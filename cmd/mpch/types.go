package main

import (
	"os/exec"
	"sync"
	"time"
)

type harnessConfig struct {
	DSN          string
	Readers      int
	Writers      int
	Duration     time.Duration
	TickInterval time.Duration
	Seed         int64
	RunDir       string
	DryRun       bool
	RunID        string
	Overwrite    bool

	WorkerBin               string
	WorkerHeartbeatInterval time.Duration
	WorkerShutdownGrace     time.Duration
	WorkerShutdownKillWait  time.Duration
	WorkerIgnoreInterrupt   bool
	WorkerMode              string
	WorkerDB                string
	WorkerTable             string
	WorkerOpInterval        time.Duration
	WorkerOpenRetry         bool
	WorkerSQLSession        string
	WorkerOpTimeout         time.Duration

	DiagnosticsTailLines int
	DumpStacksOnTimeout  bool

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
	DSN          string `json:"dsn"`
	Readers      int    `json:"readers"`
	Writers      int    `json:"writers"`
	Duration     string `json:"duration"`
	TickInterval string `json:"tick_interval"`
	Seed         int64  `json:"seed"`
	RunDir       string `json:"run_dir"`
	DryRun       bool   `json:"dry_run"`

	RunIDOverride string `json:"run_id_override,omitempty"`
	Overwrite     bool   `json:"overwrite"`

	WorkerBin               string `json:"worker_bin"`
	WorkerHeartbeatInterval string `json:"worker_heartbeat_interval"`
	WorkerShutdownGrace     string `json:"worker_shutdown_grace"`
	WorkerShutdownKillWait  string `json:"worker_shutdown_kill_wait"`
	WorkerIgnoreInterrupt   bool   `json:"worker_ignore_interrupt"`
	WorkerMode              string `json:"worker_mode"`
	WorkerDB                string `json:"worker_db"`
	WorkerTable             string `json:"worker_table"`
	WorkerOpInterval        string `json:"worker_op_interval"`
	WorkerOpenRetry         bool   `json:"worker_open_retry"`
	WorkerSQLSession        string `json:"worker_sql_session"`
	WorkerOpTimeout         string `json:"worker_op_timeout"`

	DiagnosticsTailLines int  `json:"diagnostics_tail_lines"`
	DumpStacksOnTimeout  bool `json:"dump_stacks_on_timeout"`

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

type tickFields struct {
	Count     int   `json:"count"`
	ElapsedMs int64 `json:"elapsed_ms"`
}

type workerProc struct {
	spec workerSpec
	cmd  *exec.Cmd

	stdin     ioWriteCloser
	workerDir string

	readyCh chan struct{}
	doneCh  chan struct{}

	mu      sync.Mutex
	waitErr error
}

// ioWriteCloser is a narrow alias so types.go doesn't need to import io.
type ioWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
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
	TS       time.Time  `json:"ts"`
	WorkerID string     `json:"worker_id"`
	Role     workerRole `json:"role"`
	Reason   string     `json:"reason"`
	Signal   string     `json:"signal,omitempty"`
	Killed   bool       `json:"killed"`

	ExitError string `json:"exit_error,omitempty"`
}
