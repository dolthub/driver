package main

import "time"

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

	OpenRetry  bool
	SQLSession string
	OpTimeout  time.Duration
}

type event struct {
	TS       time.Time `json:"ts"`
	RunID    string    `json:"run_id,omitempty"`
	WorkerID string    `json:"worker_id,omitempty"`
	Role     string    `json:"role,omitempty"`

	Event  string `json:"event"`
	Fields any    `json:"fields,omitempty"`
}

type errInfo struct {
	Kind      string `json:"kind"`      // lock|timeout|sql|other
	Retryable bool   `json:"retryable"` // whether we will/should retry within op timeout
	Message   string `json:"message"`
}
