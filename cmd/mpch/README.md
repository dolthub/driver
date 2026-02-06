# mpch (multi-process concurrency harness)

`mpch` is a small orchestration tool for reproducing and debugging multi-process concurrency issues when using the embedded Dolt driver (`github.com/dolthub/driver`).

It creates a deterministic “plan” (a worker manifest), optionally performs setup (SQL schema creation), then spawns many **OS processes** (`worker`) that execute a workload against a **single on-disk Dolt directory**. It writes **structured JSONL** logs to stdout and persists per-run artifacts for later analysis.

This is intentionally “process-y” (not goroutine-y) so we can reproduce process-level locking behavior and failures (locks, timeouts, connection open issues, etc.).

---

## Components

- `cmd/mpch` (this tool)
  - **Orchestrator**: plans setup/run/teardown, spawns workers, enforces timeouts, writes artifacts.

- `cmd/worker`
  - **Worker process**: does the actual work (SQL `INSERT` for writers, `SELECT COUNT(*)` for readers) and reports progress via JSONL.

---

## Quick start (SQL mode)

Build binaries:

```bash
go build -o ./test/bin/mpch ./cmd/mpch
go build -o ./test/bin/worker ./cmd/worker
```

Run a 5 minute scenario (10 writers, 5 readers) against a Dolt directory:

```bash
DSN="file://$(pwd)/test/dbs?commitname=MPCH&commitemail=mpch@example.com"
./test/bin/mpch \
  --dsn "$DSN" \
  --writers 10 --readers 5 \
  --duration 5m \
  --tick-interval 5s \
  --worker-mode sql \
  --worker-db mpch_test \
  --worker-table harness_events \
  --worker-op-interval 100ms \
  --worker-op-timeout 10s \
  --worker-sql-session per_op \
  --worker-open-retry=true \
  --worker-bin ./test/bin/worker \
  --worker-heartbeat-interval 5s \
  --run-dir ./test/runs \
  --run-id run_10w_5r_5m \
  --overwrite \
  --dry-run=false
```

Notes:
- `--dry-run=false` enables side effects (writing artifacts, running setup, spawning workers).
- `--run-id` controls the artifact directory name (`--overwrite` allows replacing an existing run directory).
- `--seed` controls deterministic worker IDs / manifest planning (default is time-based).

---

## Output

### JSONL events on stdout

`mpch` writes newline-delimited JSON to stdout for machine parsing / correlation. Each line is an event with:

- `ts`: UTC timestamp
- `run_id`: stable run identifier
- `phase`: `plan` | `setup` | `run` | `teardown` | `control` | `artifacts`
- `event`: event name within the phase
- `fields`: optional structured payload

Examples of important events:
- `plan/plan`: full resolved configuration (including defaulted timeouts)
- `setup/manifest`: the worker manifest (IDs, roles)
- `run/worker_spawned`, `run/worker_ready`, `run/all_started`
- `run/tick`: periodic ticks during the run phase
- `control/interrupt`: received SIGINT/SIGTERM
- `artifacts/meta_written`, `artifacts/manifest_written`, `artifacts/diagnostics_written`

Workers also emit JSONL to their own stdout (captured into artifacts). Worker events include:
- `ready`, `started`, `heartbeat`
- `op_ok`, `op_error`
- (sometimes) `open_retry`, `op_retry` if the worker itself retries within an op’s timeout budget

### Run artifacts on disk

When `--dry-run=false`, `mpch` creates:

```
<run-dir>/<run-id>/
  meta.json
  manifest.json
  workers/
    <worker-id>/
      stdout.jsonl
      stderr.log
      termination.json
  diagnostics/                     # written only on timeout-like failures
    mpch_stacks.txt
    workers/<worker-id>/*_tail.*
```

- `meta.json`: final run summary (plan + phase start/end timestamps + exit code)
- `manifest.json`: deterministic worker list for reproduction
- `workers/*/stdout.jsonl`: structured worker events for per-worker analysis
- `workers/*/termination.json`: how teardown ended for each worker (graceful vs killed)
- `diagnostics/*`: written for “timeout-like” failures (stack dump + tailed logs)

---

## Key flags / knobs

### Orchestration

- `--run-dir`: base directory for artifacts (default `./runs`)
- `--run-id`: stable identifier (optional). If omitted, `mpch` generates one.
- `--overwrite`: delete existing `<run-dir>/<run-id>` before running
- `--seed`: deterministic planning (stable worker IDs for a given seed)
- `--duration`: how long the run phase should last
- `--tick-interval`: how frequently `mpch` emits `run/tick` events

### Worker spawning / lifecycle

- `--worker-bin`: path/name of the worker executable to spawn
- `--worker-heartbeat-interval`: worker heartbeat cadence
- `--worker-shutdown-grace`: how long to wait after SIGINT before escalating
- `--worker-shutdown-kill-wait`: how long to wait after force-kill before failing teardown

On Unix, `mpch` puts workers in their own process groups so it can kill the entire group if needed. On Windows, process-group kill isn’t available; `mpch` uses best-effort interrupt and then force-kills processes.

### Workload (SQL mode)

- `--worker-mode=sql`: enables SQL workload mode in workers
- `--worker-db`, `--worker-table`: where workers operate
- `--worker-sql-session`:
  - `per_op`: open/close per operation
  - `per_run`: open once per worker run
- `--worker-op-interval`: frequency of operations per worker
- `--worker-op-timeout`: context timeout per operation (critical when lock contention is high)
- `--worker-open-retry`:
  - If true, workers set `embedded.Config.BackOff`, enabling retry during embedded engine open (bounded by the op context).

---

## Notes on lock contention

When multiple OS processes open the embedded engine in read-write mode against the same on-disk Dolt directory, lock contention is expected. In practice:

- **Short op timeouts** surface lots of `lock` and `timeout` errors quickly.
- **Longer op timeouts** often yield higher `op_ok` throughput but can hide the raw rate of lock contention (ops wait longer before failing).

If you want to reproduce “connection open failures”, keep `--worker-op-timeout` relatively short; if you want stable throughput, increase `--worker-op-timeout` and/or increase `--worker-op-interval`.

---

## Smoke script

The repo includes an idempotent helper script:

- `scripts/mpch_sql_smoke.sh`

It builds binaries under `./test/bin`, runs a scenario under `./test/{dbs,runs}`, and prints a short summary of worker outcomes. It supports environment overrides:

- `MPCH_TEST_DIR`
- `MPCH_WRITERS`, `MPCH_READERS`, `MPCH_DURATION`
- `MPCH_RUN_ID` (optional override; otherwise derived from the scenario)

