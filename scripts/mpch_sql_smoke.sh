#!/usr/bin/env bash
set -euo pipefail

# Idempotent SQL-mode smoke test for mpch + worker.
# - Builds fresh binaries
# - Runs mpch in --worker-mode=sql against a fresh local directory
# - Verifies artifacts and basic forward progress
#
# Usage (from repo root):
#   ./scripts/mpch_sql_smoke.sh
#
# Optional overrides:
#   MPCH_OUT_BASE=/tmp/mpch-smoke ./scripts/mpch_sql_smoke.sh

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_BASE="${MPCH_OUT_BASE:-/tmp/mpch-smoke}"
BIN_DIR="$OUT_BASE/bin"
DBS_DIR="$OUT_BASE/dbs"
RUNS_DIR="$OUT_BASE/runs"

echo "[mpch-smoke] repo: $ROOT"
echo "[mpch-smoke] out:  $OUT_BASE"

rm -rf "$OUT_BASE"
mkdir -p "$BIN_DIR" "$DBS_DIR" "$RUNS_DIR"

echo "[mpch-smoke] building binaries..."
go build -o "$BIN_DIR/mpch" "$ROOT/cmd/mpch"
go build -o "$BIN_DIR/worker" "$ROOT/cmd/worker"

DSN="file://$DBS_DIR?commitname=MPCH&commitemail=mpch@example.com"

run_case() {
  local run_id="$1"
  local readers="$2"
  local writers="$3"
  local require_ops_ok="$4" # "true"|"false"

  local out_jsonl="$OUT_BASE/mpch.$run_id.jsonl"
  local out_stderr="$OUT_BASE/mpch.$run_id.stderr"

  echo "[mpch-smoke] running mpch (sql mode) run_id=$run_id readers=$readers writers=$writers..."
  set +e
  "$BIN_DIR/mpch" \
    --dsn "$DSN" \
    --readers "$readers" --writers "$writers" \
    --seed 123 \
    --duration 4s \
    --tick-interval 1s \
    --worker-mode sql \
    --worker-db mpch_test \
    --worker-table harness_events \
    --worker-op-interval 100ms \
    --worker-op-timeout 500ms \
    --worker-sql-session per_op \
    --worker-open-retry=true \
    --worker-bin "$BIN_DIR/worker" \
    --worker-heartbeat-interval 500ms \
    --run-dir "$RUNS_DIR" \
    --dry-run=false \
    --run-id "$run_id" \
    --overwrite \
    >"$out_jsonl" 2>"$out_stderr"
  local exit_code=$?
  set -e

  echo "[mpch-smoke] mpch exit: $exit_code"
  if [[ "$exit_code" -ne 0 ]]; then
    echo "[mpch-smoke] FAIL: mpch exited non-zero for run_id=$run_id"
    echo "[mpch-smoke] stdout: $out_jsonl"
    echo "[mpch-smoke] stderr: $out_stderr"
    exit 1
  fi

  local run_path="$RUNS_DIR/$run_id"
  local meta="$run_path/meta.json"
  local manifest="$run_path/manifest.json"
  local workers_dir="$run_path/workers"

  echo "[mpch-smoke] verifying artifacts for run_id=$run_id..."
  test -s "$meta" || { echo "[mpch-smoke] FAIL: missing meta.json at $meta"; exit 1; }
  test -s "$manifest" || { echo "[mpch-smoke] FAIL: missing manifest.json at $manifest"; exit 1; }
  test -d "$workers_dir" || { echo "[mpch-smoke] FAIL: missing workers dir at $workers_dir"; exit 1; }

  shopt -s nullglob
  local worker_dirs=("$workers_dir"/*)
  if [[ "${#worker_dirs[@]}" -lt 1 ]]; then
    echo "[mpch-smoke] FAIL: expected at least 1 worker directory"
    exit 1
  fi
  for wd in "${worker_dirs[@]}"; do
    test -s "$wd/stdout.jsonl" || { echo "[mpch-smoke] FAIL: missing $wd/stdout.jsonl"; exit 1; }
  done

  echo "[mpch-smoke] verifying worker heartbeats (and ops_ok if required)..."
  if ! command -v python3 >/dev/null 2>&1; then
    echo "[mpch-smoke] FAIL: python3 is required for verification parsing"
    exit 1
  fi

  MPCH_OUT_BASE="$OUT_BASE" python3 - "$run_id" "$require_ops_ok" <<'PY'
import glob, json, os, sys

run_id = sys.argv[1]
require_ops_ok = (sys.argv[2].lower() == "true")

out_base = os.environ["MPCH_OUT_BASE"]
workers_dir = os.path.join(out_base, "runs", run_id, "workers")

files = sorted(glob.glob(os.path.join(workers_dir, "*", "stdout.jsonl")))
if not files:
  print("[mpch-smoke] FAIL: no worker stdout.jsonl files found", file=sys.stderr)
  sys.exit(1)

total_heartbeats = 0
max_ok = 0
max_err = 0

for path in files:
  heartbeats = 0
  file_max_ok = 0
  file_max_err = 0

  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      try:
        ev = json.loads(line)
      except Exception:
        continue
      if ev.get("event") != "heartbeat":
        continue
      fields = ev.get("fields") or {}
      ok = int(fields.get("ops_ok") or 0)
      er = int(fields.get("ops_err") or 0)
      file_max_ok = max(file_max_ok, ok)
      file_max_err = max(file_max_err, er)
      heartbeats += 1

  total_heartbeats += heartbeats
  max_ok = max(max_ok, file_max_ok)
  max_err = max(max_err, file_max_err)

  if heartbeats < 1:
    print(f"[mpch-smoke] FAIL: no heartbeat events found in {path}", file=sys.stderr)
    sys.exit(1)

if require_ops_ok and max_ok <= 0:
  print(f"[mpch-smoke] FAIL: require ops_ok>0 but observed max_ok={max_ok} (max_err={max_err})", file=sys.stderr)
  sys.exit(1)

print(f"[mpch-smoke] OK: heartbeats={total_heartbeats} max_ops_ok={max_ok} max_ops_err={max_err}")
PY

  echo "[mpch-smoke] OK: run_id=$run_id"
  echo "[mpch-smoke] mpch jsonl: $out_jsonl"
  echo "[mpch-smoke] run dir:   $run_path"
}

# Case A: single writer only — should show at least some ops_ok > 0.
run_case "smoke_sql_writer_only" 0 1 true

# Case B: 1 writer + readers — validates multi-process orchestration + basic liveness.
run_case "smoke_sql_rw" 2 1 false

echo "[mpch-smoke] ALL OK"

