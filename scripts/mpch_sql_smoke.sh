#!/usr/bin/env bash
set -euo pipefail

# Idempotent mpch SQL-mode run script.
# - Builds fresh binaries under ./test/bin
# - Runs mpch in --worker-mode=sql against ./test/dbs
# - Stores run artifacts under ./test/runs
# - Prints a summary of ops/retries/errors from worker logs
#
# Usage (from repo root):
#   ./scripts/mpch_sql_smoke.sh
#
# Optional overrides:
#   MPCH_TEST_DIR=./test ./scripts/mpch_sql_smoke.sh

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_DIR="${MPCH_TEST_DIR:-$ROOT/test}"
OUT_BASE="$TEST_DIR"
BIN_DIR="$OUT_BASE/bin"
DBS_DIR="$OUT_BASE/dbs"
RUNS_DIR="$OUT_BASE/runs"
RUN_ID="${MPCH_RUN_ID:-run_5w_3r_5m}"

echo "[mpch-smoke] repo: $ROOT"
echo "[mpch-smoke] test: $TEST_DIR"

# Idempotent: wipe just our subdirs / logs, keep $TEST_DIR itself.
rm -rf "$BIN_DIR" "$DBS_DIR" "$RUNS_DIR" "$OUT_BASE"/mpch.*.jsonl "$OUT_BASE"/mpch.*.stderr
mkdir -p "$BIN_DIR" "$DBS_DIR" "$RUNS_DIR"

echo "[mpch-smoke] building binaries..."
go build -o "$BIN_DIR/mpch" "$ROOT/cmd/mpch"
go build -o "$BIN_DIR/worker" "$ROOT/cmd/worker"

DSN="file://$DBS_DIR?commitname=MPCH&commitemail=mpch@example.com"

OUT_JSONL="$OUT_BASE/mpch.$RUN_ID.jsonl"
OUT_STDERR="$OUT_BASE/mpch.$RUN_ID.stderr"

echo "[mpch-smoke] running mpch (sql mode) run_id=$RUN_ID writers=5 readers=3 duration=5m..."
set +e
"$BIN_DIR/mpch" \
  --dsn "$DSN" \
  --writers 5 --readers 3 \
  --seed 123 \
  --duration 5m \
  --tick-interval 5s \
  --worker-mode sql \
  --worker-db mpch_test \
  --worker-table harness_events \
  --worker-op-interval 100ms \
  --worker-op-timeout 500ms \
  --worker-sql-session per_op \
  --worker-open-retry=true \
  --worker-bin "$BIN_DIR/worker" \
  --worker-heartbeat-interval 5s \
  --run-dir "$RUNS_DIR" \
  --dry-run=false \
  --run-id "$RUN_ID" \
  --overwrite \
  >"$OUT_JSONL" 2>"$OUT_STDERR"
EXIT_CODE=$?
set -e

echo "[mpch-smoke] mpch exit: $EXIT_CODE"
RUN_PATH="$RUNS_DIR/$RUN_ID"

if [[ "$EXIT_CODE" -ne 0 ]]; then
  echo "[mpch-smoke] NOTE: non-zero exit; check diagnostics under $RUN_PATH"
fi

echo "[mpch-smoke] verifying artifacts..."
test -s "$RUN_PATH/meta.json" || { echo "[mpch-smoke] FAIL: missing $RUN_PATH/meta.json"; exit 1; }
test -s "$RUN_PATH/manifest.json" || { echo "[mpch-smoke] FAIL: missing $RUN_PATH/manifest.json"; exit 1; }
test -d "$RUN_PATH/workers" || { echo "[mpch-smoke] FAIL: missing $RUN_PATH/workers"; exit 1; }

if ! command -v python3 >/dev/null 2>&1; then
  echo "[mpch-smoke] FAIL: python3 is required for summary parsing"
  exit 1
fi

echo "[mpch-smoke] summarizing worker results..."
python3 - "$RUN_PATH" <<'PY'
import glob, json, os, sys

run_path = sys.argv[1]
workers_dir = os.path.join(run_path, "workers")

files = sorted(glob.glob(os.path.join(workers_dir, "*", "stdout.jsonl")))
if not files:
  print("[mpch-smoke] FAIL: no worker stdout.jsonl files found", file=sys.stderr)
  sys.exit(1)

tot = {k:0 for k in ["op_ok","op_error","op_retry","open_retry","worker_exit_early","heartbeat"]}
kinds = {}

max_ops_ok = 0
max_ops_err = 0

for path in files:
  with open(path, "r", encoding="utf-8") as f:
    for line in f:
      line = line.strip()
      if not line:
        continue
      try:
        ev = json.loads(line)
      except Exception:
        continue
      name = ev.get("event")
      if name in tot:
        tot[name] += 1
      if name == "heartbeat":
        fields = ev.get("fields") or {}
        max_ops_ok = max(max_ops_ok, int(fields.get("ops_ok") or 0))
        max_ops_err = max(max_ops_err, int(fields.get("ops_err") or 0))
      if name == "op_error":
        fields = ev.get("fields") or {}
        e = fields.get("error") or {}
        if isinstance(e, dict):
          kind = e.get("kind") or "unknown"
          kinds[kind] = kinds.get(kind, 0) + 1

print("[mpch-smoke] summary:")
print("  max_ops_ok:", max_ops_ok)
print("  max_ops_err:", max_ops_err)
print("  event_counts:", tot)
print("  op_error_kinds:", dict(sorted(kinds.items(), key=lambda kv: (-kv[1], kv[0]))))

if max_ops_ok <= 0:
  print("[mpch-smoke] WARN: no ops_ok observed (check logs; likely lock contention)", file=sys.stderr)
PY

echo "[mpch-smoke] done"
echo "[mpch-smoke] mpch jsonl: $OUT_JSONL"
echo "[mpch-smoke] run dir:   $RUN_PATH"

