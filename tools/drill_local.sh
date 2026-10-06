#!/usr/bin/env bash
# Pulsar local load-and-train drill (task DR1): steps a-d of the drill,
# repeatable from zero. Docs companion: docs/QUICKSTART.md (this script is
# its one-button form); Windows notes: docs/WINDOWS-READINESS.md.
#
#   bash tools/drill_local.sh [workspace_dir]
#
# What it does, all inside <workspace_dir> (never inside any repository):
#   a. fresh venv + one-command install of the locked six-repo stack
#      (pulsar-contracts/tools/release/requirements-lock.txt) + the akshare
#      live-source extra (+ tzdata when running under native Windows);
#   b. real-network backfill of ~20 symbols x ~1 year into data/lake with a
#      completeness report (zero unexplained gaps required);
#   c. write the experiment TOML (status = candidate, C6) + the app run
#      config, then train + backtest in research mode via tools/drill_runner.py
#      (which first drives the real `pulsar research` assembly CLI);
#   d. verify the three artifacts' schema, render report.html and smoke-start
#      pulsar-ui (127.0.0.1 only, stopped right after probing).
#
# Overridable via environment:
#   DRILL_START / DRILL_END       data window      (default 2025-10-01 / 2026-09-30)
#   DRILL_BT_START / DRILL_BT_END backtest window  (default 2026-03-01 / 2026-09-30)
#   DRILL_SYMBOLS                 comma list       (default: the 20-symbol drill set)
#   DRILL_PORT                    UI smoke port    (default 7800)
#   DRILL_FRESH=1                 delete .venv/data/lake/runs/experiments first
#
# Windows (git-bash): same command; tzdata is installed automatically
# (required there, see WINDOWS-READINESS R1). Backtest defaults assume the
# default data window — override DRILL_BT_* together with DRILL_* if you
# shift the data window (leave >= 3 months for the ic_weighted warmup).

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"          # .../pulsar-app/tools
APP_REPO="$(dirname "$HERE")"
WORKSPACE="${1:-$(pwd)}"
LOCK_FILE="${DRILL_LOCK:-$APP_REPO/../pulsar-contracts/tools/release/requirements-lock.txt}"
RUNNER="$HERE/drill_runner.py"

DRILL_START="${DRILL_START:-2025-10-01}"
DRILL_END="${DRILL_END:-2026-09-30}"
DRILL_BT_START="${DRILL_BT_START:-2026-03-01}"
DRILL_BT_END="${DRILL_BT_END:-2026-09-30}"
DRILL_PORT="${DRILL_PORT:-7800}"
DRILL_SYMBOLS="${DRILL_SYMBOLS:-SH600519,SH600036,SH601318,SH600276,SH601899,SH603288,SH601127,SH600900,SH601088,SH601012,SZ000001,SZ000858,SZ002415,SZ002594,SZ000651,SZ000333,SZ300750,SZ300059,SH688012,SH688111}"

log() { printf '\n[drill %s] %s\n' "$(date +%H:%M:%S)" "$*"; }

mkdir -p "$WORKSPACE"/{data,experiments,runs}
cd "$WORKSPACE"

# venv binary layout per platform (native Windows keeps Scripts/, not bin/)
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) VENV_BIN=.venv/Scripts; WINDOWS_NATIVE=1 ;;
    *)                    VENV_BIN=.venv/bin;    WINDOWS_NATIVE=0 ;;
esac
PY="$VENV_BIN/python"
PIP="$VENV_BIN/pip"

if [[ "${DRILL_FRESH:-0}" == "1" ]]; then
    log "DRILL_FRESH=1: wiping .venv data/lake runs experiments"
    rm -rf .venv data/lake runs experiments
    mkdir -p data experiments runs
fi

# ---------------------------------------------------------------- a. install
if [[ ! -f "$LOCK_FILE" ]]; then
    echo "lock file not found: $LOCK_FILE" >&2
    echo "clone pulsar-contracts next to pulsar-app, or set DRILL_LOCK" >&2
    exit 2
fi

if [[ ! -x "$PY" ]]; then
    log "a. creating fresh venv"
    PYTHON_BIN="$(command -v python3 || command -v python)"
    "$PYTHON_BIN" -m venv .venv
    "$PIP" install --upgrade pip -q
else
    log "a. reusing existing venv (.venv)"
fi

log "a. one-command install of the locked six-repo stack"
TIME_START=$(date +%s)
"$PIP" install -r "$LOCK_FILE"
TIME_END=$(date +%s)
log "a. locked stack installed in $((TIME_END-TIME_START))s"

# akshare extra for the live free source (pinned to the lock's data anchor)
DATA_ANCHOR="$(grep '^pulsar-data @' "$LOCK_FILE" | head -1 | sed 's/^pulsar-data @ //')"
"$PIP" install "pulsar-data[akshare] @ $DATA_ANCHOR" -q

# native Windows git-bash needs tzdata (WINDOWS-READINESS R1)
if [[ "$WINDOWS_NATIVE" == "1" ]]; then
    log "a. Windows detected: installing tzdata"
    "$PIP" install tzdata -q
fi

"$PY" - <<'PY'
import pulsar_contracts, pulsar_core, pulsar_data, pulsar_exec, pulsar_app, pulsar_ui
print("a. all six packages import:",
      pulsar_contracts.__version__, pulsar_core.__version__,
      pulsar_data.__version__, pulsar_exec.__version__,
      pulsar_app.__version__, pulsar_ui.__version__)
PY

# ---------------------------------------------------------------- b. backfill
log "b. backfilling $DRILL_START .. $DRILL_END from akshare (real network, rate-limited)"
TIME_START=$(date +%s)
"$VENV_BIN/pulsar-data" backfill --source akshare --lake data/lake \
    --start "$DRILL_START" --end "$DRILL_END" \
    --symbols "$DRILL_SYMBOLS" --report data/backfill-report.json
TIME_END=$(date +%s)
"$PY" - "$DRILL_SYMBOLS" <<'PY'
import json, sys

symbols = sys.argv[1].split(",")
report = json.load(open("data/backfill-report.json"))
failed = report["failed_symbols"]
gaps = report["unexplained_gaps"]
comp = report["completeness"]
ok_bars = sum(c.get("ok", 0) for c in comp.values())
print(f"b. completeness: symbols_ok={len(comp)}/{len(symbols)} ok_bars={ok_bars} "
      f"unexplained_gaps={gaps} failed={failed or 'none'}")
assert gaps == 0, "backfill left unexplained gaps"
assert not failed, f"backfill failed for {failed}"
assert len(comp) >= 19, "expected ~20 symbols in the lake"
PY
log "b. backfill done in $((TIME_END-TIME_START))s (window $DRILL_START..$DRILL_END)"

# ------------------------------------------------- c. experiment + research run
log "c. writing experiment/run configs and running training + backtest (research)"
SYMBOLS_TOML="$("$PY" -c 'import sys; print(", ".join(f"\"{s}\"" for s in sys.argv[1].split(",")))' "$DRILL_SYMBOLS")"

cat > experiments/drill_momentum.toml <<TOML
# Generated by pulsar-app tools/drill_local.sh (DR1). status is the C6
# lifecycle field; research mode requires candidate or active.
[experiment]
id = "drill_momentum_2026"
description = "DR1 drill: momentum/low-vol/reversal, IC-weighted, top-5 monthly"
status = "candidate"

[universe]
symbols = [$SYMBOLS_TOML]

[factors]
names = ["momentum_20", "volatility_20", "reversal_5"]
preprocess = ["winsorize", "zscore"]

[model]
type = "ic_weighted"
params = { lookback = 60, horizon = 5, min_points = 10 }

[portfolio]
method = "top_n"
top_n = 5
rebalance = "monthly"

[backtest]
start = $DRILL_BT_START
end = $DRILL_BT_END
costs = "a_share_default"
seed = 7
TOML

cat > experiments/drill_run.toml <<'TOML'
# Generated by pulsar-app tools/drill_local.sh (DR1): app-level assembly
# config consumed by the `pulsar research` stage of tools/drill_runner.py.
[run]
mode = "research"

[data]
sources = ["akshare"]
lake_dir = "data/lake"

[exec]
venue = "backtest"
TOML

"$PY" "$RUNNER" \
    --lake data/lake \
    --experiment experiments/drill_momentum.toml \
    --run-config experiments/drill_run.toml \
    --runs-dir runs | tee data/drill-run.log

RUN_DIR="$(grep -Eo 'runs/[0-9a-f]+' data/drill-run.log | head -1)"
[[ -n "$RUN_DIR" ]] || { echo "could not locate run dir from drill log" >&2; exit 1; }
log "c. run directory: $RUN_DIR"

# ----------------------------------------------------------- d. artifact checks
log "d. verifying the three artifacts (schema + reload)"
"$PY" - "$RUN_DIR" <<'PY'
import sys
from pathlib import Path
import pyarrow.parquet as pq
from pulsar_core.artifacts import EVENTS_SCHEMA_VERSION, read_event_archive
from pulsar_core.manifest import load_manifest
from pulsar_core.performance import load_metrics_report

run_dir = Path(sys.argv[1])
manifest = load_manifest(run_dir / "run_manifest.json")
assert manifest.mode == "research"
assert manifest.config["experiment"]["status"] == "candidate"
table = pq.read_table(run_dir / "events.parquet")
assert set(table.column("schema_version").to_pylist()) == {EVENTS_SCHEMA_VERSION}
events = read_event_archive(run_dir / "events.parquet")
report = load_metrics_report(run_dir / "metrics_report.json")
assert report.run_id == manifest.run_id
assert len(report.equity_curve) == report.metrics.trading_days + 1
print(f"d. artifacts OK: run_id={manifest.run_id} events={len(events)} "
      f"fills={report.metrics.fills} trading_days={report.metrics.trading_days}")
PY

log "d. rendering report.html"
"$PY" -m pulsar_ui.report "$RUN_DIR"
[[ -f "$RUN_DIR/report.html" ]] || { echo "report.html missing" >&2; exit 1; }

log "d. pulsar-ui smoke (127.0.0.1:$DRILL_PORT, stopped after probing)"
"$VENV_BIN/pulsar-ui" --port "$DRILL_PORT" --runs-dir runs --lake-dir data/lake \
    > data/pulsar-ui.log 2>&1 &
UI_PID=$!
trap 'kill "$UI_PID" 2>/dev/null || true' EXIT
READY=0
for _ in $(seq 1 30); do
    if curl -sf -o /dev/null "http://127.0.0.1:$DRILL_PORT/api/runs"; then READY=1; break; fi
    sleep 1
done
[[ "$READY" == "1" ]] || { echo "pulsar-ui did not come up (see data/pulsar-ui.log)" >&2; exit 1; }
curl -sf -o /dev/null -w "d. GET /          -> %{http_code}\n" "http://127.0.0.1:$DRILL_PORT/"
curl -sf -o /dev/null -w "d. GET /api/runs  -> %{http_code}\n" "http://127.0.0.1:$DRILL_PORT/api/runs"
curl -sf -o /dev/null -w "d. GET /trades    -> %{http_code}\n" "http://127.0.0.1:$DRILL_PORT/trades"
curl -sf -o /dev/null -w "d. GET /lake      -> %{http_code}\n" "http://127.0.0.1:$DRILL_PORT/lake"
kill "$UI_PID" && wait "$UI_PID" 2>/dev/null || true
trap - EXIT
log "d. UI stopped"

log "drill complete: artifacts in $WORKSPACE/$RUN_DIR (report.html alongside)"
