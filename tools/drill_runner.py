"""Local load-and-train drill runner (task DR1): research run over a real lake.

This is the *drill* companion of ``tools/drill_local.sh`` — assembly-level
glue only, no domain logic (mirroring ``tests/e2e/harness.py``, which
documents why this wiring lives beside the tests/tools rather than in a
domain package: pulsar-app's package body is the assembly skeleton and
deliberately ships no plugins).

Two stages, matching the architecture baseline's "插件注册与运行时装配":

1. **pulsar-app CLI, research mode** — register the drill's plugins into
   the process-wide default registry (``akshare`` → the lake read-side
   port, ``backtest`` → the bar-matching venue) and invoke the real
   ``pulsar research`` entry point. The assembler verifies port
   conformance against the contracts and archives an app-level
   RunManifest under ``<runs_dir>/<run_id>/manifest.json``.
2. **experiment training + research backtest** — load the experiment
   TOML (C3 layered config, C6 ``status`` field enforced), train the
   modeler, replay the window through a bus-driven ``BacktestVenue``
   and persist the three run artifacts
   (``run_manifest.json`` / ``events.parquet`` / ``metrics_report.json``)
   under ``<runs_dir>/<run_id>/``.

Usage:

    python tools/drill_runner.py --lake ./data/lake \
        --experiment ./experiments/drill.toml \
        --run-config ./experiments/drill_run.toml --runs-dir ./runs
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from pulsar_app import PluginKind, PluginSpec
from pulsar_app.cli import main as pulsar_cli_main
from pulsar_app.registry import DEFAULT_REGISTRY
from pulsar_contracts import Instrument
from pulsar_core import RiskGate, load_experiment, run_experiment, standard_risk_chain
from pulsar_core.artifacts import write_run_artifacts
from pulsar_core.bus import EventBus
from pulsar_core.events import Event, EventKind, SessionPhase
from pulsar_data.port import LakeMarketDataPort
from pulsar_exec import BacktestVenue

_TZ = ZoneInfo("Asia/Shanghai")
_SESSION_CLOSE = time(15, 0)
_CLOSE_TIMER = "venue.session_close"


# ---------------------------------------------------------------------------
# stage 1: plugin registration + the real `pulsar research` CLI
# ---------------------------------------------------------------------------
def register_drill_plugins(lake_dir: str, initial_cash: float) -> None:
    """Populate the process-wide registry with the drill's plugins.

    Exactly the pattern from the app README: factories receive the
    resolved parameter table (plus the shared ``lake_dir`` for
    market-data plugins) and must return port-conformant objects.
    """

    def akshare_factory(lake_dir: str, **_params: object) -> LakeMarketDataPort:
        return LakeMarketDataPort(lake_dir)

    def backtest_factory(**params: object) -> BacktestVenue:
        cash = float(params.pop("initial_cash", initial_cash))
        return BacktestVenue(initial_cash=cash)

    DEFAULT_REGISTRY.register(
        PluginSpec(
            plugin_id="akshare",
            kind=PluginKind.MARKET_DATA,
            factory=akshare_factory,
            description="drill: lake read-side MarketDataPort",
        )
    )
    DEFAULT_REGISTRY.register(
        PluginSpec(
            plugin_id="backtest",
            kind=PluginKind.EXECUTION,
            factory=backtest_factory,
            description="drill: bar-matching BacktestVenue",
        )
    )


# ---------------------------------------------------------------------------
# stage 2 glue: drive one BacktestVenue from the kernel bus
# (same driving model as tests/e2e/harness.py — the replay loop feeds the
# venue; day closes expire DAY orders and roll T+1 inside the day window)
# ---------------------------------------------------------------------------
@dataclass
class _VenueDriver:
    venue: BacktestVenue
    bus: EventBus
    current_day: date | None = None
    current_bar: object | None = None

    def on_market(self, event: Event) -> None:
        bar = event.bar
        assert bar is not None  # MARKET envelope invariant
        day = bar.ts.date()
        if day != self.current_day:
            self.current_day = day
            self.bus.publish(
                Event.timer_event(
                    datetime.combine(day, _SESSION_CLOSE, tzinfo=_TZ),
                    _CLOSE_TIMER,
                    {"day": day.isoformat()},
                )
            )
        self.current_bar = bar
        self.venue.on_bar(bar)

    def on_late(self, event: Event) -> None:
        bar = self.current_bar
        if event.kind is EventKind.MARKET and event.bar is not None and event.bar is bar:
            self.venue.on_bar(bar)

    def on_event(self, event: Event) -> None:
        if event.kind is EventKind.TIMER and event.timer is not None:
            if event.timer.name == _CLOSE_TIMER:
                day = date.fromisoformat(str(event.timer.data["day"]))
                self.venue.on_session_end(day)
            return
        if event.kind is EventKind.SESSION and event.session_phase is SessionPhase.FINISHED:
            if self.current_day is not None:
                self.venue.on_session_end(self.current_day)


def bus_driven_backtest_venue(
    bus: EventBus,
    *,
    initial_cash: float,
    instruments: Iterable[Instrument],
    start: date,
) -> BacktestVenue:
    """Build a BacktestVenue wired onto ``bus`` (kind handlers first)."""
    venue = BacktestVenue(
        initial_cash=initial_cash,
        instruments=instruments,
        clock=datetime.combine(start, time(0, 0), tzinfo=_TZ),
    )
    driver = _VenueDriver(venue=venue, bus=bus)
    bus.subscribe(EventKind.MARKET, driver.on_market)
    bus.subscribe(EventKind.TIMER, driver.on_event)
    bus.subscribe(EventKind.SESSION, driver.on_event)
    bus.subscribe_all(driver.on_late)
    return venue


def instruments_from_port(port: LakeMarketDataPort, symbols: list[str], as_of: date):
    """Instrument metadata via the port, with the venue's own fallback."""
    from pulsar_contracts import Board, Exchange

    listed = {i.symbol: i for i in port.list_instruments(as_of)}
    out = []
    for symbol in symbols:
        instrument = listed.get(symbol)
        if instrument is None:
            instrument = Instrument(
                symbol=symbol,
                exchange=Exchange.SSE,
                board=Board.MAIN,
                list_date=date(1990, 12, 19),
            )
        out.append(instrument)
    return out


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="drill-runner",
        description="DR1 drill: assemble via `pulsar research`, train + backtest an experiment.",
    )
    parser.add_argument("--lake", required=True, help="lake root (data/lake)")
    parser.add_argument("--experiment", required=True, help="experiment TOML path")
    parser.add_argument(
        "--run-config",
        help="optional app run TOML for the `pulsar research` assembly stage",
    )
    parser.add_argument("--runs-dir", default="runs", help="runs output root")
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0)
    args = parser.parse_args(argv)

    runs_dir = Path(args.runs_dir)
    runs_dir.mkdir(parents=True, exist_ok=True)

    # -- stage 1: the app CLI in research mode (assembly + manifest) -------
    if args.run_config:
        register_drill_plugins(str(Path(args.lake).resolve()), args.initial_cash)
        print("== stage 1: pulsar-app CLI, research mode (assembly + manifest) ==")
        rc = pulsar_cli_main(
            ["research", "--config", args.run_config, "--runs-dir", str(runs_dir)]
        )
        if rc != 0:
            print(f"stage 1 failed with exit code {rc}", file=sys.stderr)
            return rc

    # -- stage 2: experiment training + research backtest -------------------
    print("== stage 2: experiment training + research backtest ==")
    experiment = load_experiment(Path(args.experiment))
    print(
        f"experiment {experiment.experiment_id} status={experiment.status} "
        f"universe={len(experiment.symbols)} factors={experiment.factor_names} "
        f"model={experiment.model.name if hasattr(experiment.model, 'name') else type(experiment.model).__name__} "
        f"rebalance={experiment.rebalance} window=[{experiment.start}, {experiment.end}]"
    )

    port = LakeMarketDataPort(args.lake)

    def venue_factory(bus: EventBus) -> BacktestVenue:
        return bus_driven_backtest_venue(
            bus,
            initial_cash=args.initial_cash,
            instruments=instruments_from_port(port, list(experiment.symbols), experiment.start),
            start=experiment.start,
        )

    result = run_experiment(
        experiment,
        port=port,
        venue=venue_factory,
        initial_cash=args.initial_cash,
        gate=RiskGate(standard_risk_chain()),
    )

    out = runs_dir / result.run_id
    artifacts = write_run_artifacts(
        result.run,
        events=result.runtime._bus.journal,  # noqa: SLF001 - same seam the e2e suite uses
        initial_cash=args.initial_cash,
        directory=out,
    )

    fills = [e.fill for e in artifacts_report_events(result) if e.fill is not None]
    print(f"run_id={result.run_id} trading_days={result.run.trading_days} fills={len(fills)}")
    print(f"artifacts written to {out}")
    for name in ("run_manifest.json", "events.parquet", "metrics_report.json"):
        path = out / name
        print(f"  {name}: {'OK' if path.is_file() else 'MISSING'} ({path.stat().st_size if path.is_file() else 0} bytes)")

    report = json.loads((out / "metrics_report.json").read_text(encoding="utf-8"))
    metrics = report.get("metrics", {})
    curve = report.get("equity_curve", [])
    if metrics:
        keys = (
            "total_return", "annual_return", "sharpe", "max_drawdown",
            "turnover_ratio", "fills", "final_nav",
        )
        summary = " ".join(f"{k}={metrics.get(k)}" for k in keys)
        print(f"metrics: {summary}")
    else:
        print(f"metrics keys: {sorted(report)}")
    if curve:
        print(f"equity curve points: {len(curve)} (first nav={curve[0]['nav']}, last nav={curve[-1]['nav']})")
    return 0


def artifacts_report_events(result):
    """Execution events from the run's journal (fills for the summary)."""
    from pulsar_core.events import EventKind

    return [
        event.execution
        for event in result.runtime._bus.journal  # noqa: SLF001
        if event.kind is EventKind.EXECUTION and event.execution is not None
    ]


if __name__ == "__main__":
    sys.exit(main())
