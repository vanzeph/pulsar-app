"""Free-data-source E2E engineering-correctness verification (task A9).

Full chain, fully offline, over fixtures recorded once from the free
sources (AkShare primary, baostock backup) by ``tools/make_e2e_fixtures.py``
(run manually with network access; CI never touches the network):

    data backfill (fixtures -> akshare adapter -> temp lake, zero
    unexplained gaps)
      -> dual-source path (baostock backup: cross-validation + router failover)
      -> experiment training (C3 factor pipeline from a TOML document)
      -> research backtest (BacktestVenue, bar matching over the lake port)
      -> paper simulated session (PaperBroker driven by snapshots
         synthesized from fixture bars)
      -> run artifacts (run_manifest.json + events.parquet +
         metrics_report.json, schema-validated)
      -> RunManifest reproducibility (two runs of the same config are
         bit-identical).

EXPLICIT EXCLUSION OF QUANTITATIVE BENCHMARKS: this suite proves
*engineering structure only*. It deliberately asserts **no** return,
Sharpe, drawdown, win-rate or any other performance threshold — the
acceptance criteria of task A9 forbid quantitative benchmarks. Every
assertion below checks structure, determinism, schema conformance,
state-machine legality or ledger consistency, never profitability.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd
import pytest
from pulsar_contracts import (
    AdjustMode,
    ExecutionEventType,
    Freq,
    IdempotencyKey,
    OrderIntent,
    PriceMode,
    Side,
    TimeInForce,
)

from pulsar_core import RiskGate, load_experiment, run_experiment, standard_risk_chain
from pulsar_core.artifacts import (
    EVENTS_SCHEMA_VERSION,
    EVENTS_FILENAME,
    MANIFEST_FILENAME,
    METRICS_FILENAME,
    read_event_archive,
    write_run_artifacts,
)
from pulsar_core.manifest import load_manifest
from pulsar_core.performance import load_metrics_report
from pulsar_data.backfill import BackfillRunner
from pulsar_data.crosscheck import CrossValidator
from pulsar_data.errors import FetchError
from pulsar_data.lake import DataLake
from pulsar_data.port import LakeMarketDataPort
from pulsar_data.router import DegradationLog, SourceRouter
from pulsar_data.schema import BAR_COLUMNS, Dataset
from pulsar_data.sources.base import FetchRequest
from pulsar_data.sources.baostock.adapter import BaostockSourceAdapter
from pulsar_data.sources.registry import get_adapter
from pulsar_exec import FeeSchedule, MatchingRules, PaperBroker

from harness import (  # type: ignore[import-not-found]
    FIXTURE_WINDOW,
    FixtureBaostockClient,
    bus_driven_backtest_venue,
    instruments_from_port,
    synthesize_snapshots,
)

HERE = Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "free-sources"

START, END = FIXTURE_WINDOW
#: Backtest window: leaves Jan-Feb as factor warmup inside the fixture year.
BACKTEST_START = date(2024, 3, 1)
BACKTEST_END = date(2024, 12, 31)
INITIAL_CASH = 1_000_000.0

PAPER_SYMBOLS = ("SH600036", "SZ300750")


# ---------------------------------------------------------------------------
# session fixtures: one offline backfill reused by the whole chain
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def recording() -> dict:
    manifest = json.loads((FIXTURES / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["window"] == [START.isoformat(), END.isoformat()]
    return manifest


@pytest.fixture(scope="module")
def sample_symbols(recording: dict) -> list[str]:
    symbols = recording["akshare"]["symbols"]
    assert 20 <= len(symbols) <= 30, "task A9 samples 20-30 universe names"
    return symbols


@pytest.fixture(scope="module")
def backup_symbols(recording: dict) -> list[str]:
    symbols = recording["baostock"]["symbols"]
    assert 3 <= len(symbols) <= len(recording["akshare"]["symbols"])
    return symbols


@pytest.fixture(scope="module")
def akshare_adapter() -> object:
    return get_adapter("akshare", {"fixture_dir": str(FIXTURES / "akshare")})


@pytest.fixture(scope="module")
def baostock_adapter() -> BaostockSourceAdapter:
    return BaostockSourceAdapter(client=FixtureBaostockClient(FIXTURES / "baostock"))


@pytest.fixture(scope="module")
def backfilled(tmp_path_factory: Path, akshare_adapter, sample_symbols):
    """Fixture data backfilled through the real ingestion pipeline."""
    lake = DataLake(tmp_path_factory.mktemp("e2e-lake"))
    report = BackfillRunner(akshare_adapter, lake).run(sample_symbols, START, END)
    assert not report.failed_symbols, report.failed_symbols
    return lake, report


@pytest.fixture(scope="module")
def backfill_report(backfilled):
    return backfilled[1]


@pytest.fixture(scope="module")
def lake(backfilled) -> DataLake:
    return backfilled[0]


@pytest.fixture(scope="module")
def lake_dir(backfilled) -> Path:
    return backfilled[0].root


@pytest.fixture(scope="module")
def lake_port(lake: DataLake) -> LakeMarketDataPort:
    return LakeMarketDataPort(lake)


@pytest.fixture(scope="module")
def experiment(sample_symbols: list[str], tmp_path_factory):
    """The dedicated e2e experiment document (C3 layered-config training)."""
    import tomllib

    document = (HERE / "fixtures" / "e2e_experiment.toml").read_text(encoding="utf-8")
    tree = tomllib.loads(document)
    # guard: the committed document's universe is exactly the fixture set
    assert sorted(tree["universe"]["symbols"]) == sorted(sample_symbols)
    config_path = tmp_path_factory.mktemp("e2e-config") / "e2e_experiment.toml"
    config_path.write_text(document, encoding="utf-8")
    return load_experiment(config_path)


# ---------------------------------------------------------------------------
# stage 1: data backfill from the free source
# ---------------------------------------------------------------------------
class TestBackfill:
    def test_zero_unexplained_gaps(self, backfill_report, sample_symbols):
        """Completeness vs the trading calendar: no unexplained missing bar."""
        report = backfill_report
        assert report.source == "akshare"
        assert report.calendar_rows > 230, "a full trading year of calendar rows"
        completeness = report.completeness
        gaps = {s: c for s, c in completeness.items() if c.get("gap", 0)}
        assert not gaps, gaps
        assert report.unexplained_gaps == 0

    def test_every_symbol_has_full_year_bars(self, backfill_report, sample_symbols):
        counts = backfill_report.completeness
        assert set(counts) == set(sample_symbols)
        for symbol, cells in counts.items():
            explained = cells.get("not_listed", 0) + cells.get("coverage_end", 0)
            # structure: ok bars plus explained absences tile the calendar
            assert cells["ok"] + explained >= 230, (symbol, cells)
            assert sum(cells.values()) == backfill_report.calendar_rows

    def test_lake_layout_and_watermarks(self, lake: DataLake, sample_symbols):
        bars = lake.read(Dataset.BARS_1D)
        assert set(bars["symbol"]) == set(sample_symbols)
        assert set(bars["quality"]) == {"backfilled"}, "backfill marks its rows"
        partitions = {p.name for p in (lake.root / "bars_1d").iterdir()}
        assert partitions == {f"symbol={s}" for s in sample_symbols}
        marks = lake.watermarks()
        assert not marks.empty
        assert set(marks["source"]) == {"akshare"}

    def test_dividend_and_split_samples_present(self, lake: DataLake):
        """The corporate-action coverage the task mandates: one dividend
        payer and one 2024 送转 (conversion) sample, from real source data."""
        actions = lake.read(Dataset.CORPORATE_ACTIONS)
        assert not actions.empty
        actions["ex_date"] = pd.to_datetime(actions["ex_date"])
        in_window = actions[actions["ex_date"].between(pd.Timestamp(START), pd.Timestamp(END))]
        cash = in_window[in_window["cash_dividend_per_share"] > 0]
        bonus = in_window[in_window["bonus_share_ratio"] > 0]
        assert not cash.empty, "no 2024 cash-dividend sample in the lake"
        assert not bonus.empty, "no 2024 bonus/conversion sample in the lake"

    def test_port_reads_are_deterministic(self, lake_port, sample_symbols):
        bars_a = lake_port.fetch_bars(
            sample_symbols, START, END, Freq.DAILY, AdjustMode.FORWARD
        )
        bars_b = lake_port.fetch_bars(
            sample_symbols, START, END, Freq.DAILY, AdjustMode.FORWARD
        )
        pd.testing.assert_frame_equal(bars_a, bars_b)
        calendar = lake_port.calendar(START, END)
        assert calendar == sorted(calendar) and len(calendar) > 230


# ---------------------------------------------------------------------------
# stage 2: the backup source (baostock) dual-source path
# ---------------------------------------------------------------------------
class TestBackupSource:
    def test_cross_validation_close_prices_agree(
        self, akshare_adapter, baostock_adapter, backup_symbols
    ):
        """Dual-source sampled cross-validation (D2): canonical closes agree."""
        validator = CrossValidator(akshare_adapter, baostock_adapter)
        report = validator.check(backup_symbols, START, END)
        close_events = [event for event in report.events if event.field == "close"]
        assert not close_events, close_events
        for symbol in backup_symbols:
            per_symbol = report.per_symbol[symbol]
            assert per_symbol.overlap_rows > 100, (symbol, per_symbol)

    def test_baostock_adapters_normalize_canonical(self, baostock_adapter, backup_symbols):
        for symbol in backup_symbols[:2]:
            request = FetchRequest(Dataset.BARS_1D, START, END, symbol=symbol)
            raw = baostock_adapter.fetch_raw(Dataset.BARS_1D, request)
            canonical = baostock_adapter.normalize(Dataset.BARS_1D, raw, request)
            assert not canonical.empty
            assert list(canonical.columns) == list(BAR_COLUMNS)
            assert (canonical["adjust_factor"] > 0).all()
            assert str(canonical["symbol"].iloc[0]) == symbol

    def test_router_fails_over_to_backup(self, baostock_adapter, lake_dir: Path):
        """Primary failure degrades to baostock with a durable event trail.

        The primary is the real akshare adapter with an injected upstream
        failure (the D2 drill pattern: a dead primary must never block
        the read), the backup is the real fixture-driven baostock adapter.
        """

        class DeadAkShare:
            source_id = "akshare"

            def fetch_raw(self, dataset, request):
                raise FetchError("akshare upstream dead (e2e injection)")

            def normalize(self, dataset, raw, request):  # pragma: no cover
                raise AssertionError("never reached after failover")

        log = DegradationLog(lake_dir / "_meta-test" / "degradation_events.jsonl")
        router = SourceRouter(
            [DeadAkShare(), baostock_adapter], failure_threshold=2, event_log=log
        )
        request = FetchRequest(
            Dataset.BARS_1D, date(2024, 6, 3), date(2024, 6, 28), symbol="SH600519"
        )
        raw = router.fetch_raw(Dataset.BARS_1D, request)
        assert not raw.empty, "the backup source must serve the window"
        canonical = router.normalize(Dataset.BARS_1D, raw, request)
        assert not canonical.empty
        assert router.source_id == "baostock", "serving source is the backup"
        assert router.degradations, "failover must leave a degradation event"
        assert router.degradations[0].from_source == "akshare"
        assert router.degradations[0].to_source == "baostock"
        persisted = log.read()
        assert persisted, "degradation events are durable on disk"
        assert persisted[-1]["from_source"] == "akshare"


# ---------------------------------------------------------------------------
# stage 3: experiment training + research backtest + artifacts
# ---------------------------------------------------------------------------
def make_venue_factory(lake_port, symbols):
    def factory(bus):
        return bus_driven_backtest_venue(
            bus,
            initial_cash=INITIAL_CASH,
            instruments=instruments_from_port(lake_port, symbols, BACKTEST_START),
            start=BACKTEST_START,
        )

    return factory


def run_research_backtest(lake_port, experiment_config):
    return run_experiment(
        experiment_config,
        port=lake_port,
        venue=make_venue_factory(lake_port, experiment_config.symbols),
        initial_cash=INITIAL_CASH,
        gate=RiskGate(standard_risk_chain()),
    )


@pytest.fixture(scope="module")
def research_run(lake_port, experiment):
    """One research backtest shared by the structural assertions."""
    result = run_research_backtest(lake_port, experiment)
    assert result.run.trading_days > 190
    return result


class TestResearchBacktest:
    def test_experiment_training_resolved_pipeline(self, experiment, sample_symbols):
        """The TOML resolved into the registered C3 building blocks."""
        assert experiment.experiment_id == "e2e_free_source_2024"
        assert sorted(experiment.symbols) == sorted(sample_symbols)
        assert experiment.factor_names == ("momentum_20", "volatility_20", "reversal_5")
        assert [step.name for step in experiment.preprocess] == ["winsorize", "zscore"]
        assert experiment.rebalance == "monthly"
        assert experiment.start == BACKTEST_START and experiment.end == BACKTEST_END

    def test_factor_model_produced_targets_and_orders(self, research_run, experiment):
        result = research_run
        # the factor pipeline trained: every scheduled rebalance got weights
        rebalance_days = result.strategy.rebalance_days
        assert rebalance_days, "monthly schedule over Mar-Dec 2024"
        assert all(result.strategy._targets[day] for day in rebalance_days)  # noqa: SLF001
        # orders flowed through Signal -> TargetPortfolio -> RiskGate -> intent
        assert result.runtime.submissions, "the pipeline must emit order intents"
        for submission in result.runtime.submissions:
            assert submission.intent.idempotency_key.run_id == result.run_id
            assert submission.intent.quantity % 100 == 0 or submission.intent.side is Side.SELL

    def test_venue_matched_orders_with_fees(self, research_run):
        fills = [e.fill for e in venue_events(research_run) if e.fill is not None]
        assert fills, "the BacktestVenue must fill intents"
        for fill in fills:
            assert fill.price > 0 and fill.quantity > 0
            assert fill.commission >= 0 and fill.stamp_duty >= 0
            assert fill.transfer_fee >= 0

    def test_runtime_and_venue_ledgers_agree(self, research_run):
        venue = research_run.runtime._port  # noqa: SLF001
        venue_positions = {p.symbol: p for p in venue.positions()}
        account = research_run.runtime.account.snapshot()
        fills = [e.fill for e in venue_events(research_run) if e.fill is not None]
        for position in account.positions:
            venue_side = venue_positions.get(position.symbol)
            assert venue_side is not None, position.symbol
            assert venue_side.quantity == position.quantity
        assert fills, "fee accrual requires fills"
        total_fees = sum(
            f.commission + f.stamp_duty + f.transfer_fee for f in fills
        )
        assert total_fees > 0, "per-fill fee accrual must book costs"

    def test_artifacts_three_files_schema_valid(self, research_run, tmp_path: Path):
        result = research_run
        out = tmp_path / "run"
        artifacts = write_run_artifacts(
            result.run,
            events=result.runtime._bus.journal,  # noqa: SLF001
            initial_cash=INITIAL_CASH,
            directory=out,
        )
        # 1) manifest
        manifest_path = out / MANIFEST_FILENAME
        assert manifest_path.is_file()
        reloaded = load_manifest(manifest_path)
        assert reloaded == result.run.manifest
        assert reloaded.run_id == result.run_id
        assert reloaded.mode == "research"
        assert reloaded.config["experiment"]["id"] == "e2e_free_source_2024"
        assert reloaded.config["session"]["kind"] == "bar_replay"
        # 2) events archive
        events_path = out / EVENTS_FILENAME
        assert events_path.is_file()
        import pyarrow.parquet as pq

        table = pq.read_table(events_path)
        assert set(table.schema.names) == {
            "schema_version", "run_id", "seq", "ts", "kind", "event_type",
            "symbol", "side", "price", "quantity", "commission", "stamp_duty",
            "transfer_fee", "payload",
        }
        assert set(table.column("schema_version").to_pylist()) == {EVENTS_SCHEMA_VERSION}
        replayed = read_event_archive(events_path)
        assert list(replayed) == list(result.runtime._bus.journal)  # noqa: SLF001
        # 3) metrics report
        metrics_path = out / METRICS_FILENAME
        assert metrics_path.is_file()
        report = load_metrics_report(metrics_path)
        assert report.run_id == result.run_id
        assert report.initial_cash == INITIAL_CASH
        assert report.journal_digest == result.run.journal_digest
        # structure only: one start point plus one close per trading day
        assert len(report.equity_curve) == result.run.trading_days + 1
        assert report.equity_curve[0].kind == "start"
        assert report.equity_curve[0].nav == pytest.approx(1.0)
        assert all(point.kind == "eod" for point in report.equity_curve[1:])
        assert artifacts.report == report
        # NOTE: no return/sharpe/drawdown threshold is asserted anywhere.


def venue_events(run_result):
    """Execution events captured from the venue through the run's bus."""
    from pulsar_core.events import EventKind

    return [
        event.execution
        for event in run_result.runtime._bus.journal  # noqa: SLF001
        if event.kind is EventKind.EXECUTION and event.execution is not None
    ]


# ---------------------------------------------------------------------------
# stage 4: RunManifest reproducibility (the acceptance headline)
# ---------------------------------------------------------------------------
class TestReproducibility:
    def test_same_config_two_runs_bit_identical(self, lake_port, experiment):
        first = run_research_backtest(lake_port, experiment)
        second = run_research_backtest(lake_port, experiment)

        # manifest fingerprint: same identity document, byte for byte
        assert first.run_id == second.run_id
        assert first.run.manifest.to_json() == second.run.manifest.to_json()
        assert first.run.manifest.data_watermarks == second.run.manifest.data_watermarks

        # event journal: identical dispatch digest and identical events
        assert first.run.journal_digest == second.run.journal_digest
        from pulsar_core.bus import canonical_event_json

        assert [canonical_event_json(e) for e in first.runtime._bus.journal] == [  # noqa: SLF001
            canonical_event_json(e) for e in second.runtime._bus.journal  # noqa: SLF001
        ]

        # fills: identical trade blotters
        def blotter(result):
            return [
                (
                    fill.symbol,
                    fill.side.value,
                    fill.quantity,
                    fill.price,
                    fill.commission,
                    fill.stamp_duty,
                    fill.transfer_fee,
                    fill.ts.isoformat(),
                )
                for fill in (
                    event.fill for event in venue_events(result) if event.fill is not None
                )
            ]

        assert blotter(first) == blotter(first)  # sanity
        assert blotter(first) == blotter(second)

        # net-value curve + metrics: bit-identical JSON documents
        report_a = report_of(first)
        report_b = report_of(second)
        assert report_a.to_json() == report_b.to_json()
        assert [p.nav for p in report_a.equity_curve] == [
            p.nav for p in report_b.equity_curve
        ]
        # the equity curve is a full-window curve (structure, not level)
        assert len(report_a.equity_curve) == first.run.trading_days + 1


def report_of(result):
    from pulsar_core.performance import build_metrics_report

    return build_metrics_report(
        result.runtime._bus.journal,  # noqa: SLF001
        initial_cash=INITIAL_CASH,
        run_id=result.run_id,
        journal_digest=result.run.journal_digest,
    )


# ---------------------------------------------------------------------------
# stage 5: paper simulated session (PaperBroker, snapshot driven)
# ---------------------------------------------------------------------------
class TestPaperSession:
    @pytest.fixture()
    def paper_setup(self, lake_port):
        """Synthesize the snapshot stream from fixture bars (never network).

        One stream over the whole window so per-symbol ``seq`` increases
        monotonically across days (late/duplicate seq are dropped by the
        broker), then grouped per trading day for session driving.
        """
        frame = lake_port.fetch_bars(
            list(PAPER_SYMBOLS), date(2024, 3, 25), date(2024, 4, 2), Freq.DAILY, AdjustMode.RAW
        )
        from pulsar_core.session import _bars_from_frame

        bars = sorted(_bars_from_frame(frame, Freq.DAILY), key=lambda b: (b.ts, b.symbol))
        instruments = instruments_from_port(lake_port, PAPER_SYMBOLS, date(2024, 3, 25))
        by_day: dict[date, list] = {}
        for snapshot in synthesize_snapshots(bars):
            by_day.setdefault(snapshot.ts.date(), []).append(snapshot)
        return bars, instruments, by_day

    def test_snapshot_driven_orders_and_ledger(self, paper_setup):
        bars, instruments, snapshots = paper_setup
        symbol = PAPER_SYMBOLS[0]
        bar_days: dict[date, list] = {}
        for bar in bars:
            bar_days.setdefault(bar.ts.date(), []).append(bar)
        days = sorted(snapshots)

        paper = PaperBroker(
            initial_cash=INITIAL_CASH,
            instruments=instruments,
            matching=MatchingRules(strict_price_boundary=True),
            fee_schedule=FeeSchedule(),
            clock=datetime.combine(days[0], time(9, 15)),
        )
        events: list = []
        paper.on_event(events.append)
        paper.start(now=datetime.combine(days[0], time(9, 15)))
        paper.set_previous_close(symbol, bar_days[days[0]][0].close)

        run = "e2e-paper"
        seq = 0

        def intent(side, qty, limit) -> OrderIntent:
            nonlocal seq
            seq += 1
            return OrderIntent(
                idempotency_key=IdempotencyKey(run_id=run, seq=seq),
                side=side,
                symbol=symbol,
                quantity=qty,
                price_mode=PriceMode.LIMIT,
                limit_price=limit,
                time_in_force=TimeInForce.DAY,
            )

        def types_for(order_id):
            return [e.event_type for e in events if e.order_id == order_id]

        # --- day 1: market opens, first book arrives --------------------
        books = [s for s in snapshots[days[0]] if s.symbol == symbol]
        paper.on_snapshot(books[0])

        # penetrating limit buy (strictly above best ask) fills on the next book
        buy = intent(Side.BUY, 1_000, round(books[0].asks[0].price + 0.10, 2))
        buy_id = paper.submit(buy)
        assert types_for(buy_id) == [ExecutionEventType.ACCEPTED]
        paper.on_snapshot(books[1])
        assert ExecutionEventType.FILL in types_for(buy_id)
        state = paper.query(buy_id)
        assert state.filled_quantity == 1_000
        assert state.status.value == "filled"

        # T+1: the freshly bought shares are not sellable today
        sell_now = intent(Side.SELL, 1_000, round(books[1].bids[0].price - 0.10, 2))
        sell_now_id = paper.submit(sell_now)
        assert ExecutionEventType.REJECTED in types_for(sell_now_id)

        # idempotency: replaying the same intent returns the same order id
        assert paper.submit(buy) == buy_id
        accepted_count = types_for(buy_id).count(ExecutionEventType.ACCEPTED)
        assert accepted_count == 1, "replayed intents create no duplicate orders"

        # a resting below-market buy (inside the day's price band) cancels
        resting = intent(Side.BUY, 100, round(books[1].bids[0].price * 0.95, 2))
        resting_id = paper.submit(resting)
        assert types_for(resting_id) == [ExecutionEventType.ACCEPTED]
        assert paper.cancel(resting_id).accepted is True
        assert ExecutionEventType.CANCELLED in types_for(resting_id)

        # --- day 1 close: T+1 rolls, DAY orders expire ------------------
        positions_day1 = {p.symbol: p for p in paper.positions()}
        assert positions_day1[symbol].quantity == 1_000
        assert positions_day1[symbol].available_quantity == 0, "T+1 same-day hold"
        paper.on_session_end(days[0])
        rolled = {p.symbol: p for p in paper.positions()}
        assert rolled[symbol].available_quantity == 1_000, "T+1 availability roll"

        # --- day 2: the sell now fills against the book ------------------
        day2_books = [s for s in snapshots[days[1]] if s.symbol == symbol]
        paper.on_snapshot(day2_books[0])
        sell = intent(Side.SELL, 1_000, round(day2_books[0].bids[0].price - 0.10, 2))
        sell_id = paper.submit(sell)
        paper.on_snapshot(day2_books[1])
        assert ExecutionEventType.FILL in types_for(sell_id)

        paper.on_session_end(days[1])
        paper.stop(reason="e2e complete")

        # --- ledger consistency: positions and cash reconcile with fills --
        fills = [e.fill for e in events if e.fill is not None]
        assert len(fills) >= 2
        final_positions = {p.symbol: p for p in paper.positions()}
        for held, quantity in _net_fill_quantities(fills).items():
            view = final_positions.get(held)
            assert (view.quantity if view else 0) == quantity
        expected_cash = _cash_from_fills(fills)
        assert paper.cash == pytest.approx(expected_cash, abs=0.02), (
            f"ledger {paper.cash} != fills-derived {expected_cash}"
        )
        # every order ends in a terminal state of the shared state machine
        for order_id in {e.order_id for e in events}:
            assert paper.query(order_id).status.is_terminal

    def test_paper_channel_state_transitions_legal(self, paper_setup):
        """Event sequences stay inside the shared order state machine."""
        from pulsar_exec.state_machine import (
            EVENT_ALLOWED_CURRENT,
            EVENT_TARGET_STATUS,
            OrderStatus,
        )

        bars, instruments, snapshots = paper_setup
        symbol = PAPER_SYMBOLS[1]
        days = sorted(snapshots)
        books = [s for s in snapshots[days[0]] if s.symbol == symbol]

        paper = PaperBroker(
            initial_cash=INITIAL_CASH,
            instruments=instruments,
            matching=MatchingRules(strict_price_boundary=True),
        )
        events: list = []
        paper.on_event(events.append)
        paper.start(now=datetime.combine(days[0], time(9, 30)))
        paper.on_snapshot(books[0])
        paper.submit(
            OrderIntent(
                idempotency_key=IdempotencyKey(run_id="e2e-paper-2", seq=1),
                side=Side.BUY,
                symbol=symbol,
                quantity=200,
                price_mode=PriceMode.LIMIT,
                limit_price=round(books[0].asks[0].price + 1.0, 2),
            )
        )
        paper.on_snapshot(books[1])
        paper.on_session_end(days[0])
        paper.stop()

        by_order: dict[str, list] = {}
        for event in events:
            by_order.setdefault(str(event.order_id), []).append(event)
        assert by_order, "the session must produce at least one order trail"
        for order_id, trail in by_order.items():
            status = OrderStatus.CREATED
            for event in trail:
                target = EVENT_TARGET_STATUS[event.event_type]
                assert status in EVENT_ALLOWED_CURRENT[event.event_type], (
                    f"illegal transition {status.value} --{event.event_type.value}--> "
                    f"for order {order_id}"
                )
                if target is not None:
                    status = target
            assert status.is_terminal, (order_id, status)


def _net_fill_quantities(fills) -> dict[str, int]:
    net: dict[str, int] = {}
    for fill in fills:
        sign = 1 if fill.side is Side.BUY else -1
        net[fill.symbol] = net.get(fill.symbol, 0) + sign * fill.quantity
    return net


def _cash_from_fills(fills) -> float:
    cash = INITIAL_CASH
    for fill in fills:
        value = round(fill.price * fill.quantity, 2)
        fees = round(fill.commission + fill.stamp_duty + fill.transfer_fee, 2)
        cash += value - fees if fill.side is Side.SELL else -(value + fees)
        cash = round(cash, 2)
    return cash
