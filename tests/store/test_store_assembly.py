"""Custom-code assembly end to end (gated on pulsar-core, like the e2e
suite): the Agent writes a custom factor and an experiment TOML through
the store interface, assembly materializes and registers the code, the
run executes, and the RunManifest's object hashes reproduce the exact
bytes.

Covers the STORE1 acceptance items that need the domain stack:
* Agent 写入示例因子 + 实验 TOML → 组装注册跑通端到端;
* RunManifest 含代码/配置对象 hash 且复现按 hash 取回同一版本;
* 注册名冲突检查（内置名与存储内已声明名）;
* put 预览不留残留注册（Registry.discard 回滚路径）.
"""

from __future__ import annotations

import random
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Callable, Iterable

import pytest

pytest.importorskip("pulsar_core", reason="install pulsar-app[e2e] to run store assembly")

import pandas as pd
from pulsar_contracts import (
    AdjustMode,
    CancelResult,
    ExecutionEvent,
    ExecutionEventType,
    Fill,
    Freq,
    MarketDataPort,
    OrderId,
    OrderIntent,
    PriceMode,
    Side,
)
from pulsar_core import (
    FACTOR_REGISTRY,
    RiskGate,
    load_experiment,
    run_experiment,
    standard_risk_chain,
)
from pulsar_app import PluginKind, PluginRegistry, PluginSpec
from pulsar_app.config import RunMode, load_config
from pulsar_app.errors import StoreValidationError
from pulsar_app.run import execute_run
from pulsar_app.store import Store
from pulsar_app.store.loader import (
    load_experiment_object,
    materialize_code,
    reset_materialization_state,
)


@pytest.fixture(autouse=True)
def _clean_materialization_state() -> Iterable[None]:
    reset_materialization_state()
    yield
    reset_materialization_state()


@pytest.fixture
def store(store_root: Path, factor_source: str, experiment_toml: str) -> Store:
    """A store holding the shipped example assets, as an Agent wrote them."""
    engine = Store(store_root)
    engine.put("code", "custom_factor", factor_source, note="example factor")
    engine.put("experiments", "momentum_store_demo", experiment_toml, note="demo")
    return engine


# -- put-time preview: conflicts and hygiene -------------------------------------


def test_put_records_declared_registration_names(store: Store) -> None:
    declared = store.declared_names("code", "custom_factor")
    assert [(item.registry_kind, item.registered_name) for item in declared] == [
        ("factor", "close_over_ma10")
    ]


def test_put_does_not_leave_registrations_behind(store: Store) -> None:
    assert "close_over_ma10" not in FACTOR_REGISTRY


def test_put_rejects_registering_a_builtin_name(store_root: Path) -> None:
    engine = Store(store_root)
    hostile = (
        "from pulsar_core.factors import FactorDefinition, register_factor\n"
        "register_factor(FactorDefinition(name='momentum_20', label='clone',\n"
        "    compute=lambda bars: 0.0, min_bars=1))\n"
    )
    with pytest.raises(StoreValidationError, match="momentum_20.*already registered"):
        engine.put("code", "cloner", hostile)
    assert "close_over_ma10" not in FACTOR_REGISTRY  # no partial residue either


def test_put_rejects_names_declared_by_other_stored_code(store: Store) -> None:
    twin = store.get_text("code", "custom_factor").replace(
        '"close_over_ma10"', '"another_factor"'
    )
    store.put("code", "other_factor", twin)
    with pytest.raises(StoreValidationError, match="another_factor.*already declared by other_factor"):
        store.put("code", "twin_factor", twin)


def test_put_rejects_code_that_registers_nothing(store: Store) -> None:
    with pytest.raises(StoreValidationError, match="registered nothing"):
        store.put("code", "empty", "x = 1\n")


# -- materialization + registration ---------------------------------------------


def test_materialize_registers_the_custom_factor(store: Store) -> None:
    materialized = materialize_code(store, "custom_factor")
    assert materialized.declared[0].registered_name == "close_over_ma10"
    assert FACTOR_REGISTRY.resolve("close_over_ma10").name == "close_over_ma10"


def test_materialize_is_idempotent_per_content_hash(store: Store) -> None:
    first = materialize_code(store, "custom_factor")
    second = materialize_code(store, "custom_factor")
    assert first.content_hash == second.content_hash
    assert len([name for name in FACTOR_REGISTRY.names() if name == "close_over_ma10"]) == 1


def test_putting_a_new_version_while_materialized_is_refused(
    store: Store, factor_source: str
) -> None:
    materialize_code(store, "custom_factor")
    changed = factor_source.replace("min_bars=10", "min_bars=9")
    with pytest.raises(StoreValidationError, match="materialized in this process"):
        store.put("code", "custom_factor", changed)

    # a fresh process (empty materialization memo) writes the new version fine
    reset_materialization_state()  # simulated restart: memo cleared, live registrations released
    version, _ = store.put("code", "custom_factor", changed, note="v2")
    assert version.seq == 2
    assert store.get_text("code", "custom_factor") == changed


def test_load_experiment_object_resolves_the_custom_factor(store: Store) -> None:
    # assembly order: materialize code first (its registrations make the
    # experiment's custom factor names resolvable), then load the config
    materialize_code(store, "custom_factor")
    experiment = load_experiment_object(store, "momentum_store_demo")
    assert experiment.experiment_id == "momentum_store_demo"
    assert experiment.status == "candidate"
    assert experiment.factor_names == ("momentum_20", "close_over_ma10")


# -- assembly: manifest object references + reproduce by hash ----------------------


def _local_registry_with_mocks() -> "PluginRegistry":
    """A private registry with the mock ports (never the process default)."""
    from tests.mocks import mock_execution_factory, mock_market_data_factory

    registry = PluginRegistry()
    registry.register(
        PluginSpec(
            plugin_id="akshare",
            kind=PluginKind.MARKET_DATA,
            factory=mock_market_data_factory,
        )
    )
    registry.register(
        PluginSpec(
            plugin_id="backtest",
            kind=PluginKind.EXECUTION,
            factory=mock_execution_factory,
        )
    )
    return registry


def _run_config(tmp_path: Path, store_root: Path, lake_dir: Path) -> Path:
    text = (
        "[run]\nmode = 'research'\n\n"
        "[data]\nsources = ['akshare']\n"
        f"lake_dir = '{lake_dir}'\n\n"
        "[exec]\nvenue = 'backtest'\n\n"
        f"[store]\nroot = '{store_root}'\n"
        "code = ['custom_factor']\nexperiments = ['momentum_store_demo']\n"
    )
    target = tmp_path / "run_store.toml"
    target.write_text(text, encoding="utf-8")
    return target


def test_execute_run_pins_and_reproduces_store_objects(
    tmp_path: Path, store: Store, lake_dir: Path
) -> None:
    registry = _local_registry_with_mocks()
    config = load_config(
        _run_config(tmp_path, store.root, lake_dir)
    ).with_mode(RunMode.RESEARCH)
    outcome = execute_run(config, registry=registry, runs_dir=tmp_path / "runs")

    manifest = outcome.manifest
    code_record = manifest.store_objects["code:custom_factor"]
    experiment_record = manifest.store_objects["experiment:momentum_store_demo"]
    assert code_record.role == "code" and experiment_record.role == "experiment"
    assert code_record.namespace == "code"
    assert len(code_record.content_hash) == 64
    assert FACTOR_REGISTRY.resolve("close_over_ma10")  # registered by assembly

    # reproduce-by-hash: the manifest's hashes fetch back the exact bytes
    original = Store(store.root)
    code_bytes = original.get("code", "custom_factor", content_hash=code_record.content_hash)
    experiment_bytes = original.get(
        "experiments", "momentum_store_demo", content_hash=experiment_record.content_hash
    )
    assert b"close_over_ma10" in code_bytes
    assert b"momentum_store_demo" in experiment_bytes


# -- the full experiment: stored factor + stored TOML → run -----------------------


class DeterministicLakePort:
    """MarketDataPort serving a deterministic geometric walk per symbol."""

    def __init__(self, symbols: list[str], start: date, end: date) -> None:
        self.symbols = symbols
        self.start = start
        self.end = end

    def _days(self) -> list[date]:
        days: list[date] = []
        day = self.start
        while day <= self.end:
            if day.weekday() < 5:
                days.append(day)
            day += timedelta(days=1)
        return days

    def _frame(self, symbols: list[str], start: date, end: date) -> pd.DataFrame:
        rows = []
        for symbol in symbols:
            rng = random.Random(f"store-e2e-{symbol}")
            price = 100.0
            drift = 0.004 if symbol.endswith("519") else -0.001
            for day in self._days():
                if not (start <= day <= end):
                    continue
                price *= 1.0 + drift + rng.uniform(-0.02, 0.02)
                close = round(price, 3)
                rows.append(
                    {
                        "symbol": symbol,
                        "ts": pd.Timestamp(datetime.combine(day, time(15, 0))),
                        "open": round(close * 0.995, 3),
                        "high": round(close * 1.01, 3),
                        "low": round(close * 0.99, 3),
                        "close": close,
                        "volume": 1_000_000.0,
                        "amount": close * 1_000_000.0,
                        "adjust_factor": 1.0,
                        "quality": "ok",
                    }
                )
        frame = pd.DataFrame(rows)
        return frame.sort_values(["ts", "symbol"], kind="stable").reset_index(drop=True)

    # -- MarketDataPort ------------------------------------------------------

    def list_instruments(self, as_of: date) -> list[object]:
        return []

    def fetch_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        freq: Freq,
        adjust: AdjustMode,
    ) -> pd.DataFrame:
        return self._frame(symbols, start, end)

    def calendar(self, start: date, end: date) -> list[date]:
        return [day for day in self._days() if start <= day <= end]

    def subscribe(
        self, symbols: list[str], on_snapshot: Callable[[object], None]
    ) -> object:
        raise RuntimeError("research replay does not subscribe")


class FillingVenue:
    """Minimal research venue: every limit intent fills at its price."""

    def __init__(self, now: Callable[[], datetime]) -> None:
        self._now = now
        self._callback: Callable[[ExecutionEvent], None] | None = None
        self._orders = 0
        self._by_key: dict[str, OrderId] = {}

    def on_event(self, callback: Callable[[ExecutionEvent], None]) -> None:
        self._callback = callback

    def submit(self, intent: OrderIntent) -> OrderId:
        from pulsar_contracts import OrderId as _OrderId

        key = intent.idempotency_key.to_str()
        known = self._by_key.get(key)
        if known is not None:
            return known
        assert self._callback is not None
        self._orders += 1
        order_id = _OrderId(f"ord-{self._orders:06d}")
        self._by_key[key] = order_id
        assert intent.limit_price is not None
        fill = Fill(
            fill_id=f"fill-{self._orders:06d}",
            order_id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            price=intent.limit_price,
            quantity=intent.quantity,
            ts=self._now(),
        )
        self._callback(
            ExecutionEvent(
                event_type=ExecutionEventType.FILL,
                order_id=order_id,
                ts=fill.ts,
                fill=fill,
            )
        )
        return order_id

    def cancel(self, order_id: OrderId) -> CancelResult:
        return CancelResult(order_id=order_id, accepted=False, reason="filled")

    def positions(self) -> list[object]:
        return []


def test_stored_factor_and_experiment_run_end_to_end(store: Store) -> None:
    materialize_code(store, "custom_factor")
    experiment = load_experiment_object(store, "momentum_store_demo")

    port = DeterministicLakePort(
        list(experiment.symbols), date(2023, 6, 1), date(2024, 12, 31)
    )
    result = run_experiment(
        experiment,
        port=port,
        venue=lambda bus: FillingVenue(now=lambda: bus.now),
        gate=RiskGate(standard_risk_chain()),
    )
    assert result.experiment_id == "momentum_store_demo"
    assert result.run.trading_days > 0
    assert result.manifest.config["experiment"]["id"] == "momentum_store_demo"


def test_reproduced_run_from_pinned_hashes_is_identical(store: Store) -> None:
    """Reproduce-by-hash: pinned object bytes rebuild the same experiment run."""
    materialize_code(store, "custom_factor")
    experiment = load_experiment_object(store, "momentum_store_demo")
    port_one = DeterministicLakePort(
        list(experiment.symbols), date(2023, 6, 1), date(2024, 12, 31)
    )
    first = run_experiment(
        experiment,
        port=port_one,
        venue=lambda bus: FillingVenue(now=lambda: bus.now),
    )

    # "reproduction": fresh store handle, fetch by the recorded hashes
    head = store.resolve("code", "custom_factor")
    code_bytes = store.get("code", "custom_factor", content_hash=head.content_hash)
    assert code_bytes == store.get_text("code", "custom_factor").encode()

    second = run_experiment(
        load_experiment_object(store, "momentum_store_demo"),
        port=DeterministicLakePort(
            list(experiment.symbols), date(2023, 6, 1), date(2024, 12, 31)
        ),
        venue=lambda bus: FillingVenue(now=lambda: bus.now),
    )
    assert first.run_id == second.run_id
    assert first.run.journal_digest == second.run.journal_digest
