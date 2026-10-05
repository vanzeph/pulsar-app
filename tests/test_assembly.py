"""Runtime assembly: plugin resolution, live gate, conformance, watermarks."""

from __future__ import annotations

from datetime import date

import pytest

from pulsar_app import (
    AssembledRun,
    ConfigError,
    LiveModeLockedError,
    PluginError,
    PluginKind,
    PluginSpec,
    PortConformanceError,
    RunMode,
    UnknownPluginError,
    assemble_run,
    collect_data_watermarks,
    parse_toml_config,
)
from pulsar_app.assembly import SupportsWatermark

from .conftest import registry_with_mocks
from .mocks import (
    MockExecutionPort,
    MockMarketDataPort,
    PlainMockMarketDataPort,
    mock_market_data_factory,
)

LIVE_CONFIG = """
[run]
mode = "live"
seed = 7

[data]
sources = ["akshare"]

[exec]
venue = "miniqmt"

[exec.miqmt]
unlock_env = "PULSAR_TEST_LIVE_CONFIRM"
max_order_value_cny = 5000.0
"""


def test_assemble_example_config_orders_primary_and_backups(example_config) -> None:
    assembled = assemble_run(example_config, registry_with_mocks(), env={})
    assert isinstance(assembled, AssembledRun)
    assert isinstance(assembled.market_data, MockMarketDataPort)
    assert len(assembled.backup_market_data) == 1
    assert assembled.all_market_data()[0] is assembled.market_data
    assert isinstance(assembled.execution, MockExecutionPort)
    assert assembled.mode is RunMode.RESEARCH
    assert assembled.seed == 0


def test_market_data_factory_receives_lake_dir_and_resolved_params() -> None:
    registry = registry_with_mocks()
    config = parse_toml_config(
        """
[run]
mode = "research"

[data]
sources = ["akshare"]
lake_dir = "./custom/lake"

[data.akshare]
token = "${PULSAR_TEST_TOKEN}"
timeout = 30

[exec]
venue = "backtest"
"""
    )
    assembled = assemble_run(config, registry, env={"PULSAR_TEST_TOKEN": "secret-value"})
    primary = assembled.market_data
    assert isinstance(primary, MockMarketDataPort)
    assert primary.lake_dir == "./custom/lake"
    assert primary.params == {"token": "secret-value", "timeout": 30}


def test_execution_factory_receives_venue_params() -> None:
    registry = registry_with_mocks()
    config = parse_toml_config(
        """
[run]
mode = "paper"

[data]
sources = ["akshare"]

[exec]
venue = "paper"

[exec.paper]
fee_rate = 0.0001
"""
    )
    assembled = assemble_run(config, registry, env={})
    assert isinstance(assembled.execution, MockExecutionPort)
    assert assembled.execution.params == {"fee_rate": 0.0001}


def test_missing_referenced_env_variable_fails_assembly() -> None:
    registry = registry_with_mocks()
    config = parse_toml_config(
        """
[run]
mode = "research"

[data]
sources = ["akshare"]

[data.akshare]
token = "${PULSAR_TEST_UNSET}"

[exec]
venue = "backtest"
"""
    )
    with pytest.raises(ConfigError, match="PULSAR_TEST_UNSET"):
        assemble_run(config, registry, env={})


def test_unknown_source_id_is_reported() -> None:
    registry = registry_with_mocks()
    config = parse_toml_config(
        """
[run]
mode = "research"

[data]
sources = ["tushare"]

[exec]
venue = "backtest"
"""
    )
    with pytest.raises(UnknownPluginError, match="tushare"):
        assemble_run(config, registry, env={})


def test_wrong_kind_registration_is_reported() -> None:
    registry = registry_with_mocks()
    registry.unregister("backtest", PluginKind.EXECUTION)
    registry.register(
        PluginSpec(
            plugin_id="backtest",
            kind=PluginKind.MARKET_DATA,  # wrong kind on purpose
            factory=mock_market_data_factory,
        )
    )
    config = parse_toml_config(
        '[run]\nmode = "research"\n[data]\nsources = ["akshare"]\n[exec]\nvenue = "backtest"\n'
    )
    with pytest.raises(UnknownPluginError):
        assemble_run(config, registry, env={})


def test_factory_result_must_satisfy_market_data_protocol() -> None:
    registry = registry_with_mocks()
    registry.unregister("akshare", PluginKind.MARKET_DATA)
    registry.register(
        PluginSpec(
            plugin_id="akshare",
            kind=PluginKind.MARKET_DATA,
            factory=lambda **kwargs: object(),  # no port methods
        )
    )
    config = parse_toml_config(
        '[run]\nmode = "research"\n[data]\nsources = ["akshare"]\n[exec]\nvenue = "backtest"\n'
    )
    with pytest.raises(PortConformanceError, match="MarketDataPort"):
        assemble_run(config, registry, env={})


def test_factory_result_must_satisfy_execution_protocol() -> None:
    registry = registry_with_mocks()
    registry.unregister("backtest", PluginKind.EXECUTION)
    registry.register(
        PluginSpec(
            plugin_id="backtest",
            kind=PluginKind.EXECUTION,
            factory=lambda **kwargs: object(),
        )
    )
    config = parse_toml_config(
        '[run]\nmode = "research"\n[data]\nsources = ["akshare"]\n[exec]\nvenue = "backtest"\n'
    )
    with pytest.raises(PortConformanceError, match="ExecutionPort"):
        assemble_run(config, registry, env={})


def test_factory_rejecting_kwargs_is_a_plugin_error() -> None:
    registry = registry_with_mocks()
    registry.unregister("akshare", PluginKind.MARKET_DATA)

    def strict_factory(**kwargs: object) -> object:
        raise TypeError("unexpected keyword argument 'token'")

    registry.register(
        PluginSpec(plugin_id="akshare", kind=PluginKind.MARKET_DATA, factory=strict_factory)
    )
    config = parse_toml_config(
        '[run]\nmode = "research"\n[data]\nsources = ["akshare"]\n[exec]\nvenue = "backtest"\n'
    )
    with pytest.raises(PluginError, match="akshare"):
        assemble_run(config, registry, env={})


class TestLiveGate:
    def test_live_locked_when_env_absent(self) -> None:
        config = parse_toml_config(LIVE_CONFIG)
        with pytest.raises(LiveModeLockedError, match="PULSAR_TEST_LIVE_CONFIRM"):
            assemble_run(config, registry_with_mocks(), env={})

    def test_live_locked_when_env_not_confirmed(self) -> None:
        config = parse_toml_config(LIVE_CONFIG)
        with pytest.raises(LiveModeLockedError):
            assemble_run(config, registry_with_mocks(), env={"PULSAR_TEST_LIVE_CONFIRM": "no"})

    @pytest.mark.parametrize("confirm", ["1", "true", "YES", "on"])
    def test_live_unlocks_with_explicit_confirmation(self, confirm: str) -> None:
        config = parse_toml_config(LIVE_CONFIG)
        assembled = assemble_run(
            config, registry_with_mocks(), env={"PULSAR_TEST_LIVE_CONFIRM": confirm}
        )
        assert assembled.mode is RunMode.LIVE
        assert assembled.seed == 7
        assert isinstance(assembled.execution, MockExecutionPort)
        # the venue table (gate name + caps) is handed to the plugin factory
        assert assembled.execution.params["unlock_env"] == "PULSAR_TEST_LIVE_CONFIRM"
        assert assembled.execution.params["max_order_value_cny"] == 5000.0

    def test_live_without_miqmt_section_is_a_config_error(self) -> None:
        config = parse_toml_config(
            '[run]\nmode = "live"\n[data]\nsources = ["akshare"]\n[exec]\nvenue = "miniqmt"\n'
        )
        with pytest.raises(ConfigError, match=r"\[exec\.miniqmt\]"):
            assemble_run(config, registry_with_mocks(), env={"PULSAR_TEST_LIVE_CONFIRM": "1"})


def test_assembly_requires_a_mode() -> None:
    config = parse_toml_config(
        '[data]\nsources = ["akshare"]\n[exec]\nvenue = "backtest"\n'
    )
    with pytest.raises(ConfigError, match="run.mode"):
        assemble_run(config, registry_with_mocks(), env={})


class TestWatermarks:
    def test_watermarks_collected_from_ports_reporting_them(self, example_config) -> None:
        assembled = assemble_run(example_config, registry_with_mocks(), env={})
        watermarks = collect_data_watermarks(assembled)
        assert watermarks == {
            "akshare": {"daily_bars": "2026-09-30"},
            "baostock": {"daily_bars": "2026-09-30"},
        }

    def test_ports_without_watermark_contribute_nothing(self, example_config) -> None:
        registry = registry_with_mocks()
        registry.unregister("baostock", PluginKind.MARKET_DATA)
        registry.register(
            PluginSpec(
                plugin_id="baostock",
                kind=PluginKind.MARKET_DATA,
                factory=lambda **kwargs: PlainMockMarketDataPort(),
            )
        )
        assembled = assemble_run(example_config, registry, env={})
        assert not isinstance(assembled.backup_market_data[0], SupportsWatermark)
        assert collect_data_watermarks(assembled) == {
            "akshare": {"daily_bars": "2026-09-30"}
        }

    def test_bare_date_watermark_gets_default_key(self) -> None:
        from pulsar_app.assembly import _normalize_watermarks

        assert _normalize_watermarks("src", date(2026, 1, 2)) == {"default": "2026-01-02"}

    def test_invalid_watermark_report_is_rejected(self) -> None:
        from pulsar_app.assembly import _normalize_watermarks

        with pytest.raises(PluginError):
            _normalize_watermarks("src", 42)
        with pytest.raises(PluginError):
            _normalize_watermarks("src", {"daily_bars": 4.5})


def test_run_id_is_derived_from_config_and_seed(example_config) -> None:
    assembled = assemble_run(example_config, registry_with_mocks(), env={})
    assert assembled.run_id.startswith("run-research-")
    assert assembled.run_id.endswith(assembled.config_fingerprint[:8])

    tweaked = example_config.model_copy(
        update={"run": example_config.run.model_copy(update={"seed": 99})}
    )
    other = assemble_run(tweaked, registry_with_mocks(), env={})
    assert other.config_fingerprint != assembled.config_fingerprint
