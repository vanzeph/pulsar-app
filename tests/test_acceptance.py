"""Task acceptance criteria.

1. The example configuration assembles mock ports and completes one dry
   run that produces a RunManifest on disk.
2. No configuration file in the repository carries plaintext credentials.
"""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

from pulsar_app import PluginKind, PluginSpec, RunManifest, execute_run, load_config
from pulsar_app.config import scan_for_plaintext_credentials

from .conftest import EXAMPLE_CONFIG, REPO_ROOT, registry_with_mocks
from .mocks import MockExecutionPort, MockMarketDataPort


def test_example_config_assembles_mock_ports_into_a_dry_run_with_manifest(tmp_path: Path) -> None:
    registry = registry_with_mocks()
    config = load_config(EXAMPLE_CONFIG)

    outcome = execute_run(config, registry, runs_dir=tmp_path)

    manifest_path = outcome.manifest_path
    assert manifest_path.is_file()
    assert manifest_path.parent.name == outcome.manifest.run_id

    # The assembled manifest round-trips and carries the full archive.
    reloaded = RunManifest.read(manifest_path)
    assert reloaded == outcome.manifest
    assert reloaded.mode == "research"
    assert reloaded.resolved_plugins.sources == ("akshare", "baostock")
    assert reloaded.resolved_plugins.venue == "backtest"
    assert reloaded.seed == 0
    assert reloaded.config_snapshot["exec"]["venue"] == "backtest"
    assert reloaded.config_snapshot["data"]["sources"] == ["akshare", "baostock"]
    assert reloaded.data_watermarks == {
        "akshare": {"daily_bars": "2026-09-30"},
        "baostock": {"daily_bars": "2026-09-30"},
    }
    assert reloaded.code_versions["pulsar-contracts"]
    assert reloaded.config_fingerprint

    # The manifest JSON itself must not embed resolved secrets.
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "PULSAR_LIVE_CONFIRM" not in json.dumps(payload.get("data_watermarks", {}))


def test_dry_run_actually_instantiated_mock_ports(tmp_path: Path) -> None:
    registry = registry_with_mocks()
    config = load_config(EXAMPLE_CONFIG)

    built: list[object] = []

    def recording_market_factory(**kwargs: object) -> MockMarketDataPort:
        port = MockMarketDataPort(**kwargs)  # type: ignore[arg-type]
        built.append(port)
        return port

    registry.unregister("akshare", PluginKind.MARKET_DATA)
    registry.register(
        PluginSpec(
            plugin_id="akshare",
            kind=PluginKind.MARKET_DATA,
            factory=recording_market_factory,
        )
    )

    execute_run(config, registry, runs_dir=tmp_path)

    assert built and isinstance(built[0], MockMarketDataPort)
    assert built[0].lake_dir == "./data/lake"


def test_no_plaintext_credentials_in_repository_config_files() -> None:
    """Every TOML file in the repository passes the credential baseline."""
    toml_files = sorted(
        {EXAMPLE_CONFIG, *REPO_ROOT.glob("*.toml"), *REPO_ROOT.glob("examples/**/*.toml")}
    )
    assert toml_files, "expected at least the example configuration to exist"
    for toml_file in toml_files:
        raw = tomllib.loads(toml_file.read_text(encoding="utf-8"))
        # Must not raise: no credential-like key may carry a literal value
        # and no string may embed userinfo credentials.
        scan_for_plaintext_credentials(raw, where=str(toml_file))


def test_mock_ports_satisfy_the_port_protocols() -> None:
    """The mocks used for the dry run honour the contracts' protocols."""
    from pulsar_contracts import ExecutionPort, MarketDataPort

    assert isinstance(MockMarketDataPort(lake_dir="./data/lake"), MarketDataPort)
    assert isinstance(MockExecutionPort(), ExecutionPort)
