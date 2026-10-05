"""Shared fixtures: mock-plugin registry and the example configuration."""

from __future__ import annotations

from pathlib import Path

import pytest

from pulsar_app import PluginKind, PluginRegistry, PluginSpec, RunConfig, load_config

from .mocks import mock_execution_factory, mock_market_data_factory

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG = REPO_ROOT / "examples" / "runs" / "dualma.toml"


def registry_with_mocks() -> PluginRegistry:
    """A fresh registry with mock plugins for every id the example configs use."""
    registry = PluginRegistry()
    for source_id in ("akshare", "baostock"):
        registry.register(
            PluginSpec(
                plugin_id=source_id,
                kind=PluginKind.MARKET_DATA,
                factory=mock_market_data_factory,
                description=f"mock market-data plugin ({source_id})",
            )
        )
    for venue_id in ("backtest", "paper", "miniqmt"):
        registry.register(
            PluginSpec(
                plugin_id=venue_id,
                kind=PluginKind.EXECUTION,
                factory=mock_execution_factory,
                description=f"mock execution plugin ({venue_id})",
            )
        )
    return registry


@pytest.fixture
def registry() -> PluginRegistry:
    return registry_with_mocks()


@pytest.fixture
def example_config() -> RunConfig:
    return load_config(EXAMPLE_CONFIG)
