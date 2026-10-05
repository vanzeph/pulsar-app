"""CLI skeleton: research / paper / live subcommands and exit codes."""

from __future__ import annotations

import pytest

from pulsar_app import DEFAULT_REGISTRY, PluginKind, PluginSpec, __version__
from pulsar_app.cli import (
    EXIT_CONFIG,
    EXIT_LIVE_LOCKED,
    EXIT_OK,
    EXIT_PLUGIN,
    main,
)

from .conftest import EXAMPLE_CONFIG
from .mocks import mock_execution_factory, mock_market_data_factory


@pytest.fixture
def default_registry_with_mocks():
    """Populate the process-wide registry the CLI resolves against."""
    for source_id in ("akshare", "baostock"):
        DEFAULT_REGISTRY.register(
            PluginSpec(source_id, PluginKind.MARKET_DATA, mock_market_data_factory)
        )
    for venue_id in ("backtest", "paper", "miniqmt"):
        DEFAULT_REGISTRY.register(
            PluginSpec(venue_id, PluginKind.EXECUTION, mock_execution_factory)
        )
    try:
        yield DEFAULT_REGISTRY
    finally:
        DEFAULT_REGISTRY.clear()


def test_research_subcommand_writes_manifest(
    tmp_path, default_registry_with_mocks, capsys
) -> None:
    runs_dir = tmp_path / "runs"
    code = main(["research", "--config", str(EXAMPLE_CONFIG), "--runs-dir", str(runs_dir)])
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "manifest written to" in out
    written = list(runs_dir.rglob("manifest.json"))
    assert len(written) == 1


def test_mode_conflict_between_config_and_subcommand(tmp_path, default_registry_with_mocks) -> None:
    code = main(["paper", "--config", str(EXAMPLE_CONFIG), "--runs-dir", str(tmp_path)])
    assert code == EXIT_CONFIG


def test_live_subcommand_locked_without_unlock(tmp_path, default_registry_with_mocks) -> None:
    config = tmp_path / "live.toml"
    config.write_text(
        """
[run]
mode = "live"

[data]
sources = ["akshare"]

[exec]
venue = "miniqmt"

[exec.miqmt]
unlock_env = "PULSAR_TEST_LIVE_CONFIRM"
max_order_value_cny = 5000.0
""",
        encoding="utf-8",
    )
    code = main(["live", "--config", str(config), "--runs-dir", str(tmp_path / "runs")])
    assert code == EXIT_LIVE_LOCKED


def test_live_subcommand_unlocked_with_env(
    tmp_path, default_registry_with_mocks, monkeypatch, capsys
) -> None:
    monkeypatch.setenv("PULSAR_TEST_LIVE_CONFIRM", "yes")
    config = tmp_path / "live.toml"
    config.write_text(
        """
[run]
mode = "live"

[data]
sources = ["akshare"]

[exec]
venue = "miniqmt"

[exec.miqmt]
unlock_env = "PULSAR_TEST_LIVE_CONFIRM"
max_order_value_cny = 5000.0
""",
        encoding="utf-8",
    )
    code = main(["live", "--config", str(config), "--runs-dir", str(tmp_path / "runs")])
    assert code == EXIT_OK
    assert "(live)" in capsys.readouterr().out


def test_unknown_plugin_maps_to_plugin_exit_code(tmp_path, default_registry_with_mocks) -> None:
    DEFAULT_REGISTRY.unregister("baostock", PluginKind.MARKET_DATA)
    code = main(["research", "--config", str(EXAMPLE_CONFIG), "--runs-dir", str(tmp_path)])
    assert code == EXIT_PLUGIN


def test_missing_config_file_is_a_config_error(tmp_path, default_registry_with_mocks) -> None:
    code = main(["research", "--config", str(tmp_path / "nope.toml"), "--runs-dir", str(tmp_path)])
    assert code == EXIT_CONFIG


def test_version_flag(capsys) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--version"])
    assert excinfo.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_command_is_required() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main([])
    assert excinfo.value.code != 0
