"""Configuration schema, parsing and security-baseline validation."""

from __future__ import annotations

import pytest

from pulsar_app import ConfigError, RunConfig, RunMode, Venue, load_config, parse_toml_config
from pulsar_app.config import resolve_env_refs, validate_plugin_params

from .conftest import EXAMPLE_CONFIG

VALID_MINIMAL = """
[run]
mode = "research"

[data]
sources = ["akshare"]

[exec]
venue = "backtest"
"""


def test_example_config_parses(example_config: RunConfig) -> None:
    assert example_config.run.mode is RunMode.RESEARCH
    assert example_config.data.sources == ("akshare", "baostock")
    assert example_config.data.lake_dir == "./data/lake"
    assert example_config.exec.venue is Venue.BACKTEST
    miqmt = example_config.miqmt_params()
    assert miqmt is not None
    assert miqmt.unlock_env == "PULSAR_LIVE_CONFIRM"
    assert miqmt.max_order_value_cny == 50000.0
    assert miqmt.max_daily_turnover_cny is None


def test_minimal_config_parses() -> None:
    config = parse_toml_config(VALID_MINIMAL)
    assert config.run.mode is RunMode.RESEARCH
    assert config.data.lake_dir == "./data/lake"  # default


@pytest.mark.parametrize(
    "text",
    [
        # unknown top-level section
        '[nope]\nx = 1\n[data]\nsources = ["a"]\n[exec]\nvenue = "backtest"\n',
        # missing [data]
        '[run]\nmode = "research"\n[exec]\nvenue = "backtest"\n',
        # missing [exec]
        '[run]\nmode = "research"\n[data]\nsources = ["a"]\n',
        # missing venue
        '[data]\nsources = ["a"]\n[exec]\nvenue_typo = "backtest"\n',
        # empty sources
        '[data]\nsources = []\n[exec]\nvenue = "backtest"\n',
        # duplicate sources
        '[data]\nsources = ["akshare", "akshare"]\n[exec]\nvenue = "backtest"\n',
        # invalid mode literal
        '[run]\nmode = "yolo"\n[data]\nsources = ["a"]\n[exec]\nvenue = "backtest"\n',
        # mode/venue pairing violation
        '[run]\nmode = "research"\n[data]\nsources = ["a"]\n[exec]\nvenue = "paper"\n',
        '[run]\nmode = "live"\n[data]\nsources = ["a"]\n[exec]\nvenue = "backtest"\n',
        # unknown venue table
        '[data]\nsources = ["a"]\n[exec]\nvenue = "backtest"\n[exec.foo]\nx = 1\n',
        # unknown run key
        '[run]\nstrategy = "x"\n[data]\nsources = ["a"]\n[exec]\nvenue = "backtest"\n',
        # unknown scalar in [data]
        '[data]\nsources = ["a"]\nfoo = "bar"\n[exec]\nvenue = "backtest"\n',
        # TOML syntax error
        "[run\nmode = ",
        # invalid miqmt section: bad env name
        '[data]\nsources = ["a"]\n[exec]\nvenue = "miniqmt"\n[exec.miqmt]\nunlock_env = "1BAD-NAME"\nmax_order_value_cny = 1.0\n',
        # invalid miqmt section: missing cap
        '[data]\nsources = ["a"]\n[exec]\nvenue = "miniqmt"\n[exec.miqmt]\nunlock_env = "PULSAR_X"\n',
        # invalid miqmt section: negative cap
        '[data]\nsources = ["a"]\n[exec]\nvenue = "miniqmt"\n[exec.miqmt]\nunlock_env = "PULSAR_X"\nmax_order_value_cny = -5\n',
        # reserved lake_dir inside source params
        '[data]\nsources = ["a"]\n[data.a]\nlake_dir = "./elsewhere"\n[exec]\nvenue = "backtest"\n',
        # missing sources
        '[data]\nlake_dir = "./x"\n[exec]\nvenue = "backtest"\n',
        # sources not a list of strings
        '[data]\nsources = "akshare"\n[exec]\nvenue = "backtest"\n',
        '[data]\nsources = [1]\n[exec]\nvenue = "backtest"\n',
        # lake_dir not a string
        '[data]\nsources = ["a"]\nlake_dir = 5\n[exec]\nvenue = "backtest"\n',
    ],
)
def test_invalid_configurations_are_rejected(text: str) -> None:
    with pytest.raises(ConfigError):
        parse_toml_config(text)


class TestPlaintextCredentialRejection:
    def test_credential_key_with_literal_value(self) -> None:
        text = VALID_MINIMAL + '\n[data.akshare]\napi_key = "literal-secret"\n'
        with pytest.raises(ConfigError, match="api_key"):
            parse_toml_config(text)

    def test_credential_key_with_env_ref_is_accepted(self) -> None:
        text = VALID_MINIMAL + '\n[data.akshare]\napi_key = "${PULSAR_AK_KEY}"\n'
        config = parse_toml_config(text)
        assert config.data.source_params["akshare"]["api_key"] == "${PULSAR_AK_KEY}"

    def test_credential_key_with_non_string_value(self) -> None:
        text = VALID_MINIMAL + "\n[data.akshare]\ntoken = 12345\n"
        with pytest.raises(ConfigError, match="token"):
            parse_toml_config(text)

    def test_nested_credential_key_with_literal(self) -> None:
        text = VALID_MINIMAL + '\n[data.akshare]\nauth.password = "hunter2"\n'
        with pytest.raises(ConfigError):
            parse_toml_config(text)

    def test_url_with_embedded_userinfo(self) -> None:
        text = VALID_MINIMAL + '\n[data.akshare]\nendpoint = "https://user:pass@example.com/api"\n'
        with pytest.raises(ConfigError, match="userinfo"):
            parse_toml_config(text)

    def test_plain_url_is_fine(self) -> None:
        text = VALID_MINIMAL + '\n[data.akshare]\nendpoint = "https://example.com/api"\n'
        config = parse_toml_config(text)
        assert config.data.source_params["akshare"]["endpoint"] == "https://example.com/api"

    def test_toml_date_value_rejected(self) -> None:
        text = VALID_MINIMAL + "\n[data.akshare]\nas_of = 2026-09-30\n"
        with pytest.raises(ConfigError, match="unsupported parameter value"):
            parse_toml_config(text)

    def test_non_scalar_list_item_rejected(self) -> None:
        text = VALID_MINIMAL + "\n[data.akshare]\nsymbols = [[1]]\n"
        with pytest.raises(ConfigError, match="lists"):
            parse_toml_config(text)


class TestEnvReferenceResolution:
    def test_missing_env_variable_raises(self) -> None:
        with pytest.raises(ConfigError, match="PULSAR_MISSING"):
            resolve_env_refs({"token": "${PULSAR_MISSING}"}, {})

    def test_resolution_substitutes_values(self) -> None:
        resolved = resolve_env_refs(
            {"token": "${A}", "plain": "keep", "nested": {"secret_key": "${B}"}, "tags": ["${A}", 1]},
            {"A": "va", "B": "vb"},
        )
        assert resolved == {
            "token": "va",
            "plain": "keep",
            "nested": {"secret_key": "vb"},
            "tags": ["va", 1],
        }


def test_load_config_missing_file() -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/run.toml")


def test_with_mode_injects_or_rejects() -> None:
    config = parse_toml_config(VALID_MINIMAL)
    live = config.with_mode(RunMode.RESEARCH)  # same mode: allowed (idempotent)
    assert live.run.mode is RunMode.RESEARCH

    # A config without a mode gets one injected.
    modeless = parse_toml_config(VALID_MINIMAL.replace('mode = "research"\n', ""))
    assert modeless.run.mode is None
    assert modeless.with_mode(RunMode.PAPER).run.mode is RunMode.PAPER

    # A config declaring a different mode is rejected.
    with pytest.raises(ConfigError, match="subcommand"):
        modeless.with_mode(RunMode.PAPER).with_mode(RunMode.LIVE)


def test_validate_plugin_params_credential_rule_directly() -> None:
    with pytest.raises(ConfigError):
        validate_plugin_params({"password": "nope"}, where="t")
    # environment reference form passes
    validate_plugin_params({"password": "${SOME_ENV}"}, where="t")
