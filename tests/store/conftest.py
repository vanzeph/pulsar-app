"""Fixtures for the unified-store suite (task STORE1).

The engine/validation/CLI modules run against the base install
(pulsar-app + pulsar-contracts); the *assembly* module additionally
needs pulsar-core and gates itself with ``pytest.importorskip`` in its
own test file, mirroring the e2e suite convention.
"""

from __future__ import annotations

from pathlib import Path

import pytest

#: The shipped example assets (an Agent write pushed through the store).
EXAMPLES = Path(__file__).resolve().parents[2] / "examples" / "store"


@pytest.fixture
def store_root(tmp_path: Path) -> Path:
    return tmp_path / "store"


@pytest.fixture
def factor_source() -> str:
    return (EXAMPLES / "custom_factor.py").read_text(encoding="utf-8")


@pytest.fixture
def experiment_toml() -> str:
    return (EXAMPLES / "experiment.toml").read_text(encoding="utf-8")


@pytest.fixture
def lake_dir(tmp_path: Path) -> Path:
    lake = tmp_path / "data" / "lake"
    lake.mkdir(parents=True)
    (lake / "bars").mkdir()
    (lake / "bars" / "DAILY").mkdir()
    (lake / "bars" / "DAILY" / "SH600519.parquet").write_bytes(b"existing lake payload")
    return lake
